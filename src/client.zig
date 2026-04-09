// SPDX-License-Identifier: MIT
const std = @import("std");
const builtin = @import("builtin");
const crypto_mod = @import("crypto.zig");
const protocol = @import("protocol.zig");

pub const Message = struct {
    subject: []const u8,
    sid: u64,
    reply: ?[]const u8,
    payload: []const u8,
};

pub const Frame = union(enum) {
    info: []const u8,
    ok,
    pong,
    close,
    err: []const u8,
    msg: Message,
};

pub const Auth = struct {
    key_pair: crypto_mod.Ed25519.KeyPair,

    pub fn fromSeed(seed: [crypto_mod.seed_length]u8) !Auth {
        return .{ .key_pair = try crypto_mod.keyPairFromSeed(seed) };
    }
};

const PendingMessage = struct {
    subject: []const u8,
    sid: u64,
    reply: ?[]const u8,
    size: usize,
};

pub const FrameReader = struct {
    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8) = .empty,
    cursor: usize = 0,
    pending_message: ?PendingMessage = null,

    pub fn init(allocator: std.mem.Allocator) FrameReader {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *FrameReader) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn feed(self: *FrameReader, bytes: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, bytes);
    }

    pub fn next(self: *FrameReader) !?Frame {
        if (self.pending_message == null and self.cursor > 0 and self.cursor > self.buffer.items.len / 2) {
            self.compact();
        }

        while (true) {
            if (self.pending_message) |pending| {
                if (self.buffer.items.len - self.cursor < pending.size + 2) return null;

                const payload = self.buffer.items[self.cursor .. self.cursor + pending.size];
                const crlf = self.buffer.items[self.cursor + pending.size .. self.cursor + pending.size + 2];
                if (!std.mem.eql(u8, crlf, "\r\n")) return error.InvalidFrame;

                self.cursor += pending.size + 2;
                self.pending_message = null;
                return .{
                    .msg = .{
                        .subject = pending.subject,
                        .sid = pending.sid,
                        .reply = pending.reply,
                        .payload = payload,
                    },
                };
            }

            const newline = std.mem.indexOfScalarPos(u8, self.buffer.items, self.cursor, '\n') orelse return null;
            var line = self.buffer.items[self.cursor..newline];
            self.cursor = newline + 1;
            if (line.len > 0 and line[line.len - 1] == '\r') {
                line = line[0 .. line.len - 1];
            }
            if (line.len == 0) continue;

            if (std.mem.startsWith(u8, line, "INFO")) {
                return .{ .info = trimLeft(line["INFO".len..]) };
            }

            if (std.mem.eql(u8, line, "OK")) return .ok;
            if (std.mem.eql(u8, line, "PONG")) return .pong;
            if (std.mem.eql(u8, line, "CLOSE")) return .close;

            if (std.mem.startsWith(u8, line, "-ERR")) {
                return .{ .err = trimLeft(line["-ERR".len..]) };
            }

            if (std.mem.startsWith(u8, line, "MSG")) {
                const rest = trimLeft(line["MSG".len..]);
                var it = std.mem.tokenizeScalar(u8, rest, ' ');
                const subject = it.next() orelse return error.InvalidFrame;
                const sid_text = it.next() orelse return error.InvalidFrame;
                const third = it.next() orelse return error.InvalidFrame;
                const fourth = it.next();
                if (it.next() != null) return error.InvalidFrame;

                if (fourth) |size_text| {
                    self.pending_message = .{
                        .subject = subject,
                        .sid = try parseU64(sid_text),
                        .reply = third,
                        .size = try parseU64(size_text),
                    };
                } else {
                    self.pending_message = .{
                        .subject = subject,
                        .sid = try parseU64(sid_text),
                        .reply = null,
                        .size = try parseU64(third),
                    };
                }
                continue;
            }

            return error.InvalidFrame;
        }
    }

    fn compact(self: *FrameReader) void {
        const remaining = self.buffer.items[self.cursor..];
        std.mem.copyForwards(u8, self.buffer.items[0..remaining.len], remaining);
        self.buffer.shrinkRetainingCapacity(remaining.len);
        self.cursor = 0;
    }
};

pub const Client = struct {
    allocator: std.mem.Allocator,
    stream: std.net.Stream,
    reader: FrameReader,
    scratch: [4096]u8 = undefined,

    pub fn connect(allocator: std.mem.Allocator, address: std.net.Address, auth: ?Auth) !Client {
        const stream = try std.net.tcpConnectToAddress(address);
        return fromStream(allocator, stream, auth);
    }

    pub fn fromStream(allocator: std.mem.Allocator, stream: std.net.Stream, auth: ?Auth) !Client {
        var client = Client{
            .allocator = allocator,
            .stream = stream,
            .reader = FrameReader.init(allocator),
        };
        errdefer client.reader.deinit();
        errdefer client.stream.close();

        while (true) {
            const frame = try client.nextFrame() orelse return error.UnexpectedEndOfStream;
            switch (frame) {
                .info => |info_json| {
                    const info = try parseInfo(allocator, info_json);
                    defer info.deinit();
                    try sendConnect(&client, info.value, auth);
                    break;
                },
                else => continue,
            }
        }
        return client;
    }

    pub fn deinit(self: *Client) void {
        self.reader.deinit();
        self.stream.close();
    }

    pub fn nextFrame(self: *Client) !?Frame {
        while (true) {
            if (try self.reader.next()) |frame| return frame;
            const read_len = try self.stream.read(&self.scratch);
            if (read_len == 0) return null;
            try self.reader.feed(self.scratch[0..read_len]);
        }
    }

    pub fn publish(self: *Client, subject: []const u8, reply: ?[]const u8, payload: []const u8) !void {
        var size_buf: [32]u8 = undefined;
        if (reply) |reply_to| {
            const size_text = try std.fmt.bufPrint(&size_buf, "{d}\r\n", .{payload.len});
            try self.writeParts(&[_][]const u8{ "PUB ", subject, " ", reply_to, " ", size_text });
        } else {
            const size_text = try std.fmt.bufPrint(&size_buf, "{d}\r\n", .{payload.len});
            try self.writeParts(&[_][]const u8{ "PUB ", subject, " ", size_text });
        }
        try self.stream.writeAll(payload);
        try self.stream.writeAll("\r\n");
    }

    pub fn subscribe(self: *Client, subject: []const u8, queue: ?[]const u8, sid: u64) !void {
        var sid_buf: [32]u8 = undefined;
        if (queue) |group| {
            const sid_text = try std.fmt.bufPrint(&sid_buf, "{d}\r\n", .{sid});
            try self.writeParts(&[_][]const u8{ "SUB ", subject, " ", group, " ", sid_text });
            return;
        }
        const sid_text = try std.fmt.bufPrint(&sid_buf, "{d}\r\n", .{sid});
        try self.writeParts(&[_][]const u8{ "SUB ", subject, " ", sid_text });
    }

    pub fn unsubscribe(self: *Client, sid: u64, max: ?u64) !void {
        var sid_buf: [32]u8 = undefined;
        var max_buf: [32]u8 = undefined;
        const sid_text = try std.fmt.bufPrint(&sid_buf, "{d}", .{sid});
        if (max) |limit| {
            const max_text = try std.fmt.bufPrint(&max_buf, "{d}", .{limit});
            try self.writeLine(&[_][]const u8{ "UNSUB ", sid_text, " ", max_text });
        } else {
            try self.writeLine(&[_][]const u8{ "UNSUB ", sid_text });
        }
    }

    pub fn ping(self: *Client) !void {
        try self.writeLine(&[_][]const u8{"PING"});
        while (true) {
            const frame = try self.nextFrame() orelse return error.UnexpectedEndOfStream;
            switch (frame) {
                .pong => return,
                .close => return error.ServerClosed,
                .err => |_| return error.InvalidServerResponse,
                .ok => continue,
                else => continue,
            }
        }
    }

    pub fn nextMessage(self: *Client) !?Message {
        while (true) {
            const frame = try self.nextFrame() orelse return null;
            switch (frame) {
                .msg => |msg| return msg,
                .close => return error.ServerClosed,
                .err => return error.InvalidServerResponse,
                .ok => continue,
                else => continue,
            }
        }
    }

    pub fn waitForMessage(self: *Client, subject: []const u8) !Message {
        while (true) {
            const msg = try self.nextMessage() orelse return error.UnexpectedEndOfStream;
            if (std.mem.eql(u8, msg.subject, subject)) return msg;
        }
    }

    fn writeParts(self: *Client, parts: []const []const u8) !void {
        for (parts) |part| {
            try self.stream.writeAll(part);
        }
    }

    fn writeLine(self: *Client, parts: []const []const u8) !void {
        try self.writeParts(parts);
        try self.stream.writeAll("\r\n");
    }
};

pub fn runSession(allocator: std.mem.Allocator, address: std.net.Address, auth: ?Auth) !void {
    if (builtin.os.tag == .windows) return error.UnsupportedPlatform;

    const stream = try std.net.tcpConnectToAddress(address);
    var client = Client{
        .allocator = allocator,
        .stream = stream,
        .reader = FrameReader.init(allocator),
    };
    defer client.deinit();

    const stdin = std.fs.File.stdin();
    const stdout = std.fs.File.stdout().deprecatedWriter();
    var stdin_reader = stdin.deprecatedReader();
    var frame_reader = &client.reader;
    var scratch: [4096]u8 = undefined;
    const max_line = 64 * 1024;

    while (true) {
        const frame = try client.nextFrame() orelse return error.UnexpectedEndOfStream;
        switch (frame) {
            .info => |info_json| {
                try printFrame(stdout, frame);
                const info = try parseInfo(allocator, info_json);
                defer info.deinit();

                if (info.value.auth_required and auth != null) {
                    try sendConnect(&client, info.value, auth);
                } else {
                    const connect_json = try protocol.formatConnectJson(client.allocator, .{ .auth_mode = "none" });
                    defer client.allocator.free(connect_json);
                    try client.writeLine(&[_][]const u8{ "CONNECT ", connect_json });
                }
                break;
            },
            .close => {
                try printFrame(stdout, frame);
                return;
            },
            else => {
                try printFrame(stdout, frame);
                continue;
            },
        }
    }

    while (true) {
        var fds = [_]std.posix.pollfd{
            .{ .fd = stdin.handle, .events = std.posix.POLL.IN, .revents = 0 },
            .{ .fd = client.stream.handle, .events = std.posix.POLL.IN, .revents = 0 },
        };

        _ = try std.posix.poll(&fds, -1);

        if ((fds[1].revents & std.posix.POLL.IN) != 0) {
            const read_len = try client.stream.read(&scratch);
            if (read_len == 0) return;
            try frame_reader.feed(scratch[0..read_len]);
            while (try frame_reader.next()) |frame| {
                switch (frame) {
                    .close => {
                        try stdout.writeAll("CLOSE\r\n");
                        return;
                    },
                    .ok => try stdout.writeAll("OK\r\n"),
                    else => try printFrame(stdout, frame),
                }
            }
        }

        if ((fds[0].revents & std.posix.POLL.IN) != 0) {
            const line = try stdin_reader.readUntilDelimiterOrEofAlloc(allocator, '\n', max_line) orelse return;
            defer allocator.free(line);
            const trimmed = std.mem.trimRight(u8, line, "\r");
            try client.writeLine(&[_][]const u8{trimmed});
        }
    }
}

pub fn loadAuthFromSeedFile(allocator: std.mem.Allocator, path: []const u8) !Auth {
    const contents = try std.fs.cwd().readFileAlloc(allocator, path, 16 * 1024);
    defer allocator.free(contents);

    const trimmed = std.mem.trim(u8, contents, " \t\r\n");
    const seed = try crypto_mod.decodeBase64Fixed(crypto_mod.seed_length, trimmed);
    return try Auth.fromSeed(seed);
}

pub fn makeInbox(allocator: std.mem.Allocator, prefix: []const u8) ![]u8 {
    const count = inbox_counter.fetchAdd(1, .monotonic);
    const now = @as(i128, std.time.nanoTimestamp());
    return std.fmt.allocPrint(allocator, "_INBOX.{s}.{d}.{d}", .{ prefix, now, count });
}

var inbox_counter = std.atomic.Value(u64).init(1);

pub fn parseServerAddress(text: []const u8) !std.net.Address {
    const index = std.mem.lastIndexOfScalar(u8, text, ':') orelse return error.InvalidServerAddress;
    const host = text[0..index];
    const port_text = text[index + 1 ..];
    const port = try std.fmt.parseInt(u16, port_text, 10);
    return std.net.Address.parseIp(host, port);
}

fn parseU64(text: []const u8) !u64 {
    return std.fmt.parseInt(u64, text, 10) catch error.InvalidNumber;
}

fn trimLeft(text: []const u8) []const u8 {
    var i: usize = 0;
    while (i < text.len and text[i] == ' ') : (i += 1) {}
    return text[i..];
}

fn parseInfo(allocator: std.mem.Allocator, text: []const u8) !std.json.Parsed(protocol.Info) {
    return try std.json.parseFromSlice(protocol.Info, allocator, text, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn sendConnect(client: *Client, info: protocol.Info, auth: ?Auth) !void {
    if (info.auth_required) {
        const auth_data = auth orelse return error.AuthRequired;
        if (!std.mem.eql(u8, info.auth_mode, "zkey")) return error.InvalidServerResponse;
        const nonce_text = info.nonce orelse return error.InvalidServerResponse;
        const nonce = try crypto_mod.decodeBase64Fixed(crypto_mod.seed_length, nonce_text);
        const signature = try auth_data.key_pair.sign(nonce[0..], null);
        const public_key = auth_data.key_pair.public_key.toBytes();
        const public_key_text = try crypto_mod.encodeBase64(client.allocator, public_key[0..]);
        defer client.allocator.free(public_key_text);
        const signature_bytes = signature.toBytes();
        const signature_text = try crypto_mod.encodeBase64(client.allocator, signature_bytes[0..]);
        defer client.allocator.free(signature_text);

        const connect_json = try protocol.formatConnectJson(client.allocator, .{
            .auth_mode = "zkey",
            .public_key = public_key_text,
            .signature = signature_text,
        });
        defer client.allocator.free(connect_json);
        try client.writeLine(&[_][]const u8{ "CONNECT ", connect_json });
        return;
    }

    const connect_json = try protocol.formatConnectJson(client.allocator, .{ .auth_mode = "none" });
    defer client.allocator.free(connect_json);
    try client.writeLine(&[_][]const u8{ "CONNECT ", connect_json });
}

fn printFrame(writer: anytype, frame: Frame) !void {
    switch (frame) {
        .info => |info_json| try writer.print("INFO {s}\r\n", .{info_json}),
        .ok => try writer.writeAll("OK\r\n"),
        .pong => try writer.writeAll("PONG\r\n"),
        .close => try writer.writeAll("CLOSE\r\n"),
        .err => |msg| try writer.print("-ERR '{s}'\r\n", .{msg}),
        .msg => |msg| {
            if (msg.reply) |reply_to| {
                try writer.print("MSG {s} {d} {s} {d}\r\n", .{ msg.subject, msg.sid, reply_to, msg.payload.len });
            } else {
                try writer.print("MSG {s} {d} {d}\r\n", .{ msg.subject, msg.sid, msg.payload.len });
            }
            try writer.writeAll(msg.payload);
            try writer.writeAll("\r\n");
        },
    }
}
