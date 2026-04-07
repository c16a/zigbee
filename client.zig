// SPDX-License-Identifier: MIT
const std = @import("std");

pub const Message = struct {
    subject: []const u8,
    sid: u64,
    reply: ?[]const u8,
    payload: []const u8,
};

pub const Frame = union(enum) {
    info: []const u8,
    pong,
    err: []const u8,
    msg: Message,
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
        if (self.cursor > 0 and self.cursor > self.buffer.items.len / 2) {
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

            if (std.mem.eql(u8, line, "PONG")) return .pong;

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

    pub fn connect(allocator: std.mem.Allocator, address: std.net.Address) !Client {
        const stream = try std.net.tcpConnectToAddress(address);
        return fromStream(allocator, stream);
    }

    pub fn fromStream(allocator: std.mem.Allocator, stream: std.net.Stream) !Client {
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
                .info => break,
                else => continue,
            }
        }

        try client.stream.writeAll("CONNECT {}\r\n");
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
            try self.stream.writeAll("PUB ");
            try self.stream.writeAll(subject);
            try self.stream.writeAll(" ");
            try self.stream.writeAll(reply_to);
            try self.stream.writeAll(" ");
            const size_text = try std.fmt.bufPrint(&size_buf, "{d}\r\n", .{payload.len});
            try self.stream.writeAll(size_text);
        } else {
            try self.stream.writeAll("PUB ");
            try self.stream.writeAll(subject);
            try self.stream.writeAll(" ");
            const size_text = try std.fmt.bufPrint(&size_buf, "{d}\r\n", .{payload.len});
            try self.stream.writeAll(size_text);
        }
        try self.stream.writeAll(payload);
        try self.stream.writeAll("\r\n");
    }

    pub fn subscribe(self: *Client, subject: []const u8, queue: ?[]const u8, sid: u64) !void {
        var sid_buf: [32]u8 = undefined;
        try self.stream.writeAll("SUB ");
        try self.stream.writeAll(subject);
        if (queue) |group| {
            try self.stream.writeAll(" ");
            try self.stream.writeAll(group);
        }
        try self.stream.writeAll(" ");
        const sid_text = try std.fmt.bufPrint(&sid_buf, "{d}\r\n", .{sid});
        try self.stream.writeAll(sid_text);
    }

    pub fn unsubscribe(self: *Client, sid: u64, max: ?u64) !void {
        var sid_buf: [32]u8 = undefined;
        var max_buf: [32]u8 = undefined;
        try self.stream.writeAll("UNSUB ");
        const sid_text = try std.fmt.bufPrint(&sid_buf, "{d}", .{sid});
        try self.stream.writeAll(sid_text);
        if (max) |limit| {
            try self.stream.writeAll(" ");
            const max_text = try std.fmt.bufPrint(&max_buf, "{d}", .{limit});
            try self.stream.writeAll(max_text);
        }
        try self.stream.writeAll("\r\n");
    }

    pub fn ping(self: *Client) !void {
        try self.stream.writeAll("PING\r\n");
        while (true) {
            const frame = try self.nextFrame() orelse return error.UnexpectedEndOfStream;
            switch (frame) {
                .pong => return,
                .err => |_| return error.InvalidServerResponse,
                else => continue,
            }
        }
    }

    pub fn nextMessage(self: *Client) !?Message {
        while (true) {
            const frame = try self.nextFrame() orelse return null;
            switch (frame) {
                .msg => |msg| return msg,
                .err => return error.InvalidServerResponse,
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
};

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
