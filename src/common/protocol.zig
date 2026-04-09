// SPDX-License-Identifier: MIT
const std = @import("std");

pub const default_client_server = "127.0.0.1:4222";

pub const Subscription = struct {
    subject: []const u8,
    queue: ?[]const u8,
    sid: u64,
};

pub const Unsubscribe = struct {
    sid: u64,
    max: ?u64,
};

pub const Publish = struct {
    subject: []const u8,
    reply: ?[]const u8,
    payload: []const u8,
};

pub const Info = struct {
    server_id: []const u8 = "zigbee",
    version: []const u8 = "0.1.0",
    proto: u8 = 1,
    host: []const u8 = "127.0.0.1",
    port: u16,
    max_payload: u32 = 1048576,
    auth_required: bool = false,
    auth_mode: []const u8 = "none",
    nonce: ?[]const u8 = null,
};

pub fn formatInfoJson(allocator: std.mem.Allocator, info: Info) ![]u8 {
    return try std.json.Stringify.valueAlloc(allocator, info, .{});
}

pub const Connect = struct {
    auth_mode: []const u8 = "none",
    public_key: ?[]const u8 = null,
    signature: ?[]const u8 = null,
};

pub fn formatConnectJson(allocator: std.mem.Allocator, connect: Connect) ![]u8 {
    return try std.json.Stringify.valueAlloc(allocator, connect, .{});
}

pub const OutgoingMessage = struct {
    session_id: u64,
    sid: u64,
    subject: []const u8,
    reply: ?[]const u8,
    payload: []const u8,
};

pub const OutgoingFrame = union(enum) {
    info: Info,
    ok,
    pong,
    close,
    msg: OutgoingMessage,
};

pub const ServerCommand = union(enum) {
    ping,
    pong,
    connect: []const u8,
    sub: Subscription,
    unsub: Unsubscribe,
    publish: Publish,
};

pub const Command = ServerCommand;

pub const ClientVerb = enum {
    publish,
    subscribe,
    unsubscribe,
    request,
    reply,
    ping,
    session,
};

pub const client_verbs = [_]ClientVerb{
    .publish,
    .subscribe,
    .unsubscribe,
    .request,
    .reply,
    .ping,
    .session,
};

pub fn parseClientVerb(text: []const u8) ?ClientVerb {
    if (std.mem.eql(u8, text, "pub")) return .publish;
    if (std.mem.eql(u8, text, "sub")) return .subscribe;
    if (std.mem.eql(u8, text, "unsub")) return .unsubscribe;
    if (std.mem.eql(u8, text, "request")) return .request;
    if (std.mem.eql(u8, text, "reply")) return .reply;
    if (std.mem.eql(u8, text, "ping")) return .ping;
    if (std.mem.eql(u8, text, "session")) return .session;
    return null;
}

pub fn clientVerbName(verb: ClientVerb) []const u8 {
    return switch (verb) {
        .publish => "pub",
        .subscribe => "sub",
        .unsubscribe => "unsub",
        .request => "request",
        .reply => "reply",
        .ping => "ping",
        .session => "session",
    };
}

pub const ClientPublish = struct {
    server: []const u8 = default_client_server,
    subject: []const u8,
    payload: []const u8,
    reply: ?[]const u8 = null,
};

pub const ClientSubscribe = struct {
    server: []const u8 = default_client_server,
    subject: []const u8,
    queue: ?[]const u8 = null,
    sid: u64 = 1,
    count: ?usize = null,
};

pub const ClientUnsubscribe = struct {
    server: []const u8 = default_client_server,
    sid: u64,
    max: ?u64 = null,
};

pub const ClientRequest = struct {
    server: []const u8 = default_client_server,
    subject: []const u8,
    payload: []const u8,
};

pub const ClientReply = struct {
    server: []const u8 = default_client_server,
    subject: []const u8,
    payload: []const u8,
    queue: ?[]const u8 = null,
    sid: u64 = 1,
    count: ?usize = null,
};

pub const ClientPing = struct {
    server: []const u8 = default_client_server,
    count: usize = 1,
};

pub const ClientSession = struct {
    server: []const u8 = default_client_server,
};

pub const ClientCommand = union(enum) {
    publish: ClientPublish,
    subscribe: ClientSubscribe,
    unsubscribe: ClientUnsubscribe,
    request: ClientRequest,
    reply: ClientReply,
    ping: ClientPing,
    session: ClientSession,
};

const PendingPublish = struct {
    subject: []const u8,
    reply: ?[]const u8,
    size: usize,
};

const LineCommand = union(enum) {
    ping,
    pong,
    connect: []const u8,
    sub: Subscription,
    unsub: Unsubscribe,
    publish_header: PendingPublish,
};

pub const Reader = struct {
    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8) = .empty,
    cursor: usize = 0,
    pending_publish: ?PendingPublish = null,

    pub fn init(allocator: std.mem.Allocator) Reader {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Reader) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn feed(self: *Reader, bytes: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, bytes);
    }

    pub fn next(self: *Reader) !?Command {
        if (self.pending_publish == null and self.cursor > 0 and self.cursor > self.buffer.items.len / 2) {
            self.compact();
        }

        while (true) {
            if (self.pending_publish) |pending| {
                if (self.buffer.items.len - self.cursor < pending.size + 2) return null;

                const payload = self.buffer.items[self.cursor .. self.cursor + pending.size];
                const crlf = self.buffer.items[self.cursor + pending.size .. self.cursor + pending.size + 2];
                if (!std.mem.eql(u8, crlf, "\r\n")) return error.InvalidFrame;

                self.cursor += pending.size + 2;
                self.pending_publish = null;
                return .{
                    .publish = .{
                        .subject = pending.subject,
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

            const parsed = try parseLine(line);
            switch (parsed) {
                .publish_header => |pending| {
                    self.pending_publish = pending;
                    continue;
                },
                .ping => return .ping,
                .pong => return .pong,
                .connect => |payload| return .{ .connect = payload },
                .sub => |sub| return .{ .sub = sub },
                .unsub => |unsub| return .{ .unsub = unsub },
            }
        }
    }

    fn compact(self: *Reader) void {
        const remaining = self.buffer.items[self.cursor..];
        std.mem.copyForwards(u8, self.buffer.items[0..remaining.len], remaining);
        self.buffer.shrinkRetainingCapacity(remaining.len);
        self.cursor = 0;
    }
};

fn parseLine(line: []const u8) !LineCommand {
    if (std.mem.eql(u8, line, "PING")) return .ping;
    if (std.mem.eql(u8, line, "PONG")) return .pong;

    if (std.mem.startsWith(u8, line, "CONNECT")) {
        const rest = trimLeft(line["CONNECT".len..]);
        return .{ .connect = rest };
    }

    if (std.mem.startsWith(u8, line, "SUB")) {
        const rest = trimLeft(line["SUB".len..]);
        var it = std.mem.tokenizeScalar(u8, rest, ' ');
        const subject = it.next() orelse return error.InvalidCommand;
        const second = it.next() orelse return error.InvalidCommand;
        const third = it.next();
        if (it.next() != null) return error.InvalidCommand;

        if (third) |sid_text| {
            return .{
                .sub = .{
                    .subject = subject,
                    .queue = second,
                    .sid = try parseU64(sid_text),
                },
            };
        }

        return .{
            .sub = .{
                .subject = subject,
                .queue = null,
                .sid = try parseU64(second),
            },
        };
    }

    if (std.mem.startsWith(u8, line, "UNSUB")) {
        const rest = trimLeft(line["UNSUB".len..]);
        var it = std.mem.tokenizeScalar(u8, rest, ' ');
        const sid_text = it.next() orelse return error.InvalidCommand;
        const max_text = it.next();
        if (it.next() != null) return error.InvalidCommand;
        return .{
            .unsub = .{
                .sid = try parseU64(sid_text),
                .max = if (max_text) |m| try parseU64(m) else null,
            },
        };
    }

    if (std.mem.startsWith(u8, line, "PUB")) {
        const rest = trimLeft(line["PUB".len..]);
        var it = std.mem.tokenizeScalar(u8, rest, ' ');
        const subject = it.next() orelse return error.InvalidCommand;
        const second = it.next() orelse return error.InvalidCommand;
        const third = it.next();
        if (it.next() != null) return error.InvalidCommand;

        if (third) |size_text| {
            return .{
                .publish_header = .{
                    .subject = subject,
                    .reply = second,
                    .size = try parseUsize(size_text),
                },
            };
        }

        return .{
            .publish_header = .{
                .subject = subject,
                .reply = null,
                .size = try parseUsize(second),
            },
        };
    }

    return error.InvalidCommand;
}

fn parseU64(text: []const u8) !u64 {
    return std.fmt.parseInt(u64, text, 10) catch error.InvalidNumber;
}

fn parseUsize(text: []const u8) !usize {
    return std.fmt.parseInt(usize, text, 10) catch error.InvalidNumber;
}

fn trimLeft(text: []const u8) []const u8 {
    var i: usize = 0;
    while (i < text.len and text[i] == ' ') : (i += 1) {}
    return text[i..];
}

pub fn matchesSubject(filter: []const u8, subject: []const u8) bool {
    var filter_it = std.mem.tokenizeScalar(u8, filter, '.');
    var subject_it = std.mem.tokenizeScalar(u8, subject, '.');

    while (filter_it.next()) |part| {
        if (std.mem.eql(u8, part, ">")) {
            return subject_it.next() != null;
        }

        const actual = subject_it.next() orelse return false;
        if (std.mem.eql(u8, part, "*")) continue;
        if (!std.mem.eql(u8, part, actual)) return false;
    }

    return subject_it.next() == null;
}

test "subject matching" {
    try std.testing.expect(matchesSubject("foo.bar", "foo.bar"));
    try std.testing.expect(matchesSubject("foo.*", "foo.bar"));
    try std.testing.expect(matchesSubject("foo.>", "foo.bar.baz"));
    try std.testing.expect(!matchesSubject("foo.bar", "foo"));
    try std.testing.expect(!matchesSubject("foo.*", "foo"));
    try std.testing.expect(!matchesSubject("foo.>", "foo"));
}

test "reader parses partial publish" {
    var reader = Reader.init(std.testing.allocator);
    defer reader.deinit();

    try reader.feed("PUB foo 5\r\nhe");
    try std.testing.expectEqual(@as(?Command, null), try reader.next());
    try reader.feed("llo\r\n");

    const command = try reader.next();
    try std.testing.expect(command != null);
    const payload = command.?.publish.payload;
    try std.testing.expectEqualStrings("hello", payload);
}

test "reader rejects malformed command" {
    var reader = Reader.init(std.testing.allocator);
    defer reader.deinit();

    try reader.feed("NOPE\r\n");
    try std.testing.expectError(error.InvalidCommand, reader.next());
}
