// SPDX-License-Identifier: MIT
const std = @import("std");
const config_mod = @import("config.zig");
const client_mod = @import("client.zig");
const broker_mod = @import("broker.zig");
const protocol = @import("protocol.zig");

test "protocol partial parsing and malformed frames" {
    var reader = protocol.Reader.init(std.testing.allocator);
    defer reader.deinit();

    try reader.feed("SUB foo 1\r\nPU");
    try std.testing.expect((try reader.next()) != null);
    try reader.feed("B foo 5\r\nhe");
    try std.testing.expectEqual(@as(?protocol.Command, null), try reader.next());
    try reader.feed("llo\r\n");

    const command = try reader.next();
    try std.testing.expect(command != null);
    try std.testing.expectEqualStrings("hello", command.?.publish.payload);

    try reader.feed("NOPE\r\n");
    try std.testing.expectError(error.InvalidCommand, reader.next());
}

test "broker queue groups and unsubscribe" {
    var broker = broker_mod.Broker.init(std.testing.allocator);
    defer broker.deinit();

    try broker.subscribe(1, 1, "foo", "queue");
    try broker.subscribe(2, 2, "foo", "queue");
    try broker.subscribe(3, 3, "foo", null);

    const first = try broker.publish("foo");
    defer std.testing.allocator.free(first);
    try std.testing.expectEqual(@as(usize, 2), first.len);
    try std.testing.expectEqual(@as(u64, 3), first[0].sid);
    try std.testing.expect(first[1].sid == 1 or first[1].sid == 2);

    broker.unsubscribe(3, 3, null);
    const second = try broker.publish("foo");
    defer std.testing.allocator.free(second);
    try std.testing.expectEqual(@as(usize, 1), second.len);
}

test "config file loads listen address" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{
        .sub_path = "zigbee.config.json",
        .data = "{\"listen_address\":\"127.0.0.1:4222\"}",
    });

    var loaded = try config_mod.loadFromDir(tmp.dir, std.testing.allocator, "zigbee.config.json", true);
    try std.testing.expect(loaded != null);
    defer loaded.?.deinit();
    try std.testing.expectEqualStrings("127.0.0.1:4222", loaded.?.value().listen_address.?);
}

fn makeSocketPair() ![2]std.net.Stream {
    var fds: [2]std.c.fd_t = undefined;
    if (std.c.socketpair(std.c.AF.UNIX, std.c.SOCK.STREAM, 0, &fds) != 0) {
        return error.SocketPairFailed;
    }

    return .{
        .{ .handle = @as(std.net.Stream.Handle, fds[0]) },
        .{ .handle = @as(std.net.Stream.Handle, fds[1]) },
    };
}

fn readProtocolCommand(reader: *protocol.Reader, stream: std.net.Stream, scratch: []u8) !?protocol.Command {
    while (true) {
        if (try reader.next()) |cmd| return cmd;
        const n = try stream.read(scratch);
        if (n == 0) return null;
        try reader.feed(scratch[0..n]);
    }
}

fn mockBroker(stream: std.net.Stream, allocator: std.mem.Allocator) void {
    defer stream.close();

    var reader = protocol.Reader.init(allocator);
    defer reader.deinit();

    var scratch: [4096]u8 = undefined;
    stream.writeAll("INFO {\"server_id\":\"zigbee\",\"version\":\"0.1.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":0,\"max_payload\":1048576}\r\n") catch return;

    while (true) {
        const maybe_cmd = readProtocolCommand(&reader, stream, &scratch) catch return;
        const cmd = maybe_cmd orelse return;

        switch (cmd) {
            .connect => {},
            .ping => stream.writeAll("PONG\r\n") catch return,
            .pong => {},
            .sub => |sub| {
                if (!std.mem.eql(u8, sub.subject, "svc.echo")) {
                    std.debug.panic("unexpected subscription subject: {s}", .{sub.subject});
                }
            },
            .unsub => {},
            .publish => |publish| {
                if (publish.reply) |reply_to| {
                    var header: [128]u8 = undefined;
                    const frame = std.fmt.bufPrint(&header, "MSG {s} 1 2\r\n", .{reply_to}) catch return;
                    stream.writeAll(frame) catch return;
                    stream.writeAll("ok\r\n") catch return;
                } else {
                    if (!std.mem.eql(u8, publish.subject, "svc.echo")) {
                        std.debug.panic("unexpected publish subject: {s}", .{publish.subject});
                    }
                    var header: [128]u8 = undefined;
                    const frame = std.fmt.bufPrint(&header, "MSG {s} 1 {d}\r\n", .{ publish.subject, publish.payload.len }) catch return;
                    stream.writeAll(frame) catch return;
                    stream.writeAll(publish.payload) catch return;
                    stream.writeAll("\r\n") catch return;
                }
            },
        }
    }
}

fn runMockBroker(stream: std.net.Stream) void {
    mockBroker(stream, std.testing.allocator);
}

test "client frame reader parses info pong and msg" {
    var reader = client_mod.FrameReader.init(std.testing.allocator);
    defer reader.deinit();

    try reader.feed("INFO {\"server_id\":\"zigbee\",\"version\":\"0.1.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":4222,\"max_payload\":1048576}\r\nPONG\r\nMSG inbox 2 _INBOX.reply 4\r\ntest\r\n");

    const info = try reader.next();
    try std.testing.expect(info != null);
    switch (info.?) {
        .info => |text| try std.testing.expect(std.mem.startsWith(u8, text, "{\"server_id\":\"zigbee\"")),
        else => return error.UnexpectedFrame,
    }

    const pong = try reader.next();
    try std.testing.expect(pong != null);
    try std.testing.expect(pong.? == .pong);

    const msg = try reader.next();
    try std.testing.expect(msg != null);
    switch (msg.?) {
        .msg => |frame| {
            try std.testing.expectEqualStrings("inbox", frame.subject);
            try std.testing.expectEqual(@as(u64, 2), frame.sid);
            try std.testing.expectEqualStrings("_INBOX.reply", frame.reply.?);
            try std.testing.expectEqualStrings("test", frame.payload);
        },
        else => return error.UnexpectedFrame,
    }
}

test "client transport over socketpair" {
    var streams = try makeSocketPair();
    errdefer streams[0].close();
    errdefer streams[1].close();

    const server_thread = try std.Thread.spawn(.{}, runMockBroker, .{streams[1]});
    defer server_thread.join();

    var client = try client_mod.Client.fromStream(std.testing.allocator, streams[0]);
    defer client.deinit();

    try client.ping();

    try client.subscribe("svc.echo", null, 1);
    try client.publish("svc.echo", null, "hello");
    const message = try client.waitForMessage("svc.echo");
    try std.testing.expectEqualStrings("hello", message.payload);

    const inbox = try client_mod.makeInbox(std.testing.allocator, "request");
    defer std.testing.allocator.free(inbox);
    try client.publish("svc.request", inbox, "who are you?");
    const reply = try client.waitForMessage(inbox);
    try std.testing.expectEqualStrings("ok", reply.payload);

    try client.unsubscribe(1, null);
}
