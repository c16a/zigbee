// SPDX-License-Identifier: MIT
const std = @import("std");
const client_mod = @import("client.zig");
const broker_mod = @import("broker.zig");
const protocol = @import("protocol.zig");
const server_mod = @import("server.zig");

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

fn readLine(stream: std.net.Stream, allocator: std.mem.Allocator) ![]u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(allocator);

    var buf: [1]u8 = undefined;
    while (true) {
        const n = try stream.read(&buf);
        if (n == 0) return error.EndOfStream;
        try list.appendSlice(allocator, buf[0..n]);
        if (buf[0] == '\n') {
            return try list.toOwnedSlice(allocator);
        }
    }
}

fn readFrame(stream: std.net.Stream, allocator: std.mem.Allocator) ![]u8 {
    const line = try readLine(stream, allocator);
    defer allocator.free(line);
    const trimmed = std.mem.trimRight(u8, line, "\r\n");
    var it = std.mem.tokenizeScalar(u8, trimmed, ' ');
    const kind = it.next() orelse return error.InvalidFrame;
    if (!std.mem.eql(u8, kind, "MSG")) return error.InvalidFrame;
    _ = it.next() orelse return error.InvalidFrame;
    _ = it.next() orelse return error.InvalidFrame;
    const maybe_reply_or_len = it.next() orelse return error.InvalidFrame;
    const len_text = if (it.next()) |len_value| len_value else maybe_reply_or_len;
    const payload_len = try std.fmt.parseInt(usize, len_text, 10);

    const payload = try allocator.alloc(u8, payload_len);
    errdefer allocator.free(payload);
    try readExact(stream, payload);

    var crlf: [2]u8 = undefined;
    try readExact(stream, &crlf);
    try std.testing.expectEqualStrings("\r\n", crlf[0..]);
    return payload;
}

fn readExact(stream: std.net.Stream, bytes: []u8) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        const n = try stream.read(bytes[offset..]);
        if (n == 0) return error.EndOfStream;
        offset += n;
    }
}

test "end to end pub sub unsubscribe and request reply" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);

    var thread_safe = std.heap.ThreadSafeAllocator{
        .child_allocator = gpa.allocator(),
    };
    const allocator = thread_safe.allocator();

    var broker = broker_mod.Broker.init(allocator);
    defer broker.deinit();

    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try server_mod.Server.start(allocator, &broker, address);

    const serve_thread = try std.Thread.spawn(.{}, server_mod.Server.serve, .{ &server });
    defer serve_thread.join();
    defer server.deinit();
    std.Thread.sleep(10 * std.time.ns_per_ms);

    const port = server.listener.listen_address.getPort();
    const client_address = try std.net.Address.parseIp("127.0.0.1", port);

    var subscriber = try std.net.tcpConnectToAddress(client_address);
    defer subscriber.close();
    var publisher = try std.net.tcpConnectToAddress(client_address);
    defer publisher.close();

    const subscriber_info = try readLine(subscriber, allocator);
    defer allocator.free(subscriber_info);
    const publisher_info = try readLine(publisher, allocator);
    defer allocator.free(publisher_info);

    try subscriber.writeAll("SUB foo 1\r\n");
    try subscriber.writeAll("PING\r\n");
    const subscriber_pong = try readLine(subscriber, allocator);
    defer allocator.free(subscriber_pong);
    try std.testing.expectEqualStrings("PONG\r\n", subscriber_pong);
    try publisher.writeAll("PUB foo 5\r\nhello\r\n");

    const delivered = try readFrame(subscriber, allocator);
    defer allocator.free(delivered);
    try std.testing.expectEqualStrings("hello", delivered);

    try subscriber.writeAll("UNSUB 1\r\n");
    try subscriber.writeAll("PING\r\n");
    const unsub_pong = try readLine(subscriber, allocator);
    defer allocator.free(unsub_pong);
    try std.testing.expectEqualStrings("PONG\r\n", unsub_pong);
    try publisher.writeAll("PUB foo 5\r\nagain\r\n");

    var poll_fds = [_]std.posix.pollfd{
        .{ .fd = subscriber.handle, .events = std.posix.POLL.IN, .revents = 0 },
    };
    try std.testing.expectEqual(@as(usize, 0), try std.posix.poll(&poll_fds, 0));

    try subscriber.writeAll("SUB inbox 2\r\n");
    try subscriber.writeAll("PING\r\n");
    const inbox_pong = try readLine(subscriber, allocator);
    defer allocator.free(inbox_pong);
    try std.testing.expectEqualStrings("PONG\r\n", inbox_pong);
    try publisher.writeAll("SUB _INBOX.reply 3\r\n");
    try publisher.writeAll("PING\r\n");
    const reply_pong = try readLine(publisher, allocator);
    defer allocator.free(reply_pong);
    try std.testing.expectEqualStrings("PONG\r\n", reply_pong);
    try publisher.writeAll("PUB inbox _INBOX.reply 4\r\ntest\r\n");

    const request_msg = try readLine(subscriber, allocator);
    defer allocator.free(request_msg);
    try std.testing.expect(std.mem.startsWith(u8, request_msg, "MSG inbox 2 _INBOX.reply 4"));

    try subscriber.writeAll("PUB _INBOX.reply 2\r\nok\r\n");
    const reply = try readFrame(publisher, allocator);
    defer allocator.free(reply);
    try std.testing.expectEqualStrings("ok", reply);
}

test "client handshake ping and pub sub" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer std.debug.assert(gpa.deinit() == .ok);

    var thread_safe = std.heap.ThreadSafeAllocator{
        .child_allocator = gpa.allocator(),
    };
    const allocator = thread_safe.allocator();

    var broker = broker_mod.Broker.init(allocator);
    defer broker.deinit();

    const address = try std.net.Address.parseIp("127.0.0.1", 0);
    var server = try server_mod.Server.start(allocator, &broker, address);

    const serve_thread = try std.Thread.spawn(.{}, server_mod.Server.serve, .{ &server });
    defer serve_thread.join();
    defer server.deinit();
    std.Thread.sleep(10 * std.time.ns_per_ms);

    const port = server.listener.listen_address.getPort();
    const broker_address = try std.net.Address.parseIp("127.0.0.1", port);

    var pinger = try client_mod.Client.connect(allocator, broker_address);
    defer pinger.deinit();
    try pinger.ping();

    const subject = "svc.echo";
    var subscriber = try client_mod.Client.connect(allocator, broker_address);
    defer subscriber.deinit();
    try subscriber.subscribe(subject, null, 1);
    try subscriber.ping();

    var publisher = try client_mod.Client.connect(allocator, broker_address);
    defer publisher.deinit();
    try publisher.publish(subject, null, "hello");

    const message = try subscriber.waitForMessage(subject);
    try std.testing.expectEqualStrings("hello", message.payload);
}
