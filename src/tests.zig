// SPDX-License-Identifier: MIT
const std = @import("std");
const auth_mod = @import("auth.zig");
const config_mod = @import("config.zig");
const client_mod = @import("client.zig");
const crypto_mod = @import("crypto.zig");
const broker_mod = @import("broker.zig");
const protocol = @import("protocol.zig");

test "protocol partial parsing and malformed frames" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var reader = protocol.Reader.init(allocator);
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
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var broker = broker_mod.Broker.init(allocator);
    defer broker.deinit();

    try broker.subscribe(1, 1, "foo", "queue");
    try broker.subscribe(2, 2, "foo", "queue");
    try broker.subscribe(3, 3, "foo", null);

    const first = try broker.publish("foo");
    defer allocator.free(first);
    try std.testing.expectEqual(@as(usize, 2), first.len);
    try std.testing.expectEqual(@as(u64, 3), first[0].sid);
    try std.testing.expect(first[1].sid == 1 or first[1].sid == 2);

    broker.unsubscribe(3, 3, null);
    const second = try broker.publish("foo");
    defer allocator.free(second);
    try std.testing.expectEqual(@as(usize, 1), second.len);
}

test "config file loads listen address" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{
        .sub_path = "zigbee.config.json",
        .data = "{\"listen_address\":\"127.0.0.1:4222\"}",
    });

    var loaded = try config_mod.loadFromDir(tmp.dir, allocator, "zigbee.config.json", true);
    try std.testing.expect(loaded != null);
    defer loaded.?.deinit();
    try std.testing.expectEqualStrings("127.0.0.1:4222", loaded.?.value().listen_address.?);
}

test "auth store validates keys and permissions" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const seed = [_]u8{0x11} ** crypto_mod.seed_length;
    const key_pair = try crypto_mod.keyPairFromSeed(seed);
    const public_key = key_pair.public_key.toBytes();
    const public_key_b64 = try crypto_mod.encodeBase64(allocator, public_key[0..]);
    defer allocator.free(public_key_b64);

    var store = try auth_mod.Store.initFromConfig(allocator, .{
        .mode = "static_zkey",
        .users = &.{
            .{
                .name = "svc",
                .public_key = public_key_b64,
                .allow_publish = &.{ "events.>", "rpc.requests" },
                .allow_subscribe = &.{ "rpc.replies", "updates.>" },
            },
        },
    });
    defer store.deinit();

    const principal = store.lookupByPublicKey(public_key) orelse return error.UnexpectedNull;
    try std.testing.expectEqualStrings("svc", principal.name.?);
    try std.testing.expect(auth_mod.canPublish(&principal.permissions, "events.foo"));
    try std.testing.expect(!auth_mod.canPublish(&principal.permissions, "metrics.foo"));
    try std.testing.expect(auth_mod.canSubscribe(&principal.permissions, "updates.one"));
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
    const info_json = protocol.formatInfoJson(allocator, .{ .port = 0 }) catch return;
    defer allocator.free(info_json);
    stream.writeAll("INFO ") catch return;
    stream.writeAll(info_json) catch return;
    stream.writeAll("\r\n") catch return;

    const connect_cmd = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (connect_cmd orelse return) {
        .connect => |json| {
            const parsed = std.json.parseFromSlice(protocol.Connect, allocator, json, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            }) catch return;
            defer parsed.deinit();
            if (!std.mem.eql(u8, parsed.value.auth_mode, "none")) @panic("mockBroker: expected auth_mode=none");
        },
        else => return,
    }

    const ping_cmd = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (ping_cmd orelse return) {
        .ping => stream.writeAll("PONG\r\n") catch return,
        else => return,
    }

    const first_sub = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (first_sub orelse return) {
        .sub => |sub| if (!std.mem.eql(u8, sub.subject, "svc.echo")) @panic("mockBroker: expected svc.echo subscription"),
        else => return,
    }

    const first_pub = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (first_pub orelse return) {
        .publish => |publish| {
            if (!std.mem.eql(u8, publish.subject, "svc.echo")) @panic("mockBroker: expected svc.echo publish");
            if (publish.reply != null) @panic("mockBroker: expected publish without reply subject");
            if (!std.mem.eql(u8, publish.payload, "hello")) @panic("mockBroker: expected hello payload");
            var header: [128]u8 = undefined;
            const frame = std.fmt.bufPrint(&header, "MSG {s} 1 {d}\r\n", .{ publish.subject, publish.payload.len }) catch return;
            stream.writeAll(frame) catch return;
            stream.writeAll(publish.payload) catch return;
            stream.writeAll("\r\n") catch return;
        },
        else => return,
    }

    const inbox_sub = readProtocolCommand(&reader, stream, &scratch) catch return;
    const inbox_subject = blk: {
        switch (inbox_sub orelse return) {
            .sub => |sub| break :blk allocator.dupe(u8, sub.subject) catch return,
            else => return,
        }
    };
    defer allocator.free(inbox_subject);

    const request_pub = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (request_pub orelse return) {
        .publish => |publish| {
            if (!std.mem.eql(u8, publish.subject, "svc.request")) {
                @panic("mockBroker: expected svc.request publish");
            }
            if (publish.reply == null or !std.mem.eql(u8, publish.reply.?, inbox_subject)) @panic("mockBroker: expected request reply inbox");
            if (!std.mem.eql(u8, publish.payload, "who are you?")) @panic("mockBroker: expected request payload");
            var header: [128]u8 = undefined;
            const frame = std.fmt.bufPrint(&header, "MSG {s} 1 {d}\r\n", .{ publish.reply.?, 2 }) catch return;
            stream.writeAll(frame) catch return;
            stream.writeAll("ok\r\n") catch return;
        },
        else => return,
    }

    const unsub_cmd = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (unsub_cmd orelse return) {
        .unsub => |unsub| if (unsub.sid != 1) @panic("mockBroker: expected unsubscribe sid 1"),
        else => return,
    }
}

fn runMockBroker(stream: std.net.Stream) void {
    mockBroker(stream, std.heap.page_allocator);
}

fn runMockZkeyBroker(stream: std.net.Stream) void {
    mockZkeyBroker(stream, std.heap.page_allocator);
}

fn mockOpenBroker(stream: std.net.Stream, allocator: std.mem.Allocator) void {
    defer stream.close();

    var reader = protocol.Reader.init(allocator);
    defer reader.deinit();

    var scratch: [4096]u8 = undefined;
    const info_json = protocol.formatInfoJson(allocator, .{ .port = 0 }) catch return;
    defer allocator.free(info_json);
    stream.writeAll("INFO ") catch return;
    stream.writeAll(info_json) catch return;
    stream.writeAll("\r\n") catch return;

    const first_sub = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (first_sub orelse return) {
        .sub => |sub| if (!std.mem.eql(u8, sub.subject, "svc.echo")) @panic("mockOpenBroker: expected svc.echo subscription"),
        else => return,
    }

    const first_pub = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (first_pub orelse return) {
        .publish => |publish| {
            if (!std.mem.eql(u8, publish.subject, "svc.echo")) @panic("mockOpenBroker: expected svc.echo publish");
            if (publish.reply != null) @panic("mockOpenBroker: expected publish without reply subject");
            if (!std.mem.eql(u8, publish.payload, "hello")) @panic("mockOpenBroker: expected hello payload");
            var header: [128]u8 = undefined;
            const frame = std.fmt.bufPrint(&header, "MSG {s} 1 {d}\r\n", .{ publish.subject, publish.payload.len }) catch return;
            stream.writeAll(frame) catch return;
            stream.writeAll(publish.payload) catch return;
            stream.writeAll("\r\n") catch return;
        },
        else => return,
    }
}

test "client frame reader parses info pong and msg" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var reader = client_mod.FrameReader.init(allocator);
    defer reader.deinit();

    const info_json = try protocol.formatInfoJson(allocator, .{ .port = 4222 });
    defer allocator.free(info_json);
    try reader.feed("INFO ");
    try reader.feed(info_json);
    try reader.feed("\r\nPONG\r\nMSG inbox 2 _INBOX.reply 4\r\ntest\r\n");

    const info = try reader.next();
    try std.testing.expect(info != null);
    switch (info.?) {
        .info => |text| {
            const parsed = try std.json.parseFromSlice(protocol.Info, allocator, text, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();
            try std.testing.expectEqual(@as(u16, 4222), parsed.value.port);
            try std.testing.expect(!parsed.value.auth_required);
        },
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
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const streams = try makeSocketPair();

    const server_thread = try std.Thread.spawn(.{}, runMockBroker, .{streams[1]});
    defer server_thread.join();

    var client = try client_mod.Client.fromStream(allocator, streams[0], null);
    defer client.deinit();

    try client.ping();

    try client.subscribe("svc.echo", null, 1);
    try client.publish("svc.echo", null, "hello");
    const message = try client.waitForMessage("svc.echo");
    try std.testing.expectEqualStrings("hello", message.payload);

    const inbox = try client_mod.makeInbox(allocator, "request");
    defer allocator.free(inbox);
    try client.subscribe(inbox, null, 2);
    try client.publish("svc.request", inbox, "who are you?");
    const reply = try client.waitForMessage(inbox);
    try std.testing.expectEqualStrings("ok", reply.payload);

    try client.unsubscribe(1, null);
}

test "unauthenticated raw client can publish and subscribe" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const streams = try makeSocketPair();

    const server_thread = try std.Thread.spawn(.{}, mockOpenBroker, .{ streams[1], allocator });
    defer server_thread.join();

    var reader = client_mod.FrameReader.init(allocator);
    defer reader.deinit();

    var scratch: [4096]u8 = undefined;
    while (true) {
        if (try reader.next()) |frame| {
            switch (frame) {
                .info => break,
                else => return error.UnexpectedFrame,
            }
        }
        const read_len = try streams[0].read(&scratch);
        if (read_len == 0) return error.UnexpectedEndOfStream;
        try reader.feed(scratch[0..read_len]);
    }

    try streams[0].writeAll("SUB svc.echo 1\r\n");
    try streams[0].writeAll("PUB svc.echo 5\r\nhello\r\n");

    while (true) {
        if (try reader.next()) |frame| {
            switch (frame) {
                .msg => |msg| {
                    try std.testing.expectEqualStrings("svc.echo", msg.subject);
                    try std.testing.expectEqualStrings("hello", msg.payload);
                    break;
                },
                else => return error.UnexpectedFrame,
            }
        } else {
            const read_len = try streams[0].read(&scratch);
            if (read_len == 0) return error.UnexpectedEndOfStream;
            try reader.feed(scratch[0..read_len]);
        }
    }
}

fn mockZkeyBroker(stream: std.net.Stream, allocator: std.mem.Allocator) void {
    defer stream.close();

    const seed = [_]u8{0x22} ** crypto_mod.seed_length;
    const key_pair = crypto_mod.keyPairFromSeed(seed) catch return;
    const nonce = [_]u8{0x33} ** crypto_mod.seed_length;
    const nonce_text = crypto_mod.encodeBase64(allocator, nonce[0..]) catch return;
    defer allocator.free(nonce_text);

    const info_json = protocol.formatInfoJson(allocator, .{
        .port = 0,
        .auth_required = true,
        .auth_mode = "zkey",
        .nonce = nonce_text,
    }) catch return;
    defer allocator.free(info_json);

    stream.writeAll("INFO ") catch return;
    stream.writeAll(info_json) catch return;
    stream.writeAll("\r\n") catch return;

    var reader = protocol.Reader.init(allocator);
    defer reader.deinit();

    var scratch: [4096]u8 = undefined;
    const connect_cmd = readProtocolCommand(&reader, stream, &scratch) catch return;
    const connect_json = switch (connect_cmd orelse return) {
        .connect => |json| json,
        else => return,
    };

    const parsed = std.json.parseFromSlice(protocol.Connect, allocator, connect_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    }) catch return;
    defer parsed.deinit();

    if (!std.mem.eql(u8, parsed.value.auth_mode, "zkey")) @panic("mockZkeyBroker: expected auth_mode=zkey");
    const public_key_text = parsed.value.public_key orelse return;
    const signature_text = parsed.value.signature orelse return;
    const decoded_public_key = auth_mod.decodePublicKey(public_key_text) catch return;
    const decoded_signature = auth_mod.decodeSignature(signature_text) catch return;
    if (!auth_mod.verifyNonceSignature(decoded_public_key, nonce[0..], decoded_signature)) @panic("mockZkeyBroker: expected valid signature");

    const expected_public_key = key_pair.public_key.toBytes();
    if (!std.mem.eql(u8, decoded_public_key.bytes[0..], expected_public_key[0..])) @panic("mockZkeyBroker: expected derived public key");

    const ping_cmd = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (ping_cmd orelse return) {
        .ping => stream.writeAll("PONG\r\n") catch return,
        else => return,
    }
}

test "client authenticates with zkey seed" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const streams = try makeSocketPair();

    const server_thread = try std.Thread.spawn(.{}, runMockZkeyBroker, .{streams[1]});
    defer server_thread.join();

    var client = try client_mod.Client.fromStream(
        allocator,
        streams[0],
        try client_mod.Auth.fromSeed([_]u8{0x22} ** crypto_mod.seed_length),
    );
    defer client.deinit();

    try client.ping();
}
