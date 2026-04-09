// SPDX-License-Identifier: MIT
const std = @import("std");
const posix = std.posix;
const auth_mod = @import("server/auth.zig");
const config_mod = @import("server/config.zig");
const cluster_mod = @import("server/cluster.zig");
const client_mod = @import("common_client");
const crypto_mod = client_mod.crypto_mod;
const broker_mod = @import("server/broker.zig");
const protocol = client_mod.protocol_mod;

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
    try std.testing.expect(loaded.?.value().verbose);
}

test "config verbose can be disabled" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{
        .sub_path = "zigbee.config.json",
        .data = "{\"verbose\":false}",
    });

    var loaded = try config_mod.loadFromDir(tmp.dir, allocator, "zigbee.config.json", true);
    try std.testing.expect(loaded != null);
    defer loaded.?.deinit();
    try std.testing.expect(!loaded.?.value().verbose);
}

test "config cluster settings load" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(.{
        .sub_path = "zigbee.config.json",
        .data = "{\"cluster\":{\"bind_address\":\"10.0.0.1:4333\",\"peer_addresses\":[\"10.0.0.2:4333\",\"10.0.0.3:4333\"],\"heartbeat_interval_ms\":2500}}",
    });

    var loaded = try config_mod.loadFromDir(tmp.dir, allocator, "zigbee.config.json", true);
    try std.testing.expect(loaded != null);
    defer loaded.?.deinit();
    try std.testing.expectEqualStrings("10.0.0.1:4333", loaded.?.value().cluster.bind_address.?);
    try std.testing.expectEqual(@as(usize, 2), loaded.?.value().cluster.peer_addresses.len);
    try std.testing.expectEqualStrings("10.0.0.2:4333", loaded.?.value().cluster.peer_addresses[0]);
    try std.testing.expectEqual(@as(u32, 2500), loaded.?.value().cluster.heartbeat_interval_ms);
}

test "cluster packet envelope roundtrips" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const packet = protocol.ClusterPacket{
        .kind = "publish",
        .origin_id = 123,
        .seq = 456,
        .publish = .{
            .subject = "foo.bar",
            .reply = "inbox",
            .payload_b64 = "aGVsbG8=",
        },
    };
    const json = try protocol.formatClusterPacketJson(allocator, packet);
    defer allocator.free(json);

    const parsed = try std.json.parseFromSlice(protocol.ClusterPacket, allocator, json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    try std.testing.expectEqualStrings("publish", parsed.value.kind);
    try std.testing.expectEqual(@as(u64, 123), parsed.value.origin_id);
    try std.testing.expectEqualStrings("foo.bar", parsed.value.publish.?.subject);
}

test "cluster retries dropped publish until ack" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var sender = try cluster_mod.Cluster.init(allocator, .{
        .bind_address = "127.0.0.1:49422",
        .peer_addresses = &.{},
    }, null, null);
    defer sender.deinit();
    try sender.startThread();

    const sender_address = try std.net.Address.parseIp("127.0.0.1", 49422);

    var peer = try UdpPeer.bind("127.0.0.1", 0, sender_address);

    const peer_id = 0xfeedfacecafebeef;
    try peer.sendPacket(allocator, .{
        .kind = "sub",
        .origin_id = peer_id,
        .seq = 1,
        .subscription = .{
            .session_id = 1,
            .sid = 1,
            .subject = "jobs.>",
            .queue = "workers",
        },
    });

    var sub_ack = try peer.waitForPacket(allocator, "ack", 500) orelse return error.ExpectedAck;
    defer sub_ack.deinit(allocator);

    const peer_address = peer.address;
    peer.deinit();

    const deliveries = try sender.publish("jobs.1", null, "payload");
    defer allocator.free(deliveries);
    try std.testing.expectEqual(@as(usize, 0), deliveries.len);

    std.Thread.sleep(50 * std.time.ns_per_ms);

    var retry_peer = try UdpPeer.bindAddress(peer_address, sender_address);
    defer retry_peer.deinit();

    var publish = try retry_peer.waitForPacket(allocator, "publish", 1500) orelse return error.ExpectedRetryPacket;
    defer publish.deinit(allocator);
    try std.testing.expectEqualStrings("jobs.1", publish.publish_subject.?);
    const payload_text = try retry_peer.payloadText(allocator, publish.publish_payload_b64.?);
    defer allocator.free(payload_text);
    try std.testing.expectEqualStrings("payload", payload_text);

    try retry_peer.sendPacket(allocator, .{
        .kind = "ack",
        .origin_id = peer_id,
        .seq = 2,
        .ack_origin_id = publish.origin_id,
        .ack_seq = publish.seq,
    });

    const no_retry = try retry_peer.waitForPacket(allocator, "publish", 350);
    try std.testing.expect(no_retry == null);
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

const UdpPeer = struct {
    fd: posix.socket_t,
    address: std.net.Address,
    remote_address: std.net.Address,

    fn bind(ip: []const u8, port: u16, remote_address: std.net.Address) !UdpPeer {
        const address = try std.net.Address.parseIp(ip, port);
        return try bindAddress(address, remote_address);
    }

    fn bindAddress(address: std.net.Address, remote_address: std.net.Address) !UdpPeer {
        const fd = try posix.socket(
            address.any.family,
            posix.SOCK.DGRAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK,
            posix.IPPROTO.UDP,
        );
        errdefer posix.close(fd);
        try posix.bind(fd, &address.any, address.getOsSockLen());

        var bound = address;
        var len: posix.socklen_t = @sizeOf(posix.sockaddr);
        try posix.getsockname(fd, &bound.any, &len);
        return .{ .fd = fd, .address = bound, .remote_address = remote_address };
    }

    fn deinit(self: *UdpPeer) void {
        posix.close(self.fd);
    }

    fn sendPacket(self: *UdpPeer, allocator: std.mem.Allocator, packet: protocol.ClusterPacket) !void {
        const json = try protocol.formatClusterPacketJson(allocator, packet);
        defer allocator.free(json);
        _ = try posix.sendto(self.fd, json, 0, &self.remote_address.any, self.remote_address.getOsSockLen());
    }

    fn waitForPacket(self: *UdpPeer, allocator: std.mem.Allocator, kind: []const u8, timeout_ms: u64) !?ObservedPacket {
        const deadline = std.time.nanoTimestamp() + @as(i128, timeout_ms) * std.time.ns_per_ms;
        var scratch: [65535]u8 = undefined;

        while (std.time.nanoTimestamp() < deadline) {
            var src_addr: std.net.Address = undefined;
            var src_len: posix.socklen_t = @sizeOf(posix.sockaddr);
            const len = posix.recvfrom(self.fd, scratch[0..], 0, &src_addr.any, &src_len) catch |err| switch (err) {
                error.WouldBlock => {
                    std.Thread.sleep(10 * std.time.ns_per_ms);
                    continue;
                },
                else => return err,
            };
            if (len == 0) continue;

            const parsed = try std.json.parseFromSlice(protocol.ClusterPacket, allocator, scratch[0..len], .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();
            if (std.mem.eql(u8, parsed.value.kind, kind)) {
                const observed = ObservedPacket{
                    .kind = try allocator.dupe(u8, parsed.value.kind),
                    .origin_id = parsed.value.origin_id,
                    .seq = parsed.value.seq,
                    .publish_subject = if (parsed.value.publish) |publish| try allocator.dupe(u8, publish.subject) else null,
                    .publish_payload_b64 = if (parsed.value.publish) |publish| try allocator.dupe(u8, publish.payload_b64) else null,
                    .ack_origin_id = parsed.value.ack_origin_id,
                    .ack_seq = parsed.value.ack_seq,
                };
                return observed;
            }
        }

        return null;
    }

    fn payloadText(self: *UdpPeer, allocator: std.mem.Allocator, payload_b64: []const u8) ![]u8 {
        _ = self;
        const out_len = std.base64.standard.Decoder.calcSizeForSlice(payload_b64) catch |err| switch (err) {
            error.InvalidCharacter, error.InvalidPadding => return error.InvalidBase64,
            else => return err,
        };
        const out = try allocator.alloc(u8, out_len);
        try std.base64.standard.Decoder.decode(out, payload_b64);
        return out;
    }
};

const ObservedPacket = struct {
    kind: []u8,
    origin_id: u64,
    seq: u64,
    publish_subject: ?[]u8 = null,
    publish_payload_b64: ?[]u8 = null,
    ack_origin_id: ?u64 = null,
    ack_seq: ?u64 = null,

    fn deinit(self: *ObservedPacket, allocator: std.mem.Allocator) void {
        allocator.free(self.kind);
        if (self.publish_subject) |subject| allocator.free(subject);
        if (self.publish_payload_b64) |payload| allocator.free(payload);
    }
};

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
            stream.writeAll("OK\r\n") catch return;
        },
        else => return,
    }

    const ping_cmd = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (ping_cmd orelse return) {
        .ping => {
            stream.writeAll("PONG\r\n") catch return;
            stream.writeAll("OK\r\n") catch return;
        },
        else => return,
    }

    const first_sub = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (first_sub orelse return) {
        .sub => |sub| {
            if (!std.mem.eql(u8, sub.subject, "svc.echo")) @panic("mockBroker: expected svc.echo subscription");
            stream.writeAll("OK\r\n") catch return;
        },
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
            stream.writeAll("OK\r\n") catch return;
        },
        else => return,
    }

    const inbox_sub = readProtocolCommand(&reader, stream, &scratch) catch return;
    const inbox_subject = blk: {
        switch (inbox_sub orelse return) {
            .sub => |sub| {
                stream.writeAll("OK\r\n") catch return;
                break :blk allocator.dupe(u8, sub.subject) catch return;
            },
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
            stream.writeAll("OK\r\n") catch return;
        },
        else => return,
    }

    const unsub_cmd = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (unsub_cmd orelse return) {
        .unsub => |unsub| {
            if (unsub.sid != 1) @panic("mockBroker: expected unsubscribe sid 1");
            stream.writeAll("OK\r\n") catch return;
        },
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
        .sub => |sub| {
            if (!std.mem.eql(u8, sub.subject, "svc.echo")) @panic("mockOpenBroker: expected svc.echo subscription");
            stream.writeAll("OK\r\n") catch return;
        },
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
            stream.writeAll("OK\r\n") catch return;
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
    try reader.feed("\r\nOK\r\nPONG\r\nOK\r\nMSG inbox 2 _INBOX.reply 4\r\ntest\r\n");

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

    const ok = try reader.next();
    try std.testing.expect(ok != null);
    try std.testing.expect(ok.? == .ok);

    const pong = try reader.next();
    try std.testing.expect(pong != null);
    try std.testing.expect(pong.? == .pong);

    const ok2 = try reader.next();
    try std.testing.expect(ok2 != null);
    try std.testing.expect(ok2.? == .ok);

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

    try reader.feed("CLOSE\r\n");
    const close_frame = try reader.next();
    try std.testing.expect(close_frame != null);
    try std.testing.expect(close_frame.? == .close);
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
                .ok => continue,
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
    stream.writeAll("OK\r\n") catch return;

    const ping_cmd = readProtocolCommand(&reader, stream, &scratch) catch return;
    switch (ping_cmd orelse return) {
        .ping => {
            stream.writeAll("PONG\r\n") catch return;
            stream.writeAll("OK\r\n") catch return;
        },
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
