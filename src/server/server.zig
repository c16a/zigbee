// SPDX-License-Identifier: MIT
const std = @import("std");
const broker_mod = @import("broker.zig");
const cluster_mod = @import("cluster.zig");
const auth_mod = @import("auth.zig");
const config_mod = @import("config.zig");
const common_mod = @import("common_client");
const crypto_mod = common_mod.crypto_mod;
const session = @import("session.zig");
const protocol = common_mod.protocol_mod;

pub const Server = struct {
    allocator: std.mem.Allocator,
    broker: *broker_mod.Broker,
    auth: ?auth_mod.Store,
    cluster: ?*cluster_mod.Cluster,
    verbose: bool,
    listener: std.net.Server,
    sessions_mutex: std.Thread.Mutex = .{},
    sessions: std.AutoHashMap(u64, *session.Session),
    all_sessions: std.ArrayList(*session.Session) = .empty,
    client_threads: std.ArrayList(std.Thread) = .empty,
    next_session_id: u64 = 1,
    cluster_delivery_count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn start(allocator: std.mem.Allocator, broker: *broker_mod.Broker, address: std.net.Address, auth_cfg: ?config_mod.Auth, cluster_cfg: config_mod.Cluster, verbose: bool) !*Server {
        const server = try allocator.create(Server);
        errdefer allocator.destroy(server);

        server.* = .{
            .allocator = allocator,
            .broker = broker,
            .auth = null,
            .cluster = null,
            .verbose = verbose,
            .listener = try address.listen(.{ .reuse_address = true }),
            .sessions = std.AutoHashMap(u64, *session.Session).init(allocator),
        };
        errdefer server.listener.deinit();
        if (auth_cfg) |cfg| {
            server.auth = try auth_mod.Store.initFromConfig(allocator, cfg);
        }
        errdefer if (server.auth) |*auth| auth.deinit();
        const cluster = try allocator.create(cluster_mod.Cluster);
        errdefer allocator.destroy(cluster);
        cluster.* = try cluster_mod.Cluster.init(allocator, cluster_cfg, @ptrCast(server), deliverClusterMessage);
        server.cluster = cluster;
        errdefer if (server.cluster) |cluster_ptr| {
            cluster_ptr.deinit();
            allocator.destroy(cluster_ptr);
        };
        try cluster.startThread();
        return server;
    }

    pub fn deinit(self: *Server) void {
        self.listener.deinit();

        if (self.cluster) |cluster| {
            cluster.deinit();
            self.allocator.destroy(cluster);
        }

        self.sessions_mutex.lock();
        for (self.all_sessions.items) |s| {
            s.close();
        }
        self.sessions_mutex.unlock();

        for (self.client_threads.items) |thread| {
            thread.join();
        }

        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        if (self.auth) |*auth| {
            auth.deinit();
        }
        for (self.all_sessions.items) |s| {
            self.allocator.destroy(s);
        }
        self.all_sessions.deinit(self.allocator);
        self.sessions.deinit();
        self.client_threads.deinit(self.allocator);
    }

    pub fn serve(self: *Server) !void {
        while (true) {
            const connection = self.listener.accept() catch |err| switch (err) {
                else => return,
            };

            const s = try self.makeSession(connection.stream);
            self.registerSession(s);

            const thread = try std.Thread.spawn(.{}, clientMain, .{ self, s });
            self.client_threads.append(self.allocator, thread) catch |err| {
                s.close();
                self.unregisterSession(s.id);
                return err;
            };
        }
    }

    pub fn clusterDeliveryCount(self: *Server) u64 {
        return self.cluster_delivery_count.load(.acquire);
    }

    fn makeSession(self: *Server, stream: std.net.Stream) !*session.Session {
        const s = try self.allocator.create(session.Session);
        s.* = .{
            .id = self.next_session_id,
            .stream = stream,
        };
        self.next_session_id += 1;
        return s;
    }

    fn registerSession(self: *Server, s: *session.Session) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        self.sessions.put(s.id, s) catch @panic("OOM");
        self.all_sessions.append(self.allocator, s) catch @panic("OOM");
    }

    fn unregisterSession(self: *Server, session_id: u64) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        _ = self.sessions.remove(session_id);
    }

    fn lookupSession(self: *Server, session_id: u64) ?*session.Session {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();
        return self.sessions.get(session_id);
    }

    fn sendErr(self: *Server, session_id: u64, msg: []const u8) void {
        var frame: [256]u8 = undefined;
        const frame_slice = std.fmt.bufPrint(&frame, "-ERR '{s}'\r\n", .{msg}) catch return;
        self.writeSessionFrame(session_id, &[_][]const u8{frame_slice});
    }

    fn sendMsg(self: *Server, session_id: u64, frame: protocol.OutgoingFrame) void {
        var buf: [256]u8 = undefined;
        switch (frame) {
            .info => |info| {
                const json = protocol.formatInfoJson(self.allocator, info) catch return;
                defer self.allocator.free(json);
                self.writeSessionFrame(session_id, &[_][]const u8{ "INFO ", json, "\r\n" });
            },
            .ok => self.writeSessionFrame(session_id, &[_][]const u8{"OK\r\n"}),
            .pong => self.writeSessionFrame(session_id, &[_][]const u8{"PONG\r\n"}),
            .close => self.writeSessionFrame(session_id, &[_][]const u8{"CLOSE\r\n"}),
            .msg => |msg| {
                const header = formatMsgHeader(&buf, msg) catch return;
                self.writeSessionFrame(msg.session_id, &[_][]const u8{ header, msg.payload, "\r\n" });
            },
        }
    }

    fn sendOk(self: *Server, session_id: u64) void {
        if (self.verbose) self.sendMsg(session_id, .ok);
    }

    fn writeSessionFrame(self: *Server, session_id: u64, parts: []const []const u8) void {
        const s = self.lookupSession(session_id) orelse return;
        s.write_mutex.lock();
        defer s.write_mutex.unlock();
        if (s.closed) return;

        for (parts) |part| {
            s.stream.writeAll(part) catch {
                s.close();
                self.unregisterSession(session_id);
                return;
            };
        }
    }
};

fn formatMsgHeader(buf: []u8, msg: protocol.OutgoingMessage) ![]const u8 {
    return if (msg.reply) |reply_to| blk: {
        break :blk std.fmt.bufPrint(buf, "MSG {s} {d} {s} {d}\r\n", .{ msg.subject, msg.sid, reply_to, msg.payload.len });
    } else blk: {
        break :blk std.fmt.bufPrint(buf, "MSG {s} {d} {d}\r\n", .{ msg.subject, msg.sid, msg.payload.len });
    };
}

fn deliverClusterMessage(ctx: *anyopaque, delivery: cluster_mod.Delivery, subject: []const u8, reply: ?[]const u8, payload: []const u8) void {
    const server: *Server = @ptrCast(@alignCast(ctx));
    _ = server.cluster_delivery_count.fetchAdd(1, .acq_rel);
    server.sendMsg(delivery.session_id, .{ .msg = .{
        .session_id = delivery.session_id,
        .sid = delivery.sid,
        .subject = subject,
        .reply = reply,
        .payload = payload,
    } });
}

fn clientMain(server: *Server, s: *session.Session) void {
    defer s.close();
    defer server.broker.unsubscribeSession(s.id);
    defer server.unregisterSession(s.id);
    defer if (server.cluster) |cluster| cluster.unsubscribeSession(s.id) catch {};

    const auth_required = server.auth != null;
    var auth_done = std.atomic.Value(bool).init(false);

    var auth_watchdog: ?std.Thread = null;
    if (auth_required) {
        auth_watchdog = std.Thread.spawn(.{}, authDeadlineWatcher, .{
            server,
            s,
            s.id,
            &auth_done,
        }) catch return;
        defer if (auth_watchdog) |thread| thread.join();
        defer auth_done.store(true, .release);
    }

    var nonce: [crypto_mod.seed_length]u8 = undefined;
    if (auth_required) {
        std.crypto.random.bytes(&nonce);
    }

    var nonce_text: ?[]u8 = null;
    defer if (nonce_text) |text| server.allocator.free(text);
    if (auth_required) {
        nonce_text = crypto_mod.encodeBase64(server.allocator, nonce[0..]) catch return;
    }

    server.sendMsg(s.id, .{ .info = .{
        .port = server.listener.listen_address.getPort(),
        .auth_required = auth_required,
        .auth_mode = if (auth_required) "zkey" else "none",
        .nonce = nonce_text,
    } });

    var reader = protocol.Reader.init(server.allocator);
    defer reader.deinit();

    var buffer: [4096]u8 = undefined;
    var saw_connect = false;
    var authenticated = !auth_required;
    var principal: ?*const auth_mod.Principal = null;

    while (true) {
        while (reader.next() catch |err| {
            server.sendErr(s.id, @errorName(err));
            return;
        }) |command| {
            switch (command) {
                .connect => |connect_json| {
                    if (saw_connect) {
                        server.sendErr(s.id, "duplicate CONNECT");
                        server.sendMsg(s.id, .close);
                        return;
                    }
                    saw_connect = true;

                    const parsed = std.json.parseFromSlice(protocol.Connect, server.allocator, connect_json, .{
                        .allocate = .alloc_always,
                        .ignore_unknown_fields = true,
                    }) catch {
                        server.sendErr(s.id, "auth failed");
                        server.sendMsg(s.id, .close);
                        return;
                    };
                    defer parsed.deinit();

                    if (auth_required) {
                        if (!std.mem.eql(u8, parsed.value.auth_mode, "zkey")) {
                            server.sendErr(s.id, "auth failed");
                            server.sendMsg(s.id, .close);
                            return;
                        }

                        const store = server.auth.?;
                        const public_key_text = parsed.value.public_key orelse {
                            server.sendErr(s.id, "auth failed");
                            server.sendMsg(s.id, .close);
                            return;
                        };
                        const signature_text = parsed.value.signature orelse {
                            server.sendErr(s.id, "auth failed");
                            server.sendMsg(s.id, .close);
                            return;
                        };

                        const public_key = auth_mod.decodePublicKey(public_key_text) catch {
                            server.sendErr(s.id, "invalid public key");
                            server.sendMsg(s.id, .close);
                            return;
                        };
                        const signature = auth_mod.decodeSignature(signature_text) catch {
                            server.sendErr(s.id, "invalid signature");
                            server.sendMsg(s.id, .close);
                            return;
                        };

                        principal = store.lookupByPublicKey(public_key.bytes) orelse {
                            server.sendErr(s.id, "unknown key");
                            server.sendMsg(s.id, .close);
                            return;
                        };
                        if (!auth_mod.verifyNonceSignature(public_key, nonce[0..], signature)) {
                            server.sendErr(s.id, "invalid signature");
                            server.sendMsg(s.id, .close);
                            return;
                        }
                    }

                    authenticated = true;
                    auth_done.store(true, .release);
                    server.sendOk(s.id);
                },
                .ping => {
                    if (!authenticated) {
                        server.sendErr(s.id, "auth required");
                        if (auth_required) server.sendMsg(s.id, .close);
                        return;
                    }
                    server.sendMsg(s.id, .pong);
                    server.sendOk(s.id);
                },
                .pong => {
                    if (!authenticated) {
                        server.sendErr(s.id, "auth required");
                        if (auth_required) server.sendMsg(s.id, .close);
                        return;
                    }
                    server.sendOk(s.id);
                },
                .sub => |sub| {
                    if (auth_required and !authenticated) {
                        server.sendErr(s.id, "auth required");
                        server.sendMsg(s.id, .close);
                        return;
                    }
                    if (server.auth) |_| {
                        const current = principal orelse {
                            server.sendErr(s.id, "auth required");
                            return;
                        };
                        if (!auth_mod.canSubscribe(&current.permissions, sub.subject)) {
                            server.sendErr(s.id, "permission denied");
                            return;
                        }
                    }
                    if (server.cluster) |cluster| {
                        cluster.subscribe(s.id, sub.sid, sub.subject, sub.queue) catch |err| {
                            server.sendErr(s.id, @errorName(err));
                            return;
                        };
                    }
                    server.broker.subscribe(s.id, sub.sid, sub.subject, sub.queue) catch |err| {
                        server.sendErr(s.id, @errorName(err));
                        return;
                    };
                    server.sendOk(s.id);
                },
                .unsub => |unsub| {
                    if (auth_required and !authenticated) {
                        server.sendErr(s.id, "auth required");
                        server.sendMsg(s.id, .close);
                        return;
                    }
                    if (server.cluster) |cluster| {
                        cluster.unsubscribe(s.id, unsub.sid, unsub.max) catch |err| {
                            server.sendErr(s.id, @errorName(err));
                            return;
                        };
                    }
                    server.broker.unsubscribe(s.id, unsub.sid, unsub.max);
                    server.sendOk(s.id);
                },
                .publish => |publish| {
                    if (auth_required and !authenticated) {
                        server.sendErr(s.id, "auth required");
                        server.sendMsg(s.id, .close);
                        return;
                    }
                    if (server.auth) |_| {
                        const current = principal orelse {
                            server.sendErr(s.id, "auth required");
                            return;
                        };
                        if (!auth_mod.canPublish(&current.permissions, publish.subject)) {
                            server.sendErr(s.id, "permission denied");
                            return;
                        }
                    }
                    const cluster = server.cluster orelse {
                        server.sendErr(s.id, "cluster unavailable");
                        return;
                    };
                    const deliveries = cluster.publish(publish.subject, publish.reply, publish.payload) catch |err| {
                        server.sendErr(s.id, @errorName(err));
                        return;
                    };
                    defer server.allocator.free(deliveries);
                    for (deliveries) |delivery| {
                        server.sendMsg(delivery.session_id, .{ .msg = .{
                            .session_id = delivery.session_id,
                            .sid = delivery.sid,
                            .subject = publish.subject,
                            .reply = publish.reply,
                            .payload = publish.payload,
                        } });
                    }
                    server.sendOk(s.id);
                },
            }
        }

        const read_len = s.stream.read(&buffer) catch break;
        if (read_len == 0) break;
        reader.feed(buffer[0..read_len]) catch break;
    }
}

fn authDeadlineWatcher(server: *Server, s: *session.Session, session_id: u64, done: *std.atomic.Value(bool)) void {
    const deadline = std.time.nanoTimestamp() + 5 * std.time.ns_per_s;
    while (std.time.nanoTimestamp() < deadline) {
        if (done.load(.acquire)) return;
        std.Thread.sleep(50 * std.time.ns_per_ms);
    }

    if (done.load(.acquire)) return;
    server.sendErr(session_id, "auth timeout");
    server.sendMsg(session_id, .close);
    s.close();
}
