// SPDX-License-Identifier: MIT
const std = @import("std");
const posix = std.posix;
const broker_mod = @import("broker.zig");
const cluster_mod = @import("cluster.zig");
const auth_mod = @import("auth.zig");
const config_mod = @import("config.zig");
const connection_manager = @import("connection_manager.zig");
const common_mod = @import("common_client");
const crypto_mod = common_mod.crypto_mod;
const protocol = common_mod.protocol_mod;
const session = @import("session.zig");

pub const Server = struct {
    allocator: std.mem.Allocator,
    broker: *broker_mod.Broker,
    cluster: ?*cluster_mod.Cluster,
    connections: connection_manager.ConnectionManager,

    pub fn start(
        allocator: std.mem.Allocator,
        broker: *broker_mod.Broker,
        address: std.net.Address,
        auth_cfg: ?config_mod.Auth,
        cluster_cfg: config_mod.Cluster,
        verbose: bool,
    ) !*Server {
        const server = try allocator.create(Server);
        errdefer allocator.destroy(server);

        var listener = try address.listen(.{
            .reuse_address = true,
            .force_nonblocking = true,
        });
        errdefer listener.deinit();

        var auth = if (auth_cfg) |cfg| try auth_mod.Store.initFromConfig(allocator, cfg) else null;
        errdefer if (auth) |*a| a.deinit();

        server.* = .{
            .allocator = allocator,
            .broker = broker,
            .cluster = null,
            .connections = undefined,
        };

        try server.connections.init(
            allocator,
            listener,
            auth,
            verbose,
            server,
            commandDispatch,
        );

        const cluster = try allocator.create(cluster_mod.Cluster);
        errdefer allocator.destroy(cluster);
        cluster.* = try cluster_mod.Cluster.init(allocator, cluster_cfg, @ptrCast(&server.connections), connection_manager.deliverClusterMessage);
        server.cluster = cluster;
        errdefer if (server.cluster) |cluster_ptr| {
            cluster_ptr.deinit();
            allocator.destroy(cluster_ptr);
        };
        try cluster.startThread();
        return server;
    }

    pub fn requestStop(self: *Server) void {
        self.connections.requestStop();
    }

    pub fn deinit(self: *Server) void {
        if (self.cluster) |cluster| {
            cluster.deinit();
            self.allocator.destroy(cluster);
        }

        self.connections.deinit();
    }

    pub fn serve(self: *Server) !void {
        return self.connections.serve();
    }

    pub fn clusterDeliveryCount(self: *Server) u64 {
        return self.connections.clusterDeliveryCount();
    }

    pub fn listenAddress(self: *Server) std.net.Address {
        return self.connections.listener.listen_address;
    }

    fn sendInfo(self: *Server, session_id: u64) void {
        self.connections.sendInfo(session_id);
    }

    fn sendOk(self: *Server, session_id: u64) void {
        self.connections.sendOk(session_id);
    }

    fn sendPong(self: *Server, session_id: u64) void {
        self.connections.sendPong(session_id);
    }

    fn sendErr(self: *Server, session_id: u64, msg: []const u8) void {
        self.connections.sendErr(session_id, msg);
    }

    fn sendErrAndClose(self: *Server, session_id: u64, msg: []const u8) void {
        self.connections.sendErrAndClose(session_id, msg);
    }

    fn sendMsg(self: *Server, session_id: u64, frame: protocol.OutgoingMessage) void {
        self.connections.sendMsg(session_id, frame, false);
    }

    fn queueClusterMessage(self: *Server, session_id: u64, frame: protocol.OutgoingMessage) void {
        self.connections.queueClusterMessage(session_id, frame);
    }

    fn sendParts(self: *Server, s: *session.Session, parts: []const []const u8, clustered: bool) void {
        _ = self;
        _ = s;
        _ = parts;
        _ = clustered;
    }

    fn appendParts(self: *Server, s: *session.Session, parts: []const []const u8) !void {
        _ = self;
        _ = s;
        _ = parts;
    }

    fn queuePendingWrite(self: *Server, session_id: u64) void {
        _ = self;
        _ = session_id;
    }

    fn signalWake(self: *Server) !void {
        _ = self;
    }

    fn drainWakeup(self: *Server) !void {
        _ = self;
    }

    fn flushSessionWrite(self: *Server, s: *session.Session) bool {
        _ = self;
        _ = s;
        return false;
    }

    fn closeSession(self: *Server, s: *session.Session) void {
        _ = self;
        _ = s;
    }

    fn isSessionClosed(self: *Server, s: *session.Session) bool {
        _ = self;
        _ = s;
        return true;
    }

    fn checkAuthTimeouts(self: *Server) !void {
        _ = self;
        return;
    }

    fn handleReadable(self: *Server, s: *session.Session) !void {
        _ = self;
        _ = s;
    }

    fn handleCommand(self: *Server, s: *session.Session, command: protocol.Command) !void {
        const auth_required = self.connections.auth != null;

        switch (command) {
            .connect => |connect_json| {
                if (s.saw_connect) {
                    self.sendErrAndClose(s.id, "duplicate CONNECT");
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                }
                s.saw_connect = true;

                const parsed = std.json.parseFromSlice(protocol.Connect, self.allocator, connect_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                }) catch {
                    self.sendErrAndClose(s.id, "auth failed");
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                };
                defer parsed.deinit();

                if (auth_required) {
                    if (!std.mem.eql(u8, parsed.value.auth_mode, "zkey")) {
                        self.sendErrAndClose(s.id, "auth failed");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    }

                    const store = self.connections.auth.?;
                    const public_key_text = parsed.value.public_key orelse {
                        self.sendErrAndClose(s.id, "auth failed");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };
                    const signature_text = parsed.value.signature orelse {
                        self.sendErrAndClose(s.id, "auth failed");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };

                    const public_key = auth_mod.decodePublicKey(public_key_text) catch {
                        self.sendErrAndClose(s.id, "invalid public key");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };
                    const signature = auth_mod.decodeSignature(signature_text) catch {
                        self.sendErrAndClose(s.id, "invalid signature");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };

                    s.principal = store.lookupByPublicKey(public_key.bytes) orelse {
                        self.sendErrAndClose(s.id, "unknown key");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };
                    const nonce = s.nonce orelse {
                        self.sendErrAndClose(s.id, "auth failed");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };
                    if (!auth_mod.verifyNonceSignature(public_key, nonce[0..], signature)) {
                        self.sendErrAndClose(s.id, "invalid signature");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    }
                }

                s.authenticated = true;
                s.auth_deadline_ns = 0;
                self.sendOk(s.id);
            },
            .ping => {
                if (auth_required and !s.authenticated) {
                    self.sendErrAndClose(s.id, "auth required");
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                }
                self.sendPong(s.id);
                self.sendOk(s.id);
            },
            .pong => {
                if (auth_required and !s.authenticated) {
                    self.sendErrAndClose(s.id, "auth required");
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                }
                self.sendOk(s.id);
            },
            .sub => |sub| {
                if (auth_required and !s.authenticated) {
                    self.sendErrAndClose(s.id, "auth required");
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                }
                if (self.connections.auth) |_| {
                    const current = s.principal orelse {
                        self.sendErr(s.id, "auth required");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };
                    if (!auth_mod.canSubscribe(&current.permissions, sub.subject)) {
                        self.sendErr(s.id, "permission denied");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    }
                }
                if (self.cluster) |cluster| {
                    cluster.subscribe(s.id, sub.sid, sub.subject, sub.queue) catch |err| {
                        self.sendErr(s.id, @errorName(err));
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };
                }
                self.broker.subscribe(s.id, sub.sid, sub.subject, sub.queue) catch |err| {
                    self.sendErr(s.id, @errorName(err));
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                };
                self.sendOk(s.id);
            },
            .unsub => |unsub| {
                if (auth_required and !s.authenticated) {
                    self.sendErrAndClose(s.id, "auth required");
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                }
                if (self.cluster) |cluster| {
                    cluster.unsubscribe(s.id, unsub.sid, unsub.max) catch |err| {
                        self.sendErr(s.id, @errorName(err));
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };
                }
                self.broker.unsubscribe(s.id, unsub.sid, unsub.max);
                self.sendOk(s.id);
            },
            .publish => |publish| {
                if (auth_required and !s.authenticated) {
                    self.sendErrAndClose(s.id, "auth required");
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                }
                if (self.connections.auth) |_| {
                    const current = s.principal orelse {
                        self.sendErr(s.id, "auth required");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    };
                    if (!auth_mod.canPublish(&current.permissions, publish.subject)) {
                        self.sendErr(s.id, "permission denied");
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                        return;
                    }
                }
                const cluster = self.cluster orelse {
                    self.sendErr(s.id, "cluster unavailable");
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                };
                const deliveries = cluster.publish(publish.subject, publish.reply, publish.payload) catch |err| {
                    self.sendErr(s.id, @errorName(err));
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return;
                };
                defer self.allocator.free(deliveries);
                for (deliveries) |delivery| {
                    self.sendMsg(delivery.session_id, .{
                        .session_id = delivery.session_id,
                        .sid = delivery.sid,
                        .subject = publish.subject,
                        .reply = publish.reply,
                        .payload = publish.payload,
                    });
                }
                self.sendOk(s.id);
            },
        }
    }
};

fn commandDispatch(ctx: *anyopaque, s: *session.Session, command: protocol.Command) !void {
    const server: *Server = @ptrCast(@alignCast(ctx));
    try server.handleCommand(s, command);
}
