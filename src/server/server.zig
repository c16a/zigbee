// SPDX-License-Identifier: MIT
const std = @import("std");
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

    fn handleCommand(self: *Server, s: *session.Session, command: protocol.Command) !void {
        const auth_required = self.connections.auth != null;
        const fail = struct {
            fn call(server: *Server, sess: *session.Session, msg: []const u8) void {
                server.connections.sendErrAndClose(sess.id, msg);
                if (server.connections.flushSessionWrite(sess)) server.connections.closeSession(sess);
            }
        }.call;

        switch (command) {
            .connect => |connect_json| {
                if (s.saw_connect) {
                    fail(self, s, "duplicate CONNECT");
                    return;
                }
                s.saw_connect = true;

                const parsed = std.json.parseFromSlice(protocol.Connect, self.allocator, connect_json, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                }) catch {
                    fail(self, s, "auth failed");
                    return;
                };
                defer parsed.deinit();

                if (auth_required) {
                    if (!std.mem.eql(u8, parsed.value.auth_mode, "zkey")) {
                        fail(self, s, "auth failed");
                        return;
                    }

                    const store = self.connections.auth.?;
                    const public_key_text = parsed.value.public_key orelse {
                        fail(self, s, "auth failed");
                        return;
                    };
                    const signature_text = parsed.value.signature orelse {
                        fail(self, s, "auth failed");
                        return;
                    };

                    const public_key = auth_mod.decodePublicKey(public_key_text) catch {
                        fail(self, s, "invalid public key");
                        return;
                    };
                    const signature = auth_mod.decodeSignature(signature_text) catch {
                        fail(self, s, "invalid signature");
                        return;
                    };

                    s.principal = store.lookupByPublicKey(public_key.bytes) orelse {
                        fail(self, s, "unknown key");
                        return;
                    };
                    const nonce = s.nonce orelse {
                        fail(self, s, "auth failed");
                        return;
                    };
                    if (!auth_mod.verifyNonceSignature(public_key, nonce[0..], signature)) {
                        fail(self, s, "invalid signature");
                        return;
                    }
                }

                s.authenticated = true;
                s.auth_deadline_ns = 0;
                self.connections.sendOk(s.id);
            },
            .ping => {
                if (auth_required and !s.authenticated) {
                    fail(self, s, "auth required");
                    return;
                }
                self.connections.sendPong(s.id);
                self.connections.sendOk(s.id);
            },
            .pong => {
                if (auth_required and !s.authenticated) {
                    fail(self, s, "auth required");
                    return;
                }
                self.connections.sendOk(s.id);
            },
            .sub => |sub| {
                if (auth_required and !s.authenticated) {
                    fail(self, s, "auth required");
                    return;
                }
                if (self.connections.auth) |_| {
                    const current = s.principal orelse {
                        fail(self, s, "auth required");
                        return;
                    };
                    if (!auth_mod.canSubscribe(&current.permissions, sub.subject)) {
                        fail(self, s, "permission denied");
                        return;
                    }
                }
                if (self.cluster) |cluster| {
                    cluster.subscribe(s.id, sub.sid, sub.subject, sub.queue) catch |err| {
                        fail(self, s, @errorName(err));
                        return;
                    };
                }
                self.broker.subscribe(s.id, sub.sid, sub.subject, sub.queue) catch |err| {
                    fail(self, s, @errorName(err));
                    return;
                };
                self.connections.sendOk(s.id);
            },
            .unsub => |unsub| {
                if (auth_required and !s.authenticated) {
                    fail(self, s, "auth required");
                    return;
                }
                if (self.cluster) |cluster| {
                    cluster.unsubscribe(s.id, unsub.sid, unsub.max) catch |err| {
                        fail(self, s, @errorName(err));
                        return;
                    };
                }
                self.broker.unsubscribe(s.id, unsub.sid, unsub.max);
                self.connections.sendOk(s.id);
            },
            .publish => |publish| {
                if (auth_required and !s.authenticated) {
                    fail(self, s, "auth required");
                    return;
                }
                if (self.connections.auth) |_| {
                    const current = s.principal orelse {
                        fail(self, s, "auth required");
                        return;
                    };
                    if (!auth_mod.canPublish(&current.permissions, publish.subject)) {
                        fail(self, s, "permission denied");
                        return;
                    }
                }
                const cluster = self.cluster orelse {
                    fail(self, s, "cluster unavailable");
                    return;
                };
                const deliveries = cluster.publish(publish.subject, publish.reply, publish.payload) catch |err| {
                    fail(self, s, @errorName(err));
                    return;
                };
                defer self.allocator.free(deliveries);
                for (deliveries) |delivery| {
                    self.connections.sendMsg(delivery.session_id, .{
                        .session_id = delivery.session_id,
                        .sid = delivery.sid,
                        .subject = publish.subject,
                        .reply = publish.reply,
                        .payload = publish.payload,
                    }, false);
                }
                self.connections.sendOk(s.id);
            },
        }
    }
};

fn commandDispatch(ctx: *anyopaque, s: *session.Session, command: protocol.Command) !void {
    const server: *Server = @ptrCast(@alignCast(ctx));
    try server.handleCommand(s, command);
}
