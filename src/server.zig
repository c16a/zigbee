// SPDX-License-Identifier: MIT
const std = @import("std");
const broker_mod = @import("broker.zig");
const protocol = @import("protocol.zig");

pub const Server = struct {
    allocator: std.mem.Allocator,
    broker: *broker_mod.Broker,
    listener: std.net.Server,
    sessions_mutex: std.Thread.Mutex = .{},
    sessions: std.AutoHashMap(u64, *broker_mod.Session),
    all_sessions: std.ArrayList(*broker_mod.Session) = .empty,
    client_threads: std.ArrayList(std.Thread) = .empty,
    next_session_id: u64 = 1,

    pub fn start(allocator: std.mem.Allocator, broker: *broker_mod.Broker, address: std.net.Address) !Server {
        return .{
            .allocator = allocator,
            .broker = broker,
            .listener = try address.listen(.{ .reuse_address = true }),
            .sessions = std.AutoHashMap(u64, *broker_mod.Session).init(allocator),
        };
    }

    pub fn deinit(self: *Server) void {
        self.listener.deinit();

        self.sessions_mutex.lock();
        for (self.all_sessions.items) |session| {
            session.close();
        }
        self.sessions_mutex.unlock();

        for (self.client_threads.items) |thread| {
            thread.join();
        }

        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        for (self.all_sessions.items) |session| {
            self.allocator.destroy(session);
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

            const session = try self.makeSession(connection.stream);
            self.registerSession(session);

            const thread = try std.Thread.spawn(.{}, clientMain, .{ self, session });
            self.client_threads.append(self.allocator, thread) catch |err| {
                session.close();
                self.unregisterSession(session.id);
                return err;
            };
        }
    }

    fn makeSession(self: *Server, stream: std.net.Stream) !*broker_mod.Session {
        const session = try self.allocator.create(broker_mod.Session);
        session.* = .{
            .id = self.next_session_id,
            .stream = stream,
        };
        self.next_session_id += 1;
        return session;
    }

    fn registerSession(self: *Server, session: *broker_mod.Session) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        self.sessions.put(session.id, session) catch @panic("OOM");
        self.all_sessions.append(self.allocator, session) catch @panic("OOM");
    }

    fn unregisterSession(self: *Server, session_id: u64) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        _ = self.sessions.remove(session_id);
    }

    fn lookupSession(self: *Server, session_id: u64) ?*broker_mod.Session {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();
        return self.sessions.get(session_id);
    }

    fn sendInfo(self: *Server, session_id: u64) void {
        const session = self.lookupSession(session_id) orelse return;
        session.write_mutex.lock();
        defer session.write_mutex.unlock();
        if (session.closed) return;

        var frame: [256]u8 = undefined;
        const port = self.listener.listen_address.getPort();
        const frame_slice = std.fmt.bufPrint(&frame,
            "INFO {{\"server_id\":\"zigbee\",\"version\":\"0.1.0\",\"proto\":1,\"host\":\"127.0.0.1\",\"port\":{d},\"max_payload\":1048576}}\r\n",
            .{port},
        ) catch return;
        session.stream.writeAll(frame_slice) catch {};
    }

    fn sendPong(self: *Server, session_id: u64) void {
        const session = self.lookupSession(session_id) orelse return;
        session.write_mutex.lock();
        defer session.write_mutex.unlock();
        if (session.closed) return;
        _ = session.stream.writeAll("PONG\r\n") catch {};
    }

    fn sendErr(self: *Server, session_id: u64, msg: []const u8) void {
        const session = self.lookupSession(session_id) orelse return;
        session.write_mutex.lock();
        defer session.write_mutex.unlock();
        if (session.closed) return;

        var frame: [256]u8 = undefined;
        const frame_slice = std.fmt.bufPrint(&frame, "-ERR '{s}'\r\n", .{msg}) catch return;
        session.stream.writeAll(frame_slice) catch {};
    }

    fn sendMsg(self: *Server, delivery: broker_mod.Delivery, subject: []const u8, reply: ?[]const u8, payload: []const u8) void {
        const session = self.lookupSession(delivery.client_id) orelse return;
        session.write_mutex.lock();
        defer session.write_mutex.unlock();
        if (session.closed) return;

        var frame: [256]u8 = undefined;
        const header = if (reply) |reply_to| blk: {
            break :blk std.fmt.bufPrint(&frame, "MSG {s} {d} {s} {d}\r\n", .{ subject, delivery.sid, reply_to, payload.len }) catch return;
        } else blk: {
            break :blk std.fmt.bufPrint(&frame, "MSG {s} {d} {d}\r\n", .{ subject, delivery.sid, payload.len }) catch return;
        };

        session.stream.writeAll(header) catch {
            session.close();
            self.unregisterSession(delivery.client_id);
            return;
        };
        session.stream.writeAll(payload) catch {
            session.close();
            self.unregisterSession(delivery.client_id);
            return;
        };
        session.stream.writeAll("\r\n") catch {
            session.close();
            self.unregisterSession(delivery.client_id);
        };
    }
};

fn clientMain(server: *Server, session: *broker_mod.Session) void {
    defer session.close();
    defer server.broker.unsubscribeSession(session.id);
    defer server.unregisterSession(session.id);

    server.sendInfo(session.id);

    var reader = protocol.Reader.init(server.allocator);
    defer reader.deinit();

    var buffer: [4096]u8 = undefined;

    while (true) {
        while (reader.next() catch |err| {
            server.sendErr(session.id, @errorName(err));
            return;
        }) |command| {
            switch (command) {
                .ping => server.sendPong(session.id),
                .pong => {},
                .connect => {},
                .sub => |sub| server.broker.subscribe(session.id, sub.sid, sub.subject, sub.queue) catch |err| {
                    server.sendErr(session.id, @errorName(err));
                    return;
                },
                .unsub => |unsub| server.broker.unsubscribe(session.id, unsub.sid, unsub.max),
                .publish => |publish| {
                    const deliveries = server.broker.publish(publish.subject) catch |err| {
                        server.sendErr(session.id, @errorName(err));
                        return;
                    };
                    defer server.allocator.free(deliveries);
                    for (deliveries) |delivery| {
                        server.sendMsg(delivery, publish.subject, publish.reply, publish.payload);
                    }
                },
            }
        }

        const read_len = session.stream.read(&buffer) catch break;
        if (read_len == 0) break;
        reader.feed(buffer[0..read_len]) catch break;
    }
}
