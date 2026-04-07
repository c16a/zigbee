// SPDX-License-Identifier: MIT
const std = @import("std");
const broker_mod = @import("broker.zig");
const session = @import("session.zig");
const protocol = @import("protocol.zig");

pub const Server = struct {
    allocator: std.mem.Allocator,
    broker: *broker_mod.Broker,
    listener: std.net.Server,
    sessions_mutex: std.Thread.Mutex = .{},
    sessions: std.AutoHashMap(u64, *session.Session),
    all_sessions: std.ArrayList(*session.Session) = .empty,
    client_threads: std.ArrayList(std.Thread) = .empty,
    next_session_id: u64 = 1,

    pub fn start(allocator: std.mem.Allocator, broker: *broker_mod.Broker, address: std.net.Address) !Server {
        return .{
            .allocator = allocator,
            .broker = broker,
            .listener = try address.listen(.{ .reuse_address = true }),
            .sessions = std.AutoHashMap(u64, *session.Session).init(allocator),
        };
    }

    pub fn deinit(self: *Server) void {
        self.listener.deinit();

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
            .pong => self.writeSessionFrame(session_id, &[_][]const u8{"PONG\r\n"}),
            .msg => |msg| {
                const header = formatMsgHeader(&buf, msg) catch return;
                self.writeSessionFrame(msg.session_id, &[_][]const u8{ header, msg.payload, "\r\n" });
            },
        }
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

fn clientMain(server: *Server, s: *session.Session) void {
    defer s.close();
    defer server.broker.unsubscribeSession(s.id);
    defer server.unregisterSession(s.id);

    server.sendMsg(s.id, .{ .info = .{ .port = server.listener.listen_address.getPort() } });

    var reader = protocol.Reader.init(server.allocator);
    defer reader.deinit();

    var buffer: [4096]u8 = undefined;

    while (true) {
        while (reader.next() catch |err| {
            server.sendErr(s.id, @errorName(err));
            return;
        }) |command| {
            switch (command) {
                .ping => server.sendMsg(s.id, .pong),
                .pong => {},
                .connect => {},
                .sub => |sub| server.broker.subscribe(s.id, sub.sid, sub.subject, sub.queue) catch |err| {
                    server.sendErr(s.id, @errorName(err));
                    return;
                },
                .unsub => |unsub| server.broker.unsubscribe(s.id, unsub.sid, unsub.max),
                .publish => |publish| {
                    const deliveries = server.broker.publish(publish.subject) catch |err| {
                        server.sendErr(s.id, @errorName(err));
                        return;
                    };
                    defer server.allocator.free(deliveries);
                    for (deliveries) |delivery| {
                        server.sendMsg(delivery.client_id, .{ .msg = .{
                            .session_id = delivery.client_id,
                            .sid = delivery.sid,
                            .subject = publish.subject,
                            .reply = publish.reply,
                            .payload = publish.payload,
                        } });
                    }
                },
            }
        }

        const read_len = s.stream.read(&buffer) catch break;
        if (read_len == 0) break;
        reader.feed(buffer[0..read_len]) catch break;
    }
}
