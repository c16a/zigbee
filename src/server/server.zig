// SPDX-License-Identifier: MIT
const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const linux = std.os.linux;
const broker_mod = @import("broker.zig");
const cluster_mod = @import("cluster.zig");
const auth_mod = @import("auth.zig");
const config_mod = @import("config.zig");
const common_mod = @import("common_client");
const crypto_mod = common_mod.crypto_mod;
const protocol = common_mod.protocol_mod;
const session = @import("session.zig");

const auth_timeout_ns: i128 = 5 * std.time.ns_per_s;
const wake_signal: u64 = 1;
const use_epoll = builtin.os.tag == .linux;
const use_kqueue = builtin.os.tag == .macos;

const QueueSource = enum {
    reactor,
    cluster,
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    broker: *broker_mod.Broker,
    auth: ?auth_mod.Store,
    cluster: ?*cluster_mod.Cluster,
    verbose: bool,
    listener: std.net.Server,
    epoll_fd: ?posix.socket_t = null,
    kqueue_fd: ?posix.socket_t = null,
    wake_read_fd: posix.socket_t,
    wake_write_fd: posix.socket_t,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    sessions_mutex: std.Thread.Mutex = .{},
    sessions: std.AutoHashMap(u64, *session.Session),
    sessions_by_fd: std.AutoHashMap(posix.socket_t, *session.Session),
    all_sessions: std.ArrayList(*session.Session) = .empty,
    pending_writes: std.ArrayList(u64) = .empty,
    next_session_id: u64 = 1,
    cluster_delivery_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

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

        var backend_epoll_fd: ?posix.socket_t = null;
        if (use_epoll) {
            backend_epoll_fd = try posix.epoll_create1(linux.EPOLL.CLOEXEC);
            errdefer posix.close(backend_epoll_fd.?);
        }
        var backend_kqueue_fd: ?posix.socket_t = null;
        if (use_kqueue) {
            backend_kqueue_fd = try posix.kqueue();
            errdefer posix.close(backend_kqueue_fd.?);
        }

        var wake_pipe: [2]posix.fd_t = undefined;
        const wake_read_fd: posix.socket_t = if (use_epoll) blk: {
            break :blk try posix.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK);
        } else blk: {
            wake_pipe = try posix.pipe2(.{ .CLOEXEC = true, .NONBLOCK = true });
            break :blk wake_pipe[0];
        };
        const wake_write_fd: posix.socket_t = if (use_epoll) wake_read_fd else wake_pipe[1];
        errdefer {
            posix.close(wake_read_fd);
            if (wake_write_fd != wake_read_fd) posix.close(wake_write_fd);
        }

        server.* = .{
            .allocator = allocator,
            .broker = broker,
            .auth = null,
            .cluster = null,
            .verbose = verbose,
            .listener = listener,
            .epoll_fd = backend_epoll_fd,
            .kqueue_fd = backend_kqueue_fd,
            .wake_read_fd = wake_read_fd,
            .wake_write_fd = wake_write_fd,
            .sessions = std.AutoHashMap(u64, *session.Session).init(allocator),
            .sessions_by_fd = std.AutoHashMap(posix.socket_t, *session.Session).init(allocator),
        };
        errdefer server.sessions.deinit();
        errdefer server.sessions_by_fd.deinit();

        if (auth_cfg) |cfg| {
            server.auth = try auth_mod.Store.initFromConfig(allocator, cfg);
        }
        errdefer if (server.auth) |*auth| auth.deinit();

        if (use_epoll) {
            try server.addEpollFd(listener.stream.handle, linux.EPOLL.IN);
            errdefer _ = posix.epoll_ctl(backend_epoll_fd.?, linux.EPOLL.CTL_DEL, listener.stream.handle, null) catch {};

            try server.addEpollFd(wake_read_fd, linux.EPOLL.IN);
            errdefer _ = posix.epoll_ctl(backend_epoll_fd.?, linux.EPOLL.CTL_DEL, wake_read_fd, null) catch {};
        } else if (use_kqueue) {
            try server.addKqueueChange(listener.stream.handle, std.c.EVFILT.READ, std.c.EV.ADD | std.c.EV.ENABLE);
            errdefer _ = posix.kevent(backend_kqueue_fd.?, &.{ .{
                .ident = @intCast(listener.stream.handle),
                .filter = std.c.EVFILT.READ,
                .flags = std.c.EV.DELETE,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            } }, &.{}, null) catch {};

            try server.addKqueueChange(wake_read_fd, std.c.EVFILT.READ, std.c.EV.ADD | std.c.EV.ENABLE);
            errdefer _ = posix.kevent(backend_kqueue_fd.?, &.{ .{
                .ident = @intCast(wake_read_fd),
                .filter = std.c.EVFILT.READ,
                .flags = std.c.EV.DELETE,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            } }, &.{}, null) catch {};
        }

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

    pub fn requestStop(self: *Server) void {
        self.stop.store(true, .release);
        self.signalWake() catch {};
    }

    pub fn deinit(self: *Server) void {
        if (self.cluster) |cluster| {
            cluster.deinit();
            self.allocator.destroy(cluster);
        }

        self.listener.deinit();

        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        for (self.all_sessions.items) |s| {
            s.deinit(self.allocator);
        }

        if (self.auth) |*auth| {
            auth.deinit();
        }

        self.all_sessions.deinit(self.allocator);
        self.pending_writes.deinit(self.allocator);
        self.sessions.deinit();
        self.sessions_by_fd.deinit();
        posix.close(self.wake_read_fd);
        if (self.wake_write_fd != self.wake_read_fd) posix.close(self.wake_write_fd);
        if (self.epoll_fd) |fd| posix.close(fd);
        if (self.kqueue_fd) |fd| posix.close(fd);
    }

    pub fn serve(self: *Server) !void {
        if (use_epoll) {
            return self.serveEpoll();
        }
        if (use_kqueue) {
            return self.serveKqueue();
        }
        return self.servePoll();
    }

    fn serveEpoll(self: *Server) !void {
        var events: [64]linux.epoll_event = undefined;
        while (!self.stop.load(.acquire)) {
            const count = posix.epoll_wait(self.epoll_fd.?, events[0..], 1000);
            for (events[0..count]) |event| {
                const fd = event.data.fd;
                if (fd == self.listener.stream.handle) {
                    try self.acceptConnections();
                } else if (fd == self.wake_read_fd) {
                    try self.drainWakeup();
                } else {
                    if (self.lookupSessionByFd(fd)) |s| {
                        if ((event.events & (linux.EPOLL.IN | linux.EPOLL.HUP | linux.EPOLL.RDHUP)) != 0) {
                            try self.handleReadable(s);
                        }
                        if (!self.isSessionClosed(s) and (event.events & linux.EPOLL.OUT) != 0) {
                            if (self.flushSessionWrite(s)) self.closeSession(s);
                        }
                        if (!self.isSessionClosed(s) and (event.events & (linux.EPOLL.ERR | linux.EPOLL.HUP | linux.EPOLL.RDHUP)) != 0) {
                            self.closeSession(s);
                        }
                    }
                }
            }

            try self.checkAuthTimeouts();
        }
    }

    fn servePoll(self: *Server) !void {
        var poll_fds = std.ArrayList(posix.pollfd).empty;
        defer poll_fds.deinit(self.allocator);

        while (!self.stop.load(.acquire)) {
            poll_fds.clearRetainingCapacity();
            try poll_fds.append(self.allocator, .{
                .fd = self.listener.stream.handle,
                .events = posix.POLL.IN,
                .revents = 0,
            });
            try poll_fds.append(self.allocator, .{
                .fd = self.wake_read_fd,
                .events = posix.POLL.IN,
                .revents = 0,
            });

            var it = self.sessions.iterator();
            while (it.next()) |entry| {
                const s = entry.value_ptr.*;
                s.mutex.lock();
                const closed = s.closed;
                const wants_write = s.write_offset < s.write_buffer.items.len;
                s.mutex.unlock();
                if (closed) continue;

                var events: i16 = posix.POLL.IN | posix.POLL.ERR | posix.POLL.HUP;
                if (wants_write) events |= posix.POLL.OUT;
                try poll_fds.append(self.allocator, .{
                    .fd = s.stream.handle,
                    .events = events,
                    .revents = 0,
                });
            }

            const count = try posix.poll(poll_fds.items, 1000);
            if (count == 0) {
                try self.checkAuthTimeouts();
                continue;
            }

            for (poll_fds.items) |event| {
                if (event.revents == 0) continue;
                if (event.fd == self.listener.stream.handle) {
                    try self.acceptConnections();
                    continue;
                }
                if (event.fd == self.wake_read_fd) {
                    try self.drainWakeup();
                    continue;
                }
                if (self.lookupSessionByFd(event.fd)) |s| {
                    if ((event.revents & (posix.POLL.IN | posix.POLL.HUP)) != 0) {
                        try self.handleReadable(s);
                    }
                    if (!self.isSessionClosed(s) and (event.revents & posix.POLL.OUT) != 0) {
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                    }
                    if (!self.isSessionClosed(s) and (event.revents & (posix.POLL.ERR | posix.POLL.HUP)) != 0) {
                        self.closeSession(s);
                    }
                }
            }

            try self.checkAuthTimeouts();
        }
    }

    fn serveKqueue(self: *Server) !void {
        var events: [64]posix.Kevent = undefined;
        while (!self.stop.load(.acquire)) {
            const count = try posix.kevent(self.kqueue_fd.?, &.{}, events[0..], null);
            for (events[0..count]) |event| {
                const fd: posix.socket_t = @intCast(event.ident);
                if (fd == self.listener.stream.handle) {
                    try self.acceptConnections();
                    continue;
                }
                if (fd == self.wake_read_fd) {
                    try self.drainWakeup();
                    continue;
                }
                if (self.lookupSessionByFd(fd)) |s| {
                    if ((event.filter == std.c.EVFILT.READ) and ((event.flags & (std.c.EV.EOF | std.c.EV.ERROR)) == 0)) {
                        try self.handleReadable(s);
                    }
                    if (!self.isSessionClosed(s) and event.filter == std.c.EVFILT.WRITE) {
                        if (self.flushSessionWrite(s)) self.closeSession(s);
                    }
                    if (!self.isSessionClosed(s) and ((event.flags & (std.c.EV.EOF | std.c.EV.ERROR)) != 0)) {
                        self.closeSession(s);
                    }
                }
            }

            try self.checkAuthTimeouts();
        }
    }

    pub fn clusterDeliveryCount(self: *Server) u64 {
        return @as(u64, self.cluster_delivery_count.load(.acquire));
    }

    fn acceptConnections(self: *Server) !void {
        while (true) {
            var accepted_addr: std.net.Address = undefined;
            var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
            const fd = posix.accept(
                self.listener.stream.handle,
                &accepted_addr.any,
                &addr_len,
                posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK,
            ) catch |err| switch (err) {
                error.WouldBlock => return,
                error.ConnectionAborted => continue,
                else => return err,
            };

            const s = try self.makeSession(fd, accepted_addr);
            self.registerSession(s);
            if (use_epoll) {
                try self.addEpollFd(fd, linux.EPOLL.IN);
            } else if (use_kqueue) {
                try self.addKqueueChange(fd, std.c.EVFILT.READ, std.c.EV.ADD | std.c.EV.ENABLE);
                try self.addKqueueChange(fd, std.c.EVFILT.WRITE, std.c.EV.ADD | std.c.EV.DISABLE);
            }
            self.sendInfo(s.id);
        }
    }

    fn makeSession(self: *Server, fd: posix.socket_t, address: std.net.Address) !*session.Session {
        const s = try self.allocator.create(session.Session);
        const now = std.time.nanoTimestamp();
        const auth_required = self.auth != null;
        var nonce: ?[crypto_mod.seed_length]u8 = null;
        if (auth_required) {
            var raw_nonce: [crypto_mod.seed_length]u8 = undefined;
            std.crypto.random.bytes(&raw_nonce);
            nonce = raw_nonce;
        }
        s.* = session.Session.init(
            self.allocator,
            self.next_session_id,
            .{ .handle = fd },
            address,
            !auth_required,
            if (auth_required) now + auth_timeout_ns else 0,
            nonce,
        );
        self.next_session_id += 1;
        return s;
    }

    fn registerSession(self: *Server, s: *session.Session) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        self.sessions.put(s.id, s) catch @panic("OOM");
        self.sessions_by_fd.put(s.stream.handle, s) catch @panic("OOM");
        self.all_sessions.append(self.allocator, s) catch @panic("OOM");
    }

    fn lookupSessionById(self: *Server, session_id: u64) ?*session.Session {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();
        return self.sessions.get(session_id);
    }

    fn lookupSessionByFd(self: *Server, fd: posix.socket_t) ?*session.Session {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();
        return self.sessions_by_fd.get(fd);
    }

    fn addEpollFd(self: *Server, fd: posix.socket_t, events: u32) !void {
        if (!use_epoll) return;
        var event: linux.epoll_event = .{
            .events = events,
            .data = .{ .fd = fd },
        };
        try posix.epoll_ctl(self.epoll_fd.?, linux.EPOLL.CTL_ADD, fd, &event);
    }

    fn addKqueueChange(self: *Server, fd: posix.socket_t, filter: i16, flags: u16) !void {
        if (!use_kqueue) return;
        const event: posix.Kevent = .{
            .ident = @intCast(fd),
            .filter = filter,
            .flags = flags,
            .fflags = 0,
            .data = 0,
            .udata = 0,
        };
        _ = try posix.kevent(self.kqueue_fd.?, &.{event}, &.{}, null);
    }

    fn sendInfo(self: *Server, session_id: u64) void {
        const s = self.lookupSessionById(session_id) orelse return;
        const nonce_text = if (s.nonce) |nonce| blk: {
            break :blk crypto_mod.encodeBase64(self.allocator, nonce[0..]) catch return;
        } else null;
        defer if (nonce_text) |text| self.allocator.free(text);
        const info = protocol.Info{
            .port = self.listener.listen_address.getPort(),
            .auth_required = self.auth != null,
            .auth_mode = if (self.auth != null) "zkey" else "none",
            .nonce = nonce_text,
        };
        const json = protocol.formatInfoJson(self.allocator, info) catch return;
        defer self.allocator.free(json);
        self.sendParts(s, &[_][]const u8{ "INFO ", json, "\r\n" }, .reactor);
    }

    fn sendOk(self: *Server, session_id: u64) void {
        if (!self.verbose) return;
        const s = self.lookupSessionById(session_id) orelse return;
        self.sendParts(s, &[_][]const u8{"OK\r\n"}, .reactor);
    }

    fn sendPong(self: *Server, session_id: u64) void {
        const s = self.lookupSessionById(session_id) orelse return;
        self.sendParts(s, &[_][]const u8{"PONG\r\n"}, .reactor);
    }

    fn sendErr(self: *Server, session_id: u64, msg: []const u8) void {
        const s = self.lookupSessionById(session_id) orelse return;
        var frame: [256]u8 = undefined;
        const text = std.fmt.bufPrint(&frame, "-ERR '{s}'\r\n", .{msg}) catch return;
        self.sendParts(s, &[_][]const u8{text}, .reactor);
    }

    fn sendErrAndClose(self: *Server, session_id: u64, msg: []const u8) void {
        const s = self.lookupSessionById(session_id) orelse return;
        var frame: [256]u8 = undefined;
        const text = std.fmt.bufPrint(&frame, "-ERR '{s}'\r\n", .{msg}) catch return;
        s.mutex.lock();
        defer s.mutex.unlock();
        if (s.closed) return;
        if (s.write_offset == s.write_buffer.items.len) {
            s.write_buffer.clearRetainingCapacity();
            s.write_offset = 0;
        }
        s.write_buffer.appendSlice(self.allocator, text) catch return;
        s.write_buffer.appendSlice(self.allocator, "CLOSE\r\n") catch return;
        s.closing = true;
    }

    fn sendMsg(self: *Server, session_id: u64, frame: protocol.OutgoingMessage) void {
        const s = self.lookupSessionById(session_id) orelse return;
        var buf: [256]u8 = undefined;
        const header = if (frame.reply) |reply_to| blk: {
            break :blk std.fmt.bufPrint(&buf, "MSG {s} {d} {s} {d}\r\n", .{ frame.subject, frame.sid, reply_to, frame.payload.len }) catch return;
        } else blk: {
            break :blk std.fmt.bufPrint(&buf, "MSG {s} {d} {d}\r\n", .{ frame.subject, frame.sid, frame.payload.len }) catch return;
        };
        self.sendParts(s, &[_][]const u8{ header, frame.payload, "\r\n" }, .reactor);
    }

    fn queueClusterMessage(self: *Server, session_id: u64, frame: protocol.OutgoingMessage) void {
        const s = self.lookupSessionById(session_id) orelse return;
        var buf: [256]u8 = undefined;
        const header = if (frame.reply) |reply_to| blk: {
            break :blk std.fmt.bufPrint(&buf, "MSG {s} {d} {s} {d}\r\n", .{ frame.subject, frame.sid, reply_to, frame.payload.len }) catch return;
        } else blk: {
            break :blk std.fmt.bufPrint(&buf, "MSG {s} {d} {d}\r\n", .{ frame.subject, frame.sid, frame.payload.len }) catch return;
        };
        self.sendParts(s, &[_][]const u8{ header, frame.payload, "\r\n" }, .cluster);
    }

    fn sendParts(self: *Server, s: *session.Session, parts: []const []const u8, source: QueueSource) void {
        self.appendParts(s, parts) catch return;
        if (source == .cluster) {
            self.queuePendingWrite(s.id);
            return;
        }
        if (self.flushSessionWrite(s)) {
            self.closeSession(s);
        }
    }

    fn appendParts(self: *Server, s: *session.Session, parts: []const []const u8) !void {
        s.mutex.lock();
        defer s.mutex.unlock();

        if (s.closed) return;
        if (s.closing) return;

        if (s.write_offset == s.write_buffer.items.len) {
            s.write_buffer.clearRetainingCapacity();
            s.write_offset = 0;
        }

        for (parts) |part| {
            try s.write_buffer.appendSlice(self.allocator, part);
        }
    }

    fn queuePendingWrite(self: *Server, session_id: u64) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        self.pending_writes.append(self.allocator, session_id) catch return;
        self.signalWake() catch {};
    }

    fn signalWake(self: *Server) !void {
        const bytes = std.mem.asBytes(&wake_signal);
        _ = posix.write(self.wake_write_fd, bytes) catch |err| switch (err) {
            error.WouldBlock => return,
            else => return err,
        };
    }

    fn drainWakeup(self: *Server) !void {
        var scratch: [64]u8 = undefined;
        while (true) {
            const read_len = posix.read(self.wake_read_fd, &scratch) catch |err| switch (err) {
                error.WouldBlock => break,
                else => return err,
            };
            if (read_len == 0) break;
            if (read_len < scratch.len and use_epoll) break;
            if (read_len < scratch.len and !use_epoll) continue;
        }

        var local_ids = std.ArrayList(u64).empty;
        defer local_ids.deinit(self.allocator);

        self.sessions_mutex.lock();
        while (self.pending_writes.items.len > 0) {
            const pending = self.pending_writes.pop() orelse break;
            local_ids.append(self.allocator, pending) catch {
                self.sessions_mutex.unlock();
                return;
            };
        }
        self.sessions_mutex.unlock();

        for (local_ids.items) |session_id| {
            const s = self.lookupSessionById(session_id) orelse continue;
            if (self.isSessionClosed(s)) continue;
            if (self.flushSessionWrite(s)) {
                self.closeSession(s);
            }
        }
    }

    fn flushSessionWrite(self: *Server, s: *session.Session) bool {
        var should_close = false;

        s.mutex.lock();
        defer s.mutex.unlock();

        if (s.closed) return false;

        while (s.write_offset < s.write_buffer.items.len) {
            const slice = s.write_buffer.items[s.write_offset..];
            const n = posix.write(s.stream.handle, slice) catch |err| switch (err) {
                error.WouldBlock => break,
                else => {
                    should_close = true;
                    break;
                },
            };
            if (n == 0) break;
            s.write_offset += n;
        }

        if (s.write_offset == s.write_buffer.items.len) {
            s.write_buffer.clearRetainingCapacity();
            s.write_offset = 0;
        }

        const need_write = s.write_offset < s.write_buffer.items.len;
        if (need_write != s.write_armed) {
            self.setWriteInterestLocked(s, need_write) catch {
                should_close = true;
            };
            if (!should_close) {
                s.write_armed = need_write;
            }
        }

        if (!need_write and s.closing) {
            should_close = true;
        }

        return should_close;
    }

    fn setWriteInterestLocked(self: *Server, s: *session.Session, enable: bool) !void {
        if (use_epoll) {
            const events: u32 = if (enable) linux.EPOLL.IN | linux.EPOLL.OUT else linux.EPOLL.IN;
            var event: linux.epoll_event = .{
                .events = events,
                .data = .{ .fd = s.stream.handle },
            };
            try posix.epoll_ctl(self.epoll_fd.?, linux.EPOLL.CTL_MOD, s.stream.handle, &event);
            return;
        }
        if (use_kqueue) {
            try self.addKqueueChange(s.stream.handle, std.c.EVFILT.WRITE, if (enable) std.c.EV.ENABLE else std.c.EV.DISABLE);
            return;
        }
    }

    fn closeSession(self: *Server, s: *session.Session) void {
        s.mutex.lock();
        if (s.closed) {
            s.mutex.unlock();
            return;
        }
        s.closed = true;
        s.closing = false;
        s.write_armed = false;
        s.mutex.unlock();

        if (use_epoll) {
            _ = posix.epoll_ctl(self.epoll_fd.?, linux.EPOLL.CTL_DEL, s.stream.handle, null) catch {};
        } else if (use_kqueue) {
            const changes = [_]posix.Kevent{
                .{
                    .ident = @intCast(s.stream.handle),
                    .filter = std.c.EVFILT.READ,
                    .flags = std.c.EV.DELETE,
                    .fflags = 0,
                    .data = 0,
                    .udata = 0,
                },
                .{
                    .ident = @intCast(s.stream.handle),
                    .filter = std.c.EVFILT.WRITE,
                    .flags = std.c.EV.DELETE,
                    .fflags = 0,
                    .data = 0,
                    .udata = 0,
                },
            };
            _ = posix.kevent(self.kqueue_fd.?, &changes, &.{}, null) catch {};
        }

        self.sessions_mutex.lock();
        _ = self.sessions.remove(s.id);
        _ = self.sessions_by_fd.remove(s.stream.handle);
        self.sessions_mutex.unlock();

        s.stream.close();
        if (self.cluster) |cluster| {
            cluster.unsubscribeSession(s.id) catch {};
        }
    }

    fn isSessionClosed(self: *Server, s: *session.Session) bool {
        _ = self;
        s.mutex.lock();
        defer s.mutex.unlock();
        return s.closed;
    }

    fn checkAuthTimeouts(self: *Server) !void {
        if (self.auth == null) return;

        const now = std.time.nanoTimestamp();
        for (self.all_sessions.items) |s| {
            s.mutex.lock();
            const timed_out = !s.closed and !s.authenticated and s.auth_deadline_ns != 0 and now >= s.auth_deadline_ns;
            s.mutex.unlock();
            if (!timed_out) continue;
            self.sendErrAndClose(s.id, "auth timeout");
            if (self.flushSessionWrite(s)) {
                self.closeSession(s);
            }
        }
    }

    fn handleReadable(self: *Server, s: *session.Session) !void {
        var scratch: [4096]u8 = undefined;

        while (true) {
            const read_len = s.stream.read(&scratch) catch |err| switch (err) {
                error.WouldBlock => break,
                else => {
                    self.closeSession(s);
                    return;
                },
            };
            if (read_len == 0) {
                self.closeSession(s);
                return;
            }

            s.reader.feed(scratch[0..read_len]) catch {
                self.closeSession(s);
                return;
            };

            while (true) {
                const command = s.reader.next() catch |err| {
                    self.sendErrAndClose(s.id, @errorName(err));
                    if (self.flushSessionWrite(s)) {
                        self.closeSession(s);
                    }
                    return;
                };
                const cmd = command orelse break;
                try self.handleCommand(s, cmd);
                if (self.isSessionClosed(s)) return;
            }
        }
    }

    fn handleCommand(self: *Server, s: *session.Session, command: protocol.Command) !void {
        const auth_required = self.auth != null;

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

                    const store = self.auth.?;
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
                if (self.auth) |_| {
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
                if (self.auth) |_| {
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

fn deliverClusterMessage(
    ctx: *anyopaque,
    delivery: cluster_mod.Delivery,
    subject: []const u8,
    reply: ?[]const u8,
    payload: []const u8,
) void {
    const server: *Server = @ptrCast(@alignCast(ctx));
    _ = server.cluster_delivery_count.fetchAdd(1, .acq_rel);
    server.queueClusterMessage(delivery.session_id, .{
        .session_id = delivery.session_id,
        .sid = delivery.sid,
        .subject = subject,
        .reply = reply,
        .payload = payload,
    });
}
