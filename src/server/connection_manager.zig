// SPDX-License-Identifier: MIT
const std = @import("std");
const posix = std.posix;
const auth_mod = @import("auth.zig");
const reactor_mod = @import("reactor.zig");
const common_mod = @import("common_client");
const crypto_mod = common_mod.crypto_mod;
const protocol = common_mod.protocol_mod;
const session = @import("session.zig");

const auth_timeout_ns: i128 = 5 * std.time.ns_per_s;
const wake_signal: u64 = 1;

pub const CommandHandler = *const fn (*anyopaque, *session.Session, protocol.Command) anyerror!void;

pub const ConnectionManager = struct {
    allocator: std.mem.Allocator,
    auth: ?auth_mod.Store,
    verbose: bool,
    listener: std.net.Server,
    wake_read_fd: posix.socket_t,
    wake_write_fd: posix.socket_t,
    reactor: reactor_mod.Reactor,
    command_ctx: *anyopaque,
    command_handler: CommandHandler,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    sessions_mutex: std.Thread.Mutex = .{},
    sessions: std.AutoHashMap(u64, *session.Session),
    sessions_by_fd: std.AutoHashMap(posix.socket_t, *session.Session),
    all_sessions: std.ArrayList(*session.Session) = .empty,
    pending_writes: std.ArrayList(u64) = .empty,
    next_session_id: u64 = 1,
    cluster_delivery_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

    pub fn init(
        self: *ConnectionManager,
        allocator: std.mem.Allocator,
        listener: std.net.Server,
        auth: ?auth_mod.Store,
        verbose: bool,
        command_ctx: *anyopaque,
        command_handler: CommandHandler,
    ) !void {
        const wake_pipe = try posix.pipe2(.{ .CLOEXEC = true, .NONBLOCK = true });
        const wake_read_fd: posix.socket_t = wake_pipe[0];
        const wake_write_fd: posix.socket_t = wake_pipe[1];
        errdefer {
            posix.close(wake_read_fd);
            if (wake_write_fd != wake_read_fd) posix.close(wake_write_fd);
        }

        self.* = .{
            .allocator = allocator,
            .auth = auth,
            .verbose = verbose,
            .listener = listener,
            .wake_read_fd = wake_read_fd,
            .wake_write_fd = wake_write_fd,
            .reactor = undefined,
            .command_ctx = command_ctx,
            .command_handler = command_handler,
            .sessions = std.AutoHashMap(u64, *session.Session).init(allocator),
            .sessions_by_fd = std.AutoHashMap(posix.socket_t, *session.Session).init(allocator),
        };
        errdefer self.sessions.deinit();
        errdefer self.sessions_by_fd.deinit();

        self.reactor = try reactor_mod.Reactor.init(.{
            .ctx = self,
            .should_stop = shouldStop,
            .on_listener = onListener,
            .on_wakeup = onWakeup,
            .on_readable = onReadable,
            .on_writable = onWritable,
            .on_tick = onTick,
        }, listener.stream.handle, wake_read_fd);
        errdefer self.reactor.deinit();
    }

    pub fn deinit(self: *ConnectionManager) void {
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
        self.reactor.deinit();
        posix.close(self.wake_read_fd);
        if (self.wake_write_fd != self.wake_read_fd) posix.close(self.wake_write_fd);
    }

    pub fn requestStop(self: *ConnectionManager) void {
        self.stop.store(true, .release);
        self.signalWake() catch {};
    }

    pub fn serve(self: *ConnectionManager) !void {
        return self.reactor.run();
    }

    pub fn clusterDeliveryCount(self: *ConnectionManager) u64 {
        return @as(u64, self.cluster_delivery_count.load(.acquire));
    }

    pub fn acceptConnections(self: *ConnectionManager) !void {
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
            try self.reactor.registerSession(fd);
            self.sendInfo(s.id);
        }
    }

    pub fn handleReadableFd(self: *ConnectionManager, fd: posix.socket_t) !void {
        const s = self.lookupSessionByFd(fd) orelse return;
        try self.handleReadable(s);
    }

    pub fn handleWritableFd(self: *ConnectionManager, fd: posix.socket_t) !void {
        const s = self.lookupSessionByFd(fd) orelse return;
        if (self.flushSessionWrite(s)) {
            self.closeSession(s);
        }
    }

    pub fn sendInfo(self: *ConnectionManager, session_id: u64) void {
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
        self.sendParts(s, &[_][]const u8{ "INFO ", json, "\r\n" }, false);
    }

    pub fn sendOk(self: *ConnectionManager, session_id: u64) void {
        if (!self.verbose) return;
        const s = self.lookupSessionById(session_id) orelse return;
        self.sendParts(s, &[_][]const u8{"OK\r\n"}, false);
    }

    pub fn sendPong(self: *ConnectionManager, session_id: u64) void {
        const s = self.lookupSessionById(session_id) orelse return;
        self.sendParts(s, &[_][]const u8{"PONG\r\n"}, false);
    }

    pub fn sendErr(self: *ConnectionManager, session_id: u64, msg: []const u8) void {
        const s = self.lookupSessionById(session_id) orelse return;
        var frame: [256]u8 = undefined;
        const text = std.fmt.bufPrint(&frame, "-ERR '{s}'\r\n", .{msg}) catch return;
        self.sendParts(s, &[_][]const u8{text}, false);
    }

    pub fn sendErrAndClose(self: *ConnectionManager, session_id: u64, msg: []const u8) void {
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

    pub fn sendMsg(self: *ConnectionManager, session_id: u64, frame: protocol.OutgoingMessage, clustered: bool) void {
        const s = self.lookupSessionById(session_id) orelse return;
        var buf: [256]u8 = undefined;
        const header = if (frame.reply) |reply_to| blk: {
            break :blk std.fmt.bufPrint(&buf, "MSG {s} {d} {s} {d}\r\n", .{ frame.subject, frame.sid, reply_to, frame.payload.len }) catch return;
        } else blk: {
            break :blk std.fmt.bufPrint(&buf, "MSG {s} {d} {d}\r\n", .{ frame.subject, frame.sid, frame.payload.len }) catch return;
        };
        self.sendParts(s, &[_][]const u8{ header, frame.payload, "\r\n" }, clustered);
    }

    pub fn queueClusterMessage(self: *ConnectionManager, session_id: u64, frame: protocol.OutgoingMessage) void {
        self.sendMsg(session_id, frame, true);
    }

    pub fn registerSession(self: *ConnectionManager, s: *session.Session) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        self.sessions.put(s.id, s) catch @panic("OOM");
        self.sessions_by_fd.put(s.stream.handle, s) catch @panic("OOM");
        self.all_sessions.append(self.allocator, s) catch @panic("OOM");
    }

    pub fn lookupSessionById(self: *ConnectionManager, session_id: u64) ?*session.Session {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();
        return self.sessions.get(session_id);
    }

    pub fn lookupSessionByFd(self: *ConnectionManager, fd: posix.socket_t) ?*session.Session {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();
        return self.sessions_by_fd.get(fd);
    }

    fn sendParts(self: *ConnectionManager, s: *session.Session, parts: []const []const u8, clustered: bool) void {
        self.appendParts(s, parts) catch return;
        if (clustered) {
            self.queuePendingWrite(s.id);
            return;
        }
        if (self.flushSessionWrite(s)) {
            self.closeSession(s);
        }
    }

    fn appendParts(self: *ConnectionManager, s: *session.Session, parts: []const []const u8) !void {
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

    fn queuePendingWrite(self: *ConnectionManager, session_id: u64) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        self.pending_writes.append(self.allocator, session_id) catch return;
        self.signalWake() catch {};
    }

    fn signalWake(self: *ConnectionManager) !void {
        const bytes = std.mem.asBytes(&wake_signal);
        _ = posix.write(self.wake_write_fd, bytes) catch |err| switch (err) {
            error.WouldBlock => return,
            else => return err,
        };
    }

    fn drainWakeup(self: *ConnectionManager) !void {
        var scratch: [64]u8 = undefined;
        while (true) {
            const read_len = posix.read(self.wake_read_fd, &scratch) catch |err| switch (err) {
                error.WouldBlock => break,
                else => return err,
            };
            if (read_len == 0) break;
            if (read_len < scratch.len) break;
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

    fn flushSessionWrite(self: *ConnectionManager, s: *session.Session) bool {
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
            self.reactor.setWriteInterest(s.stream.handle, need_write) catch {
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

    fn closeSession(self: *ConnectionManager, s: *session.Session) void {
        s.mutex.lock();
        if (s.closed) {
            s.mutex.unlock();
            return;
        }
        s.closed = true;
        s.closing = false;
        s.write_armed = false;
        s.mutex.unlock();
        self.reactor.unregisterSession(s.stream.handle);

        self.sessions_mutex.lock();
        _ = self.sessions.remove(s.id);
        _ = self.sessions_by_fd.remove(s.stream.handle);
        self.sessions_mutex.unlock();

        s.stream.close();
    }

    fn isSessionClosed(self: *ConnectionManager, s: *session.Session) bool {
        _ = self;
        s.mutex.lock();
        defer s.mutex.unlock();
        return s.closed;
    }

    fn checkAuthTimeouts(self: *ConnectionManager) !void {
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

    fn handleReadable(self: *ConnectionManager, s: *session.Session) !void {
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
                self.command_handler(self.command_ctx, s, cmd) catch |err| {
                    self.sendErr(s.id, @errorName(err));
                    if (self.flushSessionWrite(s)) self.closeSession(s);
                    return err;
                };
                if (self.isSessionClosed(s)) return;
            }
        }
    }

    fn shouldStop(ctx: *anyopaque) bool {
        const self: *ConnectionManager = @ptrCast(@alignCast(ctx));
        return self.stop.load(.acquire);
    }

    fn onListener(ctx: *anyopaque) !void {
        const self: *ConnectionManager = @ptrCast(@alignCast(ctx));
        try self.acceptConnections();
    }

    fn onWakeup(ctx: *anyopaque) !void {
        const self: *ConnectionManager = @ptrCast(@alignCast(ctx));
        try self.drainWakeup();
    }

    fn onReadable(ctx: *anyopaque, fd: posix.socket_t) !void {
        const self: *ConnectionManager = @ptrCast(@alignCast(ctx));
        try self.handleReadableFd(fd);
    }

    fn onWritable(ctx: *anyopaque, fd: posix.socket_t) !void {
        const self: *ConnectionManager = @ptrCast(@alignCast(ctx));
        try self.handleWritableFd(fd);
    }

    fn onTick(ctx: *anyopaque) !void {
        const self: *ConnectionManager = @ptrCast(@alignCast(ctx));
        try self.checkAuthTimeouts();
    }

    fn makeSession(self: *ConnectionManager, fd: posix.socket_t, address: std.net.Address) !*session.Session {
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
};

pub fn deliverClusterMessage(
    ctx: *anyopaque,
    delivery: @import("cluster.zig").Delivery,
    subject: []const u8,
    reply: ?[]const u8,
    payload: []const u8,
) void {
    const self: *ConnectionManager = @ptrCast(@alignCast(ctx));
    _ = self.cluster_delivery_count.fetchAdd(1, .acq_rel);
    self.queueClusterMessage(delivery.session_id, .{
        .session_id = delivery.session_id,
        .sid = delivery.sid,
        .subject = subject,
        .reply = reply,
        .payload = payload,
    });
}
