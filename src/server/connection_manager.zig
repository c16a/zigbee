// SPDX-License-Identifier: MIT
const std = @import("std");
const posix = std.posix;
const auth_mod = @import("auth.zig");
const xev = @import("xev");
const common_mod = @import("common_client");
const crypto_mod = common_mod.crypto_mod;
const protocol = common_mod.protocol_mod;
const session = @import("session.zig");

const auth_timeout_ns: i128 = 5 * std.time.ns_per_s;

pub const CommandHandler = *const fn (*anyopaque, *session.Session, protocol.Command) anyerror!void;

pub const ConnectionManager = struct {
    allocator: std.mem.Allocator,
    auth: ?auth_mod.Store,
    verbose: bool,
    listener: std.net.Server,
    listener_tcp: xev.TCP,
    loop: xev.Loop,
    wakeup: xev.Async,
    auth_timer: xev.Timer,
    accept_completion: xev.Completion = .{},
    wakeup_completion: xev.Completion = .{},
    auth_timer_completion: xev.Completion = .{},
    auth_timer_active: bool = false,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    sessions_mutex: std.Thread.Mutex = .{},
    sessions: std.AutoHashMap(u64, *session.Session),
    sessions_by_fd: std.AutoHashMap(posix.socket_t, *session.Session),
    all_sessions: std.ArrayList(*session.Session) = .empty,
    pending_writes: std.ArrayList(u64) = .empty,
    next_session_id: u64 = 1,
    cluster_delivery_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    command_ctx: *anyopaque,
    command_handler: CommandHandler,
    listener_address: std.net.Address,

    pub fn init(
        self: *ConnectionManager,
        allocator: std.mem.Allocator,
        listener: std.net.Server,
        auth: ?auth_mod.Store,
        verbose: bool,
        command_ctx: *anyopaque,
        command_handler: CommandHandler,
    ) !void {
        var loop = try xev.Loop.init(.{});
        errdefer loop.deinit();

        var wakeup = try xev.Async.init();
        errdefer wakeup.deinit();

        var auth_timer = try xev.Timer.init();
        errdefer auth_timer.deinit();

        self.* = .{
            .allocator = allocator,
            .auth = auth,
            .verbose = verbose,
            .listener = listener,
            .listener_tcp = xev.TCP.initFd(listener.stream.handle),
            .loop = loop,
            .wakeup = wakeup,
            .auth_timer = auth_timer,
            .sessions = std.AutoHashMap(u64, *session.Session).init(allocator),
            .sessions_by_fd = std.AutoHashMap(posix.socket_t, *session.Session).init(allocator),
            .command_ctx = command_ctx,
            .command_handler = command_handler,
            .listener_address = listener.listen_address,
        };
        errdefer self.sessions.deinit();
        errdefer self.sessions_by_fd.deinit();

        if (self.auth != null) {
            self.auth_timer_active = true;
        }
    }

    pub fn deinit(self: *ConnectionManager) void {
        self.listener.deinit();

        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        for (self.all_sessions.items) |s| {
            s.deinit(self.allocator);
            self.allocator.destroy(s);
        }

        if (self.auth) |*auth| {
            auth.deinit();
        }

        self.all_sessions.deinit(self.allocator);
        self.pending_writes.deinit(self.allocator);
        self.sessions.deinit();
        self.sessions_by_fd.deinit();
        self.auth_timer.deinit();
        self.wakeup.deinit();
        self.loop.deinit();
    }

    pub fn requestStop(self: *ConnectionManager) void {
        self.stop.store(true, .release);
        self.wakeup.notify() catch {};
    }

    pub fn serve(self: *ConnectionManager) !void {
        if (self.stop.load(.acquire)) return;

        self.listener_tcp.accept(&self.loop, &self.accept_completion, ConnectionManager, self, onAccept);
        self.wakeup.wait(&self.loop, &self.wakeup_completion, ConnectionManager, self, onWakeup);
        if (self.auth_timer_active) {
            self.auth_timer_run();
        }
        return self.loop.run(.until_done);
    }

    pub fn clusterDeliveryCount(self: *ConnectionManager) u64 {
        return @as(u64, self.cluster_delivery_count.load(.acquire));
    }

    pub fn sendInfo(self: *ConnectionManager, session_id: u64) void {
        const s = self.lookupSessionById(session_id) orelse return;
        const nonce_text = if (s.nonce) |nonce| blk: {
            break :blk crypto_mod.encodeBase64(self.allocator, nonce[0..]) catch return;
        } else null;
        defer if (nonce_text) |text| self.allocator.free(text);
        const info = protocol.Info{
            .port = self.listener_address.getPort(),
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
        var should_kick = false;
        {
            s.mutex.lock();
            defer s.mutex.unlock();
            if (s.closed or s.closing) return;
            if (s.write_offset == s.write_buffer.items.len) {
                s.write_buffer.clearRetainingCapacity();
                s.write_offset = 0;
            }
            s.write_buffer.appendSlice(self.allocator, text) catch return;
            s.write_buffer.appendSlice(self.allocator, "CLOSE\r\n") catch return;
            s.closing = true;
            should_kick = true;
        }
        if (should_kick) self.maybeKickWrite(s);
    }

    pub fn flushSessionWrite(self: *ConnectionManager, s: *session.Session) bool {
        self.maybeKickWrite(s);
        return false;
    }

    pub fn closeSession(self: *ConnectionManager, s: *session.Session) void {
        self.closeSessionLocked(s);
    }

    pub fn isSessionClosed(self: *ConnectionManager, s: *session.Session) bool {
        return self.isSessionClosedLocked(s);
    }

    pub fn checkAuthTimeouts(self: *ConnectionManager) void {
        self.checkAuthTimeoutsLocked();
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
        self.sessions_by_fd.put(s.stream.fd, s) catch @panic("OOM");
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
        self.maybeKickWrite(s);
    }

    fn appendParts(self: *ConnectionManager, s: *session.Session, parts: []const []const u8) !void {
        s.mutex.lock();
        defer s.mutex.unlock();

        if (s.closed or s.closing) return;

        if (s.write_offset == s.write_buffer.items.len) {
            s.write_buffer.clearRetainingCapacity();
            s.write_offset = 0;
        }

        for (parts) |part| {
            try s.write_buffer.appendSlice(self.allocator, part);
        }
    }

    fn maybeKickWrite(self: *ConnectionManager, s: *session.Session) void {
        s.mutex.lock();
        defer s.mutex.unlock();
        if (s.closed or s.write_active) return;
        if (s.write_offset >= s.write_buffer.items.len) return;
        self.startWriteLocked(s);
    }

    fn queuePendingWrite(self: *ConnectionManager, session_id: u64) void {
        self.sessions_mutex.lock();
        defer self.sessions_mutex.unlock();

        self.pending_writes.append(self.allocator, session_id) catch return;
        self.wakeup.notify() catch {};
    }

    fn drainPendingWrites(self: *ConnectionManager) void {
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
            self.maybeKickWrite(s);
        }
    }

    fn startRead(self: *ConnectionManager, s: *session.Session) void {
        s.mutex.lock();
        defer s.mutex.unlock();

        if (s.closed or s.read_active) return;
        s.read_active = true;
        s.stream.read(&self.loop, &s.read_completion, .{ .slice = &s.read_buffer }, session.Session, s, onRead);
    }

    fn startWriteLocked(self: *ConnectionManager, s: *session.Session) void {
        if (s.closed or s.write_active) return;
        if (s.write_offset >= s.write_buffer.items.len) return;
        s.write_active = true;
        const slice = s.write_buffer.items[s.write_offset..];
        s.stream.write(&self.loop, &s.write_completion, .{ .slice = slice }, session.Session, s, onWrite);
    }

    fn closeSessionLocked(self: *ConnectionManager, s: *session.Session) void {
        s.mutex.lock();
        defer s.mutex.unlock();

        if (s.closed) return;
        s.closed = true;
        s.closing = false;
        s.read_active = false;
        s.write_active = false;
        s.write_armed = false;

        self.sessions_mutex.lock();
        _ = self.sessions.remove(s.id);
        _ = self.sessions_by_fd.remove(s.stream.fd);
        self.sessions_mutex.unlock();

        std.posix.close(s.stream.fd);
    }

    fn checkAuthTimeoutsLocked(self: *ConnectionManager) void {
        if (self.auth == null) return;

        const now = std.time.nanoTimestamp();
        for (self.all_sessions.items) |s| {
            s.mutex.lock();
            const timed_out = !s.closed and !s.closing and !s.authenticated and s.auth_deadline_ns != 0 and now >= s.auth_deadline_ns;
            s.mutex.unlock();
            if (!timed_out) continue;
            self.sendErrAndClose(s.id, "auth timeout");
        }
    }

    fn handleReadableData(self: *ConnectionManager, s: *session.Session, data: []const u8) !void {
        s.reader.feed(data) catch {
            self.sendErrAndClose(s.id, "parse error");
            return;
        };

        while (true) {
            const command = s.reader.next() catch |err| {
                self.sendErrAndClose(s.id, @errorName(err));
                return err;
            };
            const cmd = command orelse break;
            self.command_handler(self.command_ctx, s, cmd) catch |err| {
                self.sendErr(s.id, @errorName(err));
                return err;
            };
            if (self.isSessionClosedLocked(s)) return;
        }
    }

    fn isSessionClosedLocked(self: *ConnectionManager, s: *session.Session) bool {
        _ = self;
        s.mutex.lock();
        defer s.mutex.unlock();
        return s.closed;
    }

    fn auth_timer_run(self: *ConnectionManager) void {
        self.auth_timer.run(&self.loop, &self.auth_timer_completion, 1000, ConnectionManager, self, onAuthTimer);
    }

    fn onAccept(ctx: ?*ConnectionManager, _: *xev.Loop, _: *xev.Completion, result: xev.AcceptError!xev.TCP) xev.CallbackAction {
        const self = ctx.?;
        if (self.stop.load(.acquire)) return .disarm;

        const accepted = result catch {
            return .rearm;
        };

        var address: std.net.Address = self.listener_address;
        var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
        if (posix.getpeername(accepted.fd, &address.any, &addr_len)) |_| {
            address = std.net.Address.initPosix(@alignCast(&address.any));
        } else |_| {}

        const s = self.makeSession(accepted, address) catch {
            std.posix.close(accepted.fd);
            return .rearm;
        };
        self.registerSession(s);
        self.startRead(s);
        self.sendInfo(s.id);
        return .rearm;
    }

    fn onWakeup(ctx: ?*ConnectionManager, l: *xev.Loop, _: *xev.Completion, result: xev.Async.WaitError!void) xev.CallbackAction {
        const self = ctx.?;
        _ = result catch {};

        if (self.stop.load(.acquire)) {
            l.stop();
            return .disarm;
        }

        self.drainPendingWrites();
        return .rearm;
    }

    fn onAuthTimer(ctx: ?*ConnectionManager, _: *xev.Loop, _: *xev.Completion, result: xev.Timer.RunError!void) xev.CallbackAction {
        const self = ctx.?;
        _ = result catch {};
        if (self.stop.load(.acquire)) return .disarm;
        self.checkAuthTimeoutsLocked();
        return .rearm;
    }

    fn onRead(
        ctx: ?*session.Session,
        _: *xev.Loop,
        _: *xev.Completion,
        tcp: xev.TCP,
        buf: xev.ReadBuffer,
        result: xev.ReadError!usize,
    ) xev.CallbackAction {
        const s = ctx.?;
        const self: *ConnectionManager = @ptrCast(@alignCast(s.manager_ctx));
        _ = tcp;

        s.mutex.lock();
        s.read_active = false;
        s.mutex.unlock();

        const read_len = result catch {
            self.closeSessionLocked(s);
            return .disarm;
        };
        if (read_len == 0) {
            self.closeSessionLocked(s);
            return .disarm;
        }

        const bytes = switch (buf) {
            .slice => |slice| slice[0..read_len],
            .array => |array| array[0..read_len],
        };

        self.handleReadableData(s, bytes) catch {};
        if (!self.isSessionClosedLocked(s)) self.startRead(s);
        return .disarm;
    }

    fn onWrite(
        ctx: ?*session.Session,
        _: *xev.Loop,
        _: *xev.Completion,
        tcp: xev.TCP,
        buf: xev.WriteBuffer,
        result: xev.WriteError!usize,
    ) xev.CallbackAction {
        const s = ctx.?;
        const self: *ConnectionManager = @ptrCast(@alignCast(s.manager_ctx));
        _ = tcp;
        _ = buf;

        s.mutex.lock();
        defer s.mutex.unlock();

        s.write_active = false;
        const written = result catch {
            self.closeSessionLocked(s);
            return .disarm;
        };

        s.write_offset += written;
        if (s.write_offset >= s.write_buffer.items.len) {
            s.write_buffer.clearRetainingCapacity();
            s.write_offset = 0;
        }

        if (s.closed) return .disarm;

        if (s.write_offset < s.write_buffer.items.len) {
            s.write_active = true;
            const slice = s.write_buffer.items[s.write_offset..];
            s.stream.write(&self.loop, &s.write_completion, .{ .slice = slice }, session.Session, s, onWrite);
            return .disarm;
        }

        if (s.closing) {
            self.closeSessionLocked(s);
        }
        return .disarm;
    }

    fn makeSession(self: *ConnectionManager, stream: xev.TCP, address: std.net.Address) !*session.Session {
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
            stream,
            address,
            self,
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
