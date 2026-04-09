// SPDX-License-Identifier: MIT
const std = @import("std");
const posix = std.posix;
const iface = @import("../reactor_iface.zig");

pub const Reactor = struct {
    callbacks: iface.Callbacks,
    kqueue_fd: posix.socket_t,
    listener_fd: posix.socket_t,
    wake_fd: posix.socket_t,

    pub fn init(callbacks: iface.Callbacks, listener_fd: posix.socket_t, wake_fd: posix.socket_t) !Reactor {
        const kqueue_fd = try posix.kqueue();
        errdefer posix.close(kqueue_fd);

        var reactor = Reactor{
            .callbacks = callbacks,
            .kqueue_fd = kqueue_fd,
            .listener_fd = listener_fd,
            .wake_fd = wake_fd,
        };
        try reactor.addEvent(listener_fd, std.c.EVFILT.READ, std.c.EV.ADD | std.c.EV.ENABLE);
        try reactor.addEvent(wake_fd, std.c.EVFILT.READ, std.c.EV.ADD | std.c.EV.ENABLE);
        return reactor;
    }

    pub fn deinit(self: *Reactor) void {
        posix.close(self.kqueue_fd);
        self.* = undefined;
    }

    pub fn run(self: *Reactor) !void {
        var events: [64]posix.Kevent = undefined;
        while (!self.callbacks.should_stop(self.callbacks.ctx)) {
            const count = try posix.kevent(self.kqueue_fd, &.{}, events[0..], null);
            for (events[0..count]) |event| {
                const fd: posix.socket_t = @intCast(event.ident);
                if (fd == self.listener_fd) {
                    try self.callbacks.on_listener(self.callbacks.ctx);
                    continue;
                }
                if (fd == self.wake_fd) {
                    try self.callbacks.on_wakeup(self.callbacks.ctx);
                    continue;
                }
                if ((event.filter == std.c.EVFILT.READ) and ((event.flags & (std.c.EV.EOF | std.c.EV.ERROR)) == 0)) {
                    try self.callbacks.on_readable(self.callbacks.ctx, fd);
                } else if (event.filter == std.c.EVFILT.READ) {
                    try self.callbacks.on_readable(self.callbacks.ctx, fd);
                }
                if (event.filter == std.c.EVFILT.WRITE) {
                    try self.callbacks.on_writable(self.callbacks.ctx, fd);
                }
            }
            try self.callbacks.on_tick(self.callbacks.ctx);
        }
    }

    pub fn registerSession(self: *Reactor, fd: posix.socket_t) !void {
        try self.addEvent(fd, std.c.EVFILT.READ, std.c.EV.ADD | std.c.EV.ENABLE);
    }

    pub fn setWriteInterest(self: *Reactor, fd: posix.socket_t, enable: bool) !void {
        try self.addEvent(fd, std.c.EVFILT.WRITE, if (enable) std.c.EV.ADD | std.c.EV.ENABLE else std.c.EV.DELETE);
    }

    pub fn unregisterSession(self: *Reactor, fd: posix.socket_t) void {
        _ = posix.kevent(self.kqueue_fd, &[_]posix.Kevent{
            .{
                .ident = @intCast(fd),
                .filter = std.c.EVFILT.READ,
                .flags = std.c.EV.DELETE,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            },
            .{
                .ident = @intCast(fd),
                .filter = std.c.EVFILT.WRITE,
                .flags = std.c.EV.DELETE,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            },
        }, &.{}, null) catch {};
    }

    fn addEvent(self: *Reactor, fd: posix.socket_t, filter: i16, flags: u16) !void {
        const event: posix.Kevent = .{
            .ident = @intCast(fd),
            .filter = filter,
            .flags = flags,
            .fflags = 0,
            .data = 0,
            .udata = 0,
        };
        _ = try posix.kevent(self.kqueue_fd, &.{event}, &.{}, null);
    }
};
