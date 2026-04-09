// SPDX-License-Identifier: MIT
const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const iface = @import("../reactor_iface.zig");

pub const Reactor = struct {
    callbacks: iface.Callbacks,
    epoll_fd: posix.socket_t,
    listener_fd: posix.socket_t,
    wake_fd: posix.socket_t,

    pub fn init(callbacks: iface.Callbacks, listener_fd: posix.socket_t, wake_fd: posix.socket_t) !Reactor {
        const epoll_fd = try posix.epoll_create1(linux.EPOLL.CLOEXEC);
        errdefer posix.close(epoll_fd);

        var reactor = Reactor{
            .callbacks = callbacks,
            .epoll_fd = epoll_fd,
            .listener_fd = listener_fd,
            .wake_fd = wake_fd,
        };
        try reactor.addFd(listener_fd, linux.EPOLL.IN);
        try reactor.addFd(wake_fd, linux.EPOLL.IN);
        return reactor;
    }

    pub fn deinit(self: *Reactor) void {
        posix.close(self.epoll_fd);
        self.* = undefined;
    }

    pub fn run(self: *Reactor) !void {
        var events: [64]linux.epoll_event = undefined;
        while (!self.callbacks.should_stop(self.callbacks.ctx)) {
            const count = posix.epoll_wait(self.epoll_fd, events[0..], 1000);
            for (events[0..count]) |event| {
                const fd = event.data.fd;
                if (fd == self.listener_fd) {
                    try self.callbacks.on_listener(self.callbacks.ctx);
                    continue;
                }
                if (fd == self.wake_fd) {
                    try self.callbacks.on_wakeup(self.callbacks.ctx);
                    continue;
                }
                if ((event.events & (linux.EPOLL.IN | linux.EPOLL.HUP | linux.EPOLL.RDHUP | linux.EPOLL.ERR)) != 0) {
                    try self.callbacks.on_readable(self.callbacks.ctx, fd);
                }
                if ((event.events & linux.EPOLL.OUT) != 0) {
                    try self.callbacks.on_writable(self.callbacks.ctx, fd);
                }
            }
            try self.callbacks.on_tick(self.callbacks.ctx);
        }
    }

    pub fn registerSession(self: *Reactor, fd: posix.socket_t) !void {
        try self.addFd(fd, linux.EPOLL.IN);
    }

    pub fn setWriteInterest(self: *Reactor, fd: posix.socket_t, enable: bool) !void {
        const events: u32 = if (enable) linux.EPOLL.IN | linux.EPOLL.OUT else linux.EPOLL.IN;
        var event: linux.epoll_event = .{
            .events = events,
            .data = .{ .fd = fd },
        };
        try posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_MOD, fd, &event);
    }

    pub fn unregisterSession(self: *Reactor, fd: posix.socket_t) void {
        _ = posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_DEL, fd, null) catch {};
    }

    fn addFd(self: *Reactor, fd: posix.socket_t, events: u32) !void {
        var event: linux.epoll_event = .{
            .events = events,
            .data = .{ .fd = fd },
        };
        try posix.epoll_ctl(self.epoll_fd, linux.EPOLL.CTL_ADD, fd, &event);
    }
};
