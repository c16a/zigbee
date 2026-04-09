// SPDX-License-Identifier: MIT
const std = @import("std");

pub const Callbacks = struct {
    ctx: *anyopaque,
    should_stop: *const fn (*anyopaque) bool,
    on_listener: *const fn (*anyopaque) anyerror!void,
    on_wakeup: *const fn (*anyopaque) anyerror!void,
    on_readable: *const fn (*anyopaque, std.posix.socket_t) anyerror!void,
    on_writable: *const fn (*anyopaque, std.posix.socket_t) anyerror!void,
    on_tick: *const fn (*anyopaque) anyerror!void,
};
