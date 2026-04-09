// SPDX-License-Identifier: MIT
const builtin = @import("builtin");

pub const iface = @import("reactor_iface.zig");

pub const Reactor = switch (builtin.os.tag) {
    .linux => @import("reactor/epoll.zig").Reactor,
    .macos => @import("reactor/kqueue.zig").Reactor,
    else => @compileError("reactor support is only implemented for Linux and macOS"),
};
