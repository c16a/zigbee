// SPDX-License-Identifier: MIT
const std = @import("std");

pub const Session = struct {
    id: u64,
    stream: std.net.Stream,
    write_mutex: std.Thread.Mutex = .{},
    close_mutex: std.Thread.Mutex = .{},
    closed: bool = false,

    pub fn close(self: *Session) void {
        self.close_mutex.lock();
        defer self.close_mutex.unlock();
        if (self.closed) return;
        self.closed = true;
        self.stream.close();
    }
};
