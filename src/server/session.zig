// SPDX-License-Identifier: MIT
const std = @import("std");
const auth_mod = @import("auth.zig");
const common_mod = @import("common_client");
const crypto_mod = common_mod.crypto_mod;
const protocol = common_mod.protocol_mod;
const xev = @import("xev");

pub const Session = struct {
    id: u64,
    stream: xev.TCP,
    address: std.net.Address,
    manager_ctx: *anyopaque,
    mutex: std.Thread.Mutex = .{},
    reader: protocol.Reader,
    read_buffer: [4096]u8 = undefined,
    read_completion: xev.Completion = .{},
    write_completion: xev.Completion = .{},
    write_buffer: std.ArrayList(u8) = .empty,
    write_offset: usize = 0,
    write_armed: bool = false,
    read_active: bool = false,
    write_active: bool = false,
    closing: bool = false,
    closed: bool = false,
    saw_connect: bool = false,
    authenticated: bool = false,
    auth_deadline_ns: i128 = 0,
    nonce: ?[crypto_mod.seed_length]u8 = null,
    principal: ?*const auth_mod.Principal = null,

    pub fn init(
        allocator: std.mem.Allocator,
        id: u64,
        stream: xev.TCP,
        address: std.net.Address,
        manager_ctx: *anyopaque,
        authenticated: bool,
        auth_deadline_ns: i128,
        nonce: ?[crypto_mod.seed_length]u8,
    ) Session {
        return .{
            .id = id,
            .stream = stream,
            .address = address,
            .manager_ctx = manager_ctx,
            .reader = protocol.Reader.init(allocator),
            .authenticated = authenticated,
            .auth_deadline_ns = auth_deadline_ns,
            .nonce = nonce,
        };
    }

    pub fn deinit(self: *Session, allocator: std.mem.Allocator) void {
        self.reader.deinit();
        self.write_buffer.deinit(allocator);
        if (!self.closed) {
            std.posix.close(self.stream.fd);
        }
        self.* = undefined;
    }
};
