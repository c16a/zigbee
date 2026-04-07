// SPDX-License-Identifier: MIT
const std = @import("std");
const broker_mod = @import("broker.zig");
const server_mod = @import("server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var thread_safe = std.heap.ThreadSafeAllocator{
        .child_allocator = gpa.allocator(),
    };
    const allocator = thread_safe.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);
    const port = try parsePort(args);

    var broker = broker_mod.Broker.init(allocator);
    defer broker.deinit();

    const address = try std.net.Address.parseIp("0.0.0.0", port);
    var server = try server_mod.Server.start(allocator, &broker, address);
    defer server.deinit();

    try server.serve();
}

fn parsePort(args: [][:0]u8) !u16 {
    var port: u16 = 4222;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--port")) {
            if (i + 1 >= args.len) return error.MissingPort;
            port = try std.fmt.parseInt(u16, args[i + 1], 10);
            i += 1;
            continue;
        }
        port = try std.fmt.parseInt(u16, arg, 10);
    }
    return port;
}
