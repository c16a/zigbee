// SPDX-License-Identifier: MIT
const std = @import("std");
const config_mod = @import("config.zig");
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

    const config_path = try parseConfigPath(args);
    var config = try config_mod.load(allocator, config_path, !std.mem.eql(u8, config_path, config_mod.default_config_path));
    defer if (config) |*loaded| loaded.deinit();

    var broker = broker_mod.Broker.init(allocator);
    defer broker.deinit();

    const address = try resolveListenAddress(config);
    var server = try server_mod.Server.start(allocator, &broker, address);
    defer server.deinit();

    std.log.info("zigbee server listening on {any}", .{address});
    try server.serve();
}

fn parseConfigPath(args: [][:0]u8) ![]const u8 {
    var config_path: []const u8 = config_mod.default_config_path;
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--config")) {
            if (i + 1 >= args.len) return error.MissingConfigPath;
            config_path = args[i + 1];
            i += 1;
            continue;
        }
        return error.UnexpectedArgument;
    }
    return config_path;
}

fn resolveListenAddress(config: ?config_mod.LoadedConfig) !std.net.Address {
    const address = if (config) |loaded| blk: {
        if (loaded.value().listen_address) |listen_address| {
            break :blk try std.net.Address.parseIpAndPort(listen_address);
        }
        break :blk try std.net.Address.parseIp("0.0.0.0", 4222);
    } else try std.net.Address.parseIp("0.0.0.0", 4222);

    return address;
}
