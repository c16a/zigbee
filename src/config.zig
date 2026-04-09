// SPDX-License-Identifier: MIT
const std = @import("std");

pub const default_config_path = "zigbee.config.json";

pub const AuthUser = struct {
    name: ?[]const u8 = null,
    public_key: []const u8,
    allow_publish: []const []const u8 = &.{},
    allow_subscribe: []const []const u8 = &.{},
};

pub const Auth = struct {
    mode: []const u8 = "static_zkey",
    users: []const AuthUser = &.{},
};

pub const Config = struct {
    listen_address: ?[]const u8 = null,
    verbose: bool = true,
    auth: ?Auth = null,
};

pub const LoadedConfig = struct {
    parsed: std.json.Parsed(Config),

    pub fn deinit(self: *LoadedConfig) void {
        self.parsed.deinit();
    }

    pub fn value(self: LoadedConfig) Config {
        return self.parsed.value;
    }
};

pub fn load(allocator: std.mem.Allocator, path: []const u8, required: bool) !?LoadedConfig {
    return loadFromDir(std.fs.cwd(), allocator, path, required);
}

pub fn loadFromDir(dir: std.fs.Dir, allocator: std.mem.Allocator, path: []const u8, required: bool) !?LoadedConfig {
    const file = dir.openFile(path, .{}) catch |err| switch (err) {
        error.FileNotFound => {
            if (required) return error.FileNotFound;
            return null;
        },
        else => return err,
    };
    defer file.close();

    const contents = try file.readToEndAlloc(allocator, 128 * 1024);
    defer allocator.free(contents);

    const parsed = try std.json.parseFromSlice(Config, allocator, contents, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    return .{ .parsed = parsed };
}
