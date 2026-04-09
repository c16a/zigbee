// SPDX-License-Identifier: MIT
const std = @import("std");
const config_mod = @import("config.zig");
const common_mod = @import("common_client");
const crypto_mod = common_mod.crypto_mod;
const protocol_mod = common_mod.protocol_mod;

pub const Permissions = struct {
    allow_publish: []const []const u8,
    allow_subscribe: []const []const u8,
};

pub const Principal = struct {
    name: ?[]const u8,
    public_key: [crypto_mod.public_key_length]u8,
    permissions: Permissions,
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    principals: []Principal = &.{},
    by_key: std.AutoHashMap([crypto_mod.public_key_length]u8, usize),

    pub fn initFromConfig(allocator: std.mem.Allocator, auth: config_mod.Auth) !Store {
        if (!std.mem.eql(u8, auth.mode, "static_zkey")) return error.InvalidAuthMode;

        var principals = std.ArrayList(Principal).empty;
        var by_key = std.AutoHashMap([crypto_mod.public_key_length]u8, usize).init(allocator);
        errdefer {
            freePrincipals(allocator, principals.items);
            principals.deinit(allocator);
            by_key.deinit();
        }

        for (auth.users) |user| {
            const public_key = try decodePublicKey(user.public_key);
            if (by_key.contains(public_key.bytes)) return error.DuplicatePublicKey;

            const principal = Principal{
                .name = if (user.name) |name| try allocator.dupe(u8, name) else null,
                .public_key = public_key.bytes,
                .permissions = .{
                    .allow_publish = try duplicateAndValidatePatterns(allocator, user.allow_publish),
                    .allow_subscribe = try duplicateAndValidatePatterns(allocator, user.allow_subscribe),
                },
            };
            try principals.append(allocator, principal);
            try by_key.put(public_key.bytes, principals.items.len - 1);
        }

        return .{
            .allocator = allocator,
            .principals = try principals.toOwnedSlice(allocator),
            .by_key = by_key,
        };
    }

    pub fn deinit(self: *Store) void {
        freePrincipals(self.allocator, self.principals);
        self.allocator.free(self.principals);
        self.by_key.deinit();
        self.* = undefined;
    }

    pub fn lookupByPublicKey(self: *const Store, key: [crypto_mod.public_key_length]u8) ?*const Principal {
        const index = self.by_key.get(key) orelse return null;
        return &self.principals[index];
    }
};

pub fn decodePublicKey(text: []const u8) !crypto_mod.Ed25519.PublicKey {
    const bytes = try crypto_mod.decodeBase64Fixed(crypto_mod.public_key_length, text);
    return crypto_mod.Ed25519.PublicKey.fromBytes(bytes) catch error.InvalidPublicKey;
}

pub fn decodeSignature(text: []const u8) !crypto_mod.Ed25519.Signature {
    const bytes = try crypto_mod.decodeBase64Fixed(crypto_mod.signature_length, text);
    return crypto_mod.Ed25519.Signature.fromBytes(bytes);
}

pub fn verifyNonceSignature(public_key: crypto_mod.Ed25519.PublicKey, nonce: []const u8, signature: crypto_mod.Ed25519.Signature) bool {
    return crypto_mod.verifySignature(public_key, nonce, signature);
}

pub fn canPublish(perms: *const Permissions, subject: []const u8) bool {
    return matchesAny(perms.allow_publish, subject);
}

pub fn canSubscribe(perms: *const Permissions, subject: []const u8) bool {
    return matchesAny(perms.allow_subscribe, subject);
}

fn matchesAny(patterns: []const []const u8, subject: []const u8) bool {
    for (patterns) |pattern| {
        if (protocol_mod.matchesSubject(pattern, subject)) return true;
    }
    return false;
}

fn duplicateAndValidatePatterns(allocator: std.mem.Allocator, patterns: []const []const u8) ![]const []const u8 {
    var list = std.ArrayList([]const u8).empty;
    errdefer {
        for (list.items) |pattern| allocator.free(pattern);
        list.deinit(allocator);
    }
    for (patterns) |pattern| {
        try validateSubjectPattern(pattern);
        try list.append(allocator, try allocator.dupe(u8, pattern));
    }
    return try list.toOwnedSlice(allocator);
}

fn freePatterns(allocator: std.mem.Allocator, patterns: []const []const u8) void {
    for (patterns) |pattern| allocator.free(pattern);
    allocator.free(patterns);
}

fn freePrincipals(allocator: std.mem.Allocator, principals: []const Principal) void {
    for (principals) |principal| {
        if (principal.name) |name| allocator.free(name);
        freePatterns(allocator, principal.permissions.allow_publish);
        freePatterns(allocator, principal.permissions.allow_subscribe);
    }
}

fn validateSubjectPattern(pattern: []const u8) !void {
    if (pattern.len == 0) return error.InvalidSubjectPattern;

    var token_start: usize = 0;
    while (token_start < pattern.len) {
        var token_end = token_start;
        while (token_end < pattern.len and pattern[token_end] != '.') : (token_end += 1) {}
        const token = pattern[token_start..token_end];
        if (token.len == 0) return error.InvalidSubjectPattern;
        if (std.mem.indexOfScalar(u8, token, '*') != null and !std.mem.eql(u8, token, "*")) {
            return error.InvalidSubjectPattern;
        }
        if (std.mem.indexOfScalar(u8, token, '>') != null and !std.mem.eql(u8, token, ">")) {
            return error.InvalidSubjectPattern;
        }
        if (std.mem.eql(u8, token, ">") and token_end != pattern.len) return error.InvalidSubjectPattern;
        token_start = token_end + 1;
    }
}

test "permissions use subject matcher" {
    const perms = Permissions{
        .allow_publish = &.{ "foo.>", "bar.baz" },
        .allow_subscribe = &.{ "svc.*" },
    };
    try std.testing.expect(canPublish(&perms, "foo.bar.baz"));
    try std.testing.expect(canPublish(&perms, "bar.baz"));
    try std.testing.expect(!canPublish(&perms, "nope"));
    try std.testing.expect(canSubscribe(&perms, "svc.one"));
    try std.testing.expect(!canSubscribe(&perms, "svc.one.two"));
}
