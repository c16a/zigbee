// SPDX-License-Identifier: MIT
const std = @import("std");
const protocol_mod = @import("common_client").protocol_mod;

pub const ParsedCommand = struct {
    command: Command,
    zkey_seed_path: ?[]const u8 = null,
};

pub const Command = union(enum) {
    client: protocol_mod.ClientCommand,
    bench: BenchCommand,
};

pub const BenchCommon = struct {
    server: []const u8 = protocol_mod.default_client_server,
    clients: usize = 1,
    msgs: usize = 1000,
    size: usize = 128,
    sleep_ns: u64 = 0,
    no_progress: bool = false,
    multi_subject: bool = false,
    multi_subject_max: usize = 100_000,
    queue: ?[]const u8 = null,
};

pub const BenchPublish = struct {
    common: BenchCommon,
    subject: []const u8,
};

pub const BenchSubscribe = struct {
    common: BenchCommon,
    subject: []const u8,
};

pub const BenchRequest = struct {
    common: BenchCommon,
    subject: []const u8,
};

pub const BenchReply = struct {
    common: BenchCommon,
    subject: []const u8,
    queue: []const u8 = "bench",
};

pub const BenchLatency = struct {
    common: BenchCommon,
};

pub const BenchCommand = union(enum) {
    publish: BenchPublish,
    subscribe: BenchSubscribe,
    request: BenchRequest,
    reply: BenchReply,
    latency: BenchLatency,
};

pub const StartGate = struct {
    ready: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    go: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

pub const BenchTotals = struct {
    messages: u64 = 0,
    bytes: u64 = 0,
};
