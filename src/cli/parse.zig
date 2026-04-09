// SPDX-License-Identifier: MIT
const std = @import("std");
const protocol_mod = @import("common_client").protocol_mod;
const types = @import("cli_types");

pub fn parseCommand(allocator: std.mem.Allocator, args: []const []const u8) !types.ParsedCommand {
    if (args.len <= 1) return error.HelpRequested;
    if (isHelpArg(args[1])) return error.HelpRequested;

    var cursor = ArgCursor.init(args[1..]);
    var global_server: ?[]const u8 = null;
    var global_zkey_seed: ?[]const u8 = null;

    while (cursor.peek()) |arg| {
        if (std.mem.eql(u8, arg, "--server")) {
            _ = cursor.next();
            global_server = try takeValue(&cursor, "--server");
            continue;
        }
        if (std.mem.eql(u8, arg, "--zkey-seed")) {
            _ = cursor.next();
            global_zkey_seed = try takeValue(&cursor, "--zkey-seed");
            continue;
        }
        break;
    }

    const verb = cursor.next() orelse return error.MissingCommand;

    var command: types.Command = undefined;
    if (std.mem.eql(u8, verb, "bench")) {
        command = .{ .bench = try parseBench(allocator, &cursor) };
    } else {
        const protocol_verb = protocol_mod.parseClientVerb(verb) orelse return error.UnknownCommand;
        command = .{ .client = switch (protocol_verb) {
            .publish => .{ .publish = try parsePublish(allocator, &cursor) },
            .subscribe => .{ .subscribe = try parseSubscribe(allocator, &cursor) },
            .unsubscribe => .{ .unsubscribe = try parseUnsubscribe(&cursor) },
            .request => .{ .request = try parseRequest(allocator, &cursor) },
            .reply => .{ .reply = try parseReply(allocator, &cursor) },
            .ping => .{ .ping = try parsePing(&cursor) },
            .session => .{ .session = try parseSession(&cursor) },
        } };
    }

    if (global_server) |server| {
        command = applyGlobalServer(command, server);
    }
    return .{
        .command = command,
        .zkey_seed_path = global_zkey_seed,
    };
}

fn applyGlobalServer(command: types.Command, server: []const u8) types.Command {
    return switch (command) {
        .client => |cmd| .{ .client = applyClientServer(cmd, server) },
        .bench => |cmd| .{ .bench = applyGlobalServerBench(cmd, server) },
    };
}

fn applyClientServer(cmd: protocol_mod.ClientCommand, server: []const u8) protocol_mod.ClientCommand {
    return switch (cmd) {
        .publish => |payload| .{ .publish = applyServerIfDefault(payload, server) },
        .subscribe => |payload| .{ .subscribe = applyServerIfDefault(payload, server) },
        .unsubscribe => |payload| .{ .unsubscribe = applyServerIfDefault(payload, server) },
        .request => |payload| .{ .request = applyServerIfDefault(payload, server) },
        .reply => |payload| .{ .reply = applyServerIfDefault(payload, server) },
        .ping => |payload| .{ .ping = applyServerIfDefault(payload, server) },
        .session => |payload| .{ .session = applyServerIfDefault(payload, server) },
    };
}

fn applyServerIfDefault(value: anytype, server: []const u8) @TypeOf(value) {
    if (!std.mem.eql(u8, value.server, protocol_mod.default_client_server)) return value;
    var updated = value;
    updated.server = server;
    return updated;
}

fn applyGlobalServerBench(cmd: types.BenchCommand, server: []const u8) types.BenchCommand {
    return switch (cmd) {
        .publish => |bench| .{ .publish = .{ .common = applyGlobalServerCommon(bench.common, server), .subject = bench.subject } },
        .subscribe => |bench| .{ .subscribe = .{ .common = applyGlobalServerCommon(bench.common, server), .subject = bench.subject } },
        .request => |bench| .{ .request = .{ .common = applyGlobalServerCommon(bench.common, server), .subject = bench.subject } },
        .reply => |bench| .{ .reply = .{ .common = applyGlobalServerCommon(bench.common, server), .subject = bench.subject, .queue = bench.queue } },
        .latency => |bench| .{ .latency = .{ .common = applyGlobalServerCommon(bench.common, server) } },
    };
}

fn applyGlobalServerCommon(common: types.BenchCommon, server: []const u8) types.BenchCommon {
    if (std.mem.eql(u8, common.server, protocol_mod.default_client_server)) {
        var updated = common;
        updated.server = server;
        return updated;
    }
    return common;
}

pub fn printUsage() void {
    std.debug.print("Usage: zigbee-cli [--server IP:port] [--zkey-seed path] <command> ...\n", .{});
    std.debug.print("\nCommands:\n", .{});
    for (protocol_mod.client_verbs) |verb| {
        std.debug.print("  {s}\n", .{protocol_mod.clientVerbName(verb)});
    }
    std.debug.print("  bench <pub|sub|request|reply|latency> ...\n", .{});
}

const ArgCursor = struct {
    args: []const []const u8,
    index: usize = 0,

    fn init(args: []const []const u8) ArgCursor {
        return .{ .args = args };
    }

    fn next(self: *ArgCursor) ?[]const u8 {
        if (self.index >= self.args.len) return null;
        const arg = self.args[self.index];
        self.index += 1;
        return arg;
    }

    fn peek(self: *const ArgCursor) ?[]const u8 {
        if (self.index >= self.args.len) return null;
        return self.args[self.index];
    }
};

fn parsePublish(allocator: std.mem.Allocator, cursor: *ArgCursor) !protocol_mod.ClientPublish {
    var cmd = protocol_mod.ClientPublish{ .subject = undefined, .payload = undefined };
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);

    while (cursor.next()) |arg| {
        if (std.mem.eql(u8, arg, "--server")) {
            cmd.server = try takeValue(cursor, "--server");
            continue;
        }
        if (std.mem.eql(u8, arg, "--reply")) {
            cmd.reply = try takeValue(cursor, "--reply");
            continue;
        }
        if (std.mem.startsWith(u8, arg, "-")) return error.UnexpectedArgument;
        try positionals.append(allocator, arg);
    }

    if (positionals.items.len < 1) return error.MissingSubject;
    cmd.subject = positionals.items[0];
    cmd.payload = try joinWords(allocator, positionals.items[1..]);
    return cmd;
}

fn parseSubscribe(allocator: std.mem.Allocator, cursor: *ArgCursor) !protocol_mod.ClientSubscribe {
    var cmd = protocol_mod.ClientSubscribe{ .subject = undefined };
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);

    while (cursor.next()) |arg| {
        if (std.mem.eql(u8, arg, "--server")) {
            cmd.server = try takeValue(cursor, "--server");
            continue;
        }
        if (std.mem.eql(u8, arg, "--queue")) {
            cmd.queue = try takeValue(cursor, "--queue");
            continue;
        }
        if (std.mem.eql(u8, arg, "--sid")) {
            cmd.sid = try parseU64(try takeValue(cursor, "--sid"));
            continue;
        }
        if (std.mem.eql(u8, arg, "--count")) {
            cmd.count = try parseU64(try takeValue(cursor, "--count"));
            continue;
        }
        try positionals.append(allocator, arg);
    }

    if (positionals.items.len < 1) return error.MissingSubject;
    cmd.subject = positionals.items[0];
    return cmd;
}

fn parseUnsubscribe(cursor: *ArgCursor) !protocol_mod.ClientUnsubscribe {
    var cmd = protocol_mod.ClientUnsubscribe{ .sid = 0 };
    while (cursor.next()) |arg| {
        if (std.mem.eql(u8, arg, "--server")) {
            cmd.server = try takeValue(cursor, "--server");
            continue;
        }
        if (std.mem.eql(u8, arg, "--sid")) {
            cmd.sid = try parseU64(try takeValue(cursor, "--sid"));
            continue;
        }
        if (std.mem.eql(u8, arg, "--max")) {
            cmd.max = try parseU64(try takeValue(cursor, "--max"));
            continue;
        }
        if (cmd.sid == 0) {
            cmd.sid = try parseU64(arg);
            continue;
        }
        return error.UnexpectedArgument;
    }
    if (cmd.sid == 0) return error.MissingSid;
    return cmd;
}

fn parseRequest(allocator: std.mem.Allocator, cursor: *ArgCursor) !protocol_mod.ClientRequest {
    var cmd = protocol_mod.ClientRequest{ .subject = undefined, .payload = undefined };
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);

    while (cursor.next()) |arg| {
        if (std.mem.eql(u8, arg, "--server")) {
            cmd.server = try takeValue(cursor, "--server");
            continue;
        }
        try positionals.append(allocator, arg);
    }

    if (positionals.items.len < 1) return error.MissingSubject;
    cmd.subject = positionals.items[0];
    cmd.payload = try joinWords(allocator, positionals.items[1..]);
    return cmd;
}

fn parseReply(allocator: std.mem.Allocator, cursor: *ArgCursor) !protocol_mod.ClientReply {
    var cmd = protocol_mod.ClientReply{ .subject = undefined, .payload = undefined };
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);

    while (cursor.next()) |arg| {
        if (std.mem.eql(u8, arg, "--server")) {
            cmd.server = try takeValue(cursor, "--server");
            continue;
        }
        if (std.mem.eql(u8, arg, "--queue")) {
            cmd.queue = try takeValue(cursor, "--queue");
            continue;
        }
        if (std.mem.eql(u8, arg, "--sid")) {
            cmd.sid = try parseU64(try takeValue(cursor, "--sid"));
            continue;
        }
        if (std.mem.eql(u8, arg, "--count")) {
            cmd.count = try parseU64(try takeValue(cursor, "--count"));
            continue;
        }
        try positionals.append(allocator, arg);
    }

    if (positionals.items.len < 1) return error.MissingSubject;
    cmd.subject = positionals.items[0];
    cmd.payload = try joinWords(allocator, positionals.items[1..]);
    return cmd;
}

fn parsePing(cursor: *ArgCursor) !protocol_mod.ClientPing {
    var cmd = protocol_mod.ClientPing{};
    while (cursor.next()) |arg| {
        if (std.mem.eql(u8, arg, "--server")) {
            cmd.server = try takeValue(cursor, "--server");
            continue;
        }
        if (std.mem.eql(u8, arg, "--count")) {
            cmd.count = try parseU64(try takeValue(cursor, "--count"));
            continue;
        }
        return error.UnexpectedArgument;
    }
    return cmd;
}

fn parseSession(cursor: *ArgCursor) !protocol_mod.ClientSession {
    var cmd = protocol_mod.ClientSession{};
    while (cursor.next()) |arg| {
        if (std.mem.eql(u8, arg, "--server")) {
            cmd.server = try takeValue(cursor, "--server");
            continue;
        }
        return error.UnexpectedArgument;
    }
    return cmd;
}

fn parseBench(allocator: std.mem.Allocator, cursor: *ArgCursor) !types.BenchCommand {
    const verb = cursor.next() orelse return error.MissingCommand;
    if (std.mem.eql(u8, verb, "pub")) return .{ .publish = try parseBenchPublish(allocator, cursor) };
    if (std.mem.eql(u8, verb, "sub")) return .{ .subscribe = try parseBenchSubscribe(allocator, cursor) };
    if (std.mem.eql(u8, verb, "request")) return .{ .request = try parseBenchRequest(allocator, cursor) };
    if (std.mem.eql(u8, verb, "reply")) return .{ .reply = try parseBenchReply(allocator, cursor) };
    if (std.mem.eql(u8, verb, "latency")) return .{ .latency = try parseBenchLatency(allocator, cursor) };
    return error.UnknownCommand;
}

fn parseBenchCommon(allocator: std.mem.Allocator, cursor: *ArgCursor, positionals: *std.ArrayList([]const u8)) !types.BenchCommon {
    var cmd = types.BenchCommon{};
    while (cursor.next()) |arg| {
        if (std.mem.eql(u8, arg, "--server")) {
            cmd.server = try takeValue(cursor, "--server");
            continue;
        }
        if (std.mem.eql(u8, arg, "--clients")) {
            cmd.clients = try std.fmt.parseInt(usize, try takeValue(cursor, "--clients"), 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--msgs")) {
            cmd.msgs = try parseU64(try takeValue(cursor, "--msgs"));
            continue;
        }
        if (std.mem.eql(u8, arg, "--size")) {
            cmd.size = try std.fmt.parseInt(usize, try takeValue(cursor, "--size"), 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--sleep")) {
            cmd.sleep_ns = try parseDuration(try takeValue(cursor, "--sleep"));
            continue;
        }
        if (std.mem.eql(u8, arg, "--no-progress")) {
            cmd.no_progress = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--multi-subject")) {
            cmd.multi_subject = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--multi-subject-max")) {
            cmd.multi_subject_max = try parseU64(try takeValue(cursor, "--multi-subject-max"));
            continue;
        }
        if (std.mem.eql(u8, arg, "--queue")) {
            cmd.queue = try takeValue(cursor, "--queue");
            continue;
        }
        try positionals.append(allocator, arg);
    }
    return cmd;
}

fn parseBenchPublish(allocator: std.mem.Allocator, cursor: *ArgCursor) !types.BenchPublish {
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);
    const common = try parseBenchCommon(allocator, cursor, &positionals);
    if (positionals.items.len < 1) return error.MissingSubject;
    return .{ .common = common, .subject = positionals.items[0] };
}

fn parseBenchSubscribe(allocator: std.mem.Allocator, cursor: *ArgCursor) !types.BenchSubscribe {
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);
    const common = try parseBenchCommon(allocator, cursor, &positionals);
    if (positionals.items.len < 1) return error.MissingSubject;
    return .{ .common = common, .subject = positionals.items[0] };
}

fn parseBenchRequest(allocator: std.mem.Allocator, cursor: *ArgCursor) !types.BenchRequest {
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);
    const common = try parseBenchCommon(allocator, cursor, &positionals);
    if (positionals.items.len < 1) return error.MissingSubject;
    return .{ .common = common, .subject = positionals.items[0] };
}

fn parseBenchReply(allocator: std.mem.Allocator, cursor: *ArgCursor) !types.BenchReply {
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);
    const common = try parseBenchCommon(allocator, cursor, &positionals);
    if (positionals.items.len < 1) return error.MissingSubject;
    return .{ .common = common, .subject = positionals.items[0], .queue = common.queue orelse "bench" };
}

fn parseBenchLatency(allocator: std.mem.Allocator, cursor: *ArgCursor) !types.BenchLatency {
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);
    const common = try parseBenchCommon(allocator, cursor, &positionals);
    if (positionals.items.len != 0) return error.UnexpectedArgument;
    return .{ .common = common };
}

fn takeValue(cursor: *ArgCursor, flag: []const u8) ![]const u8 {
    _ = flag;
    return cursor.next() orelse return error.MissingValue;
}

fn joinWords(allocator: std.mem.Allocator, words: []const []const u8) ![]u8 {
    if (words.len == 0) return try allocator.dupe(u8, "");
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(allocator);
    for (words, 0..) |word, index| {
        if (index > 0) try list.append(allocator, ' ');
        try list.appendSlice(allocator, word);
    }
    return try list.toOwnedSlice(allocator);
}

fn parseU64(text: []const u8) !u64 {
    return std.fmt.parseInt(u64, text, 10) catch error.InvalidNumber;
}

fn parseDuration(text: []const u8) !u64 {
    if (std.mem.endsWith(u8, text, "ms")) {
        return (try parseU64(text[0 .. text.len - 2])) * std.time.ns_per_ms;
    }
    if (std.mem.endsWith(u8, text, "us")) {
        return (try parseU64(text[0 .. text.len - 2])) * std.time.ns_per_us;
    }
    if (std.mem.endsWith(u8, text, "ns")) {
        return try parseU64(text[0 .. text.len - 2]);
    }
    if (std.mem.endsWith(u8, text, "s")) {
        return (try parseU64(text[0 .. text.len - 1])) * std.time.ns_per_s;
    }
    return (try parseU64(text)) * std.time.ns_per_ms;
}

fn isHelpArg(arg: []const u8) bool {
    return std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "help");
}

test "parse publish command" {
    const argv = [_][]const u8{ "zigbee-cli", "pub", "--server", "127.0.0.1:4222", "foo", "hello", "world" };
    const parsed = try parseCommand(std.testing.allocator, argv[0..]);
    defer std.testing.allocator.free(parsed.command.client.publish.payload);
    try std.testing.expect(parsed.command == .client);
    try std.testing.expect(parsed.command.client == .publish);
    try std.testing.expectEqualStrings("foo", parsed.command.client.publish.subject);
    try std.testing.expectEqualStrings("hello world", parsed.command.client.publish.payload);
    try std.testing.expectEqualStrings("127.0.0.1:4222", parsed.command.client.publish.server);
}

test "parse global server ping command" {
    const argv = [_][]const u8{ "zigbee-cli", "--server", "127.0.0.1:4222", "ping" };
    const parsed = try parseCommand(std.testing.allocator, argv[0..]);
    try std.testing.expect(parsed.command == .client);
    try std.testing.expect(parsed.command.client == .ping);
    try std.testing.expectEqualStrings("127.0.0.1:4222", parsed.command.client.ping.server);
}

test "parse session command" {
    const argv = [_][]const u8{ "zigbee-cli", "session" };
    const parsed = try parseCommand(std.testing.allocator, argv[0..]);
    try std.testing.expect(parsed.command == .client);
    try std.testing.expect(parsed.command.client == .session);
    try std.testing.expectEqualStrings(protocol_mod.default_client_server, parsed.command.client.session.server);
}

test "parse bench request command" {
    const argv = [_][]const u8{ "zigbee-cli", "bench", "request", "--clients", "4", "--msgs", "1000", "foo" };
    const parsed = try parseCommand(std.testing.allocator, argv[0..]);
    try std.testing.expect(parsed.command == .bench);
    try std.testing.expect(parsed.command.bench == .request);
    try std.testing.expectEqual(@as(usize, 4), parsed.command.bench.request.common.clients);
    try std.testing.expectEqual(@as(u64, 1000), parsed.command.bench.request.common.msgs);
    try std.testing.expectEqualStrings("foo", parsed.command.bench.request.subject);
}
