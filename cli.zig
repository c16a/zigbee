// SPDX-License-Identifier: MIT
const std = @import("std");
const client_mod = @import("client.zig");

const default_server = "127.0.0.1:4222";

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    var thread_safe = std.heap.ThreadSafeAllocator{
        .child_allocator = gpa.allocator(),
    };
    const allocator = thread_safe.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const command = try parseCommand(allocator, args);
    try runCommand(allocator, command);
}

const Command = union(enum) {
    publish: PublishCommand,
    subscribe: SubscribeCommand,
    unsubscribe: UnsubscribeCommand,
    request: RequestCommand,
    reply: ReplyCommand,
    ping: PingCommand,
    bench: BenchCommand,
};

const PublishCommand = struct {
    server: []const u8 = default_server,
    subject: []const u8,
    payload: []const u8,
    reply: ?[]const u8 = null,
};

const SubscribeCommand = struct {
    server: []const u8 = default_server,
    subject: []const u8,
    queue: ?[]const u8 = null,
    sid: u64 = 1,
    count: ?u64 = null,
};

const UnsubscribeCommand = struct {
    server: []const u8 = default_server,
    sid: u64,
    max: ?u64 = null,
};

const RequestCommand = struct {
    server: []const u8 = default_server,
    subject: []const u8,
    payload: []const u8,
};

const ReplyCommand = struct {
    server: []const u8 = default_server,
    subject: []const u8,
    payload: []const u8,
    queue: ?[]const u8 = null,
    sid: u64 = 1,
    count: ?u64 = null,
};

const PingCommand = struct {
    server: []const u8 = default_server,
    count: u64 = 1,
};

const BenchCommon = struct {
    server: []const u8 = default_server,
    clients: usize = 1,
    msgs: u64 = 1000,
    size: usize = 128,
    sleep_ns: u64 = 0,
    no_progress: bool = false,
    multi_subject: bool = false,
    multi_subject_max: u64 = 100_000,
    queue: ?[]const u8 = null,
};

const BenchPublish = struct {
    common: BenchCommon,
    subject: []const u8,
};

const BenchSubscribe = struct {
    common: BenchCommon,
    subject: []const u8,
};

const BenchRequest = struct {
    common: BenchCommon,
    subject: []const u8,
};

const BenchReply = struct {
    common: BenchCommon,
    subject: []const u8,
    queue: []const u8 = "bench",
};

const BenchLatency = struct {
    common: BenchCommon,
};

const BenchCommand = union(enum) {
    publish: BenchPublish,
    subscribe: BenchSubscribe,
    request: BenchRequest,
    reply: BenchReply,
    latency: BenchLatency,
};

const ParsedCommand = Command;

fn parseCommand(allocator: std.mem.Allocator, args: []const []const u8) !ParsedCommand {
    var cursor = ArgCursor.init(args[1..]);
    const verb = cursor.next() orelse return error.MissingCommand;

    if (std.mem.eql(u8, verb, "pub")) return .{ .publish = try parsePublish(allocator, &cursor) };
    if (std.mem.eql(u8, verb, "sub")) return .{ .subscribe = try parseSubscribe(allocator, &cursor) };
    if (std.mem.eql(u8, verb, "unsub")) return .{ .unsubscribe = try parseUnsubscribe(&cursor) };
    if (std.mem.eql(u8, verb, "request")) return .{ .request = try parseRequest(allocator, &cursor) };
    if (std.mem.eql(u8, verb, "reply")) return .{ .reply = try parseReply(allocator, &cursor) };
    if (std.mem.eql(u8, verb, "ping")) return .{ .ping = try parsePing(&cursor) };
    if (std.mem.eql(u8, verb, "bench")) return .{ .bench = try parseBench(allocator, &cursor) };

    return error.UnknownCommand;
}

fn runCommand(allocator: std.mem.Allocator, command: ParsedCommand) !void {
    switch (command) {
        .publish => |cmd| try runPublish(allocator, cmd),
        .subscribe => |cmd| try runSubscribe(allocator, cmd),
        .unsubscribe => |cmd| try runUnsubscribe(allocator, cmd),
        .request => |cmd| try runRequest(allocator, cmd),
        .reply => |cmd| try runReply(allocator, cmd),
        .ping => |cmd| try runPing(allocator, cmd),
        .bench => |cmd| try runBench(allocator, cmd),
    }
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

fn parsePublish(allocator: std.mem.Allocator, cursor: *ArgCursor) !PublishCommand {
    var cmd = PublishCommand{ .subject = undefined, .payload = undefined };
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

fn parseSubscribe(allocator: std.mem.Allocator, cursor: *ArgCursor) !SubscribeCommand {
    var cmd = SubscribeCommand{ .subject = undefined };
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

fn parseUnsubscribe(cursor: *ArgCursor) !UnsubscribeCommand {
    var cmd = UnsubscribeCommand{ .sid = 0 };
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

fn parseRequest(allocator: std.mem.Allocator, cursor: *ArgCursor) !RequestCommand {
    var cmd = RequestCommand{ .subject = undefined, .payload = undefined };
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

fn parseReply(allocator: std.mem.Allocator, cursor: *ArgCursor) !ReplyCommand {
    var cmd = ReplyCommand{ .subject = undefined, .payload = undefined };
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

fn parsePing(cursor: *ArgCursor) !PingCommand {
    var cmd = PingCommand{};
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

fn parseBench(allocator: std.mem.Allocator, cursor: *ArgCursor) !BenchCommand {
    const verb = cursor.next() orelse return error.MissingCommand;
    if (std.mem.eql(u8, verb, "pub")) return .{ .publish = try parseBenchPublish(allocator, cursor) };
    if (std.mem.eql(u8, verb, "sub")) return .{ .subscribe = try parseBenchSubscribe(allocator, cursor) };
    if (std.mem.eql(u8, verb, "request")) return .{ .request = try parseBenchRequest(allocator, cursor) };
    if (std.mem.eql(u8, verb, "reply")) return .{ .reply = try parseBenchReply(allocator, cursor) };
    if (std.mem.eql(u8, verb, "latency")) return .{ .latency = try parseBenchLatency(allocator, cursor) };
    return error.UnknownCommand;
}

fn parseBenchCommon(allocator: std.mem.Allocator, cursor: *ArgCursor, positionals: *std.ArrayList([]const u8)) !BenchCommon {
    var cmd = BenchCommon{};
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

fn parseBenchPublish(allocator: std.mem.Allocator, cursor: *ArgCursor) !BenchPublish {
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);
    const common = try parseBenchCommon(allocator, cursor, &positionals);
    if (positionals.items.len < 1) return error.MissingSubject;
    return .{ .common = common, .subject = positionals.items[0] };
}

fn parseBenchSubscribe(allocator: std.mem.Allocator, cursor: *ArgCursor) !BenchSubscribe {
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);
    const common = try parseBenchCommon(allocator, cursor, &positionals);
    if (positionals.items.len < 1) return error.MissingSubject;
    return .{ .common = common, .subject = positionals.items[0] };
}

fn parseBenchRequest(allocator: std.mem.Allocator, cursor: *ArgCursor) !BenchRequest {
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);
    const common = try parseBenchCommon(allocator, cursor, &positionals);
    if (positionals.items.len < 1) return error.MissingSubject;
    return .{ .common = common, .subject = positionals.items[0] };
}

fn parseBenchReply(allocator: std.mem.Allocator, cursor: *ArgCursor) !BenchReply {
    var positionals: std.ArrayList([]const u8) = .empty;
    defer positionals.deinit(allocator);
    const common = try parseBenchCommon(allocator, cursor, &positionals);
    if (positionals.items.len < 1) return error.MissingSubject;
    return .{ .common = common, .subject = positionals.items[0], .queue = common.queue orelse "bench" };
}

fn parseBenchLatency(allocator: std.mem.Allocator, cursor: *ArgCursor) !BenchLatency {
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

fn runPublish(allocator: std.mem.Allocator, cmd: PublishCommand) !void {
    var client = try connectClient(allocator, cmd.server);
    defer client.deinit();
    try client.publish(cmd.subject, cmd.reply, cmd.payload);
    std.debug.print("published {d} bytes to \"{s}\"\n", .{ cmd.payload.len, cmd.subject });
}

fn runSubscribe(allocator: std.mem.Allocator, cmd: SubscribeCommand) !void {
    var client = try connectClient(allocator, cmd.server);
    defer client.deinit();
    try client.subscribe(cmd.subject, cmd.queue, cmd.sid);
    var seen: u64 = 0;
    while (true) {
        const maybe_msg = try client.nextMessage();
        const msg = maybe_msg orelse return error.UnexpectedEndOfStream;
        if (!std.mem.eql(u8, msg.subject, cmd.subject)) continue;
        seen += 1;
        printMessage("received", msg);
        if (cmd.count) |limit| {
            if (seen >= limit) {
                try client.unsubscribe(cmd.sid, null);
                return;
            }
        }
    }
}

fn runUnsubscribe(allocator: std.mem.Allocator, cmd: UnsubscribeCommand) !void {
    var client = try connectClient(allocator, cmd.server);
    defer client.deinit();
    try client.unsubscribe(cmd.sid, cmd.max);
    std.debug.print("sent UNSUB for sid {d}\n", .{cmd.sid});
}

fn runRequest(allocator: std.mem.Allocator, cmd: RequestCommand) !void {
    var client = try connectClient(allocator, cmd.server);
    defer client.deinit();
    const inbox = try client_mod.makeInbox(allocator, "request");
    defer allocator.free(inbox);
    try client.subscribe(inbox, null, 1);
    try client.publish(cmd.subject, inbox, cmd.payload);
    const msg = try client.waitForMessage(inbox);
    std.debug.print("{s}\n", .{msg.payload});
}

fn runReply(allocator: std.mem.Allocator, cmd: ReplyCommand) !void {
    var client = try connectClient(allocator, cmd.server);
    defer client.deinit();
    try client.subscribe(cmd.subject, cmd.queue, cmd.sid);
    var seen: u64 = 0;
    while (true) {
        const msg = try client.nextMessage() orelse return error.UnexpectedEndOfStream;
        if (!std.mem.eql(u8, msg.subject, cmd.subject)) continue;
        if (msg.reply) |reply_to| {
            try client.publish(reply_to, null, cmd.payload);
        }
        seen += 1;
        if (cmd.count) |limit| {
            if (seen >= limit) {
                try client.unsubscribe(cmd.sid, null);
                return;
            }
        }
    }
}

fn runPing(allocator: std.mem.Allocator, cmd: PingCommand) !void {
    var client = try connectClient(allocator, cmd.server);
    defer client.deinit();
    var i: u64 = 0;
    while (i < cmd.count) : (i += 1) {
        const start = std.time.nanoTimestamp();
        try client.ping();
        const elapsed = std.time.nanoTimestamp() - start;
        std.debug.print("PONG {d} ns\n", .{elapsed});
    }
}

fn connectClient(allocator: std.mem.Allocator, server: []const u8) !client_mod.Client {
    const address = try client_mod.parseServerAddress(server);
    return try client_mod.Client.connect(allocator, address);
}

fn printMessage(prefix: []const u8, msg: client_mod.Message) void {
    if (msg.reply) |reply_to| {
        std.debug.print("{s} on \"{s}\" sid={d} reply=\"{s}\"\n", .{ prefix, msg.subject, msg.sid, reply_to });
    } else {
        std.debug.print("{s} on \"{s}\" sid={d}\n", .{ prefix, msg.subject, msg.sid });
    }
    std.debug.print("{s}\n", .{msg.payload});
}

const StartGate = struct {
    ready: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    go: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

const BenchTotals = struct {
    messages: u64 = 0,
    bytes: u64 = 0,
};

fn runBench(allocator: std.mem.Allocator, cmd: BenchCommand) !void {
    switch (cmd) {
        .publish => |bench| try runBenchPublish(allocator, bench),
        .subscribe => |bench| try runBenchSubscribe(allocator, bench),
        .request => |bench| try runBenchRequest(allocator, bench),
        .reply => |bench| try runBenchReply(allocator, bench),
        .latency => |bench| try runBenchLatency(allocator, bench),
    }
}

fn runBenchPublish(allocator: std.mem.Allocator, bench: BenchPublish) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    const payload = try allocator.alloc(u8, bench.common.size);
    defer allocator.free(payload);
    @memset(payload, 'x');

    var gate = StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(PubWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .subject = bench.subject,
            .payload = payload,
            .messages = splitCount(bench.common.msgs, index, client_count),
            .sleep_ns = bench.common.sleep_ns,
            .multi_subject = bench.common.multi_subject,
            .multi_subject_max = bench.common.multi_subject_max,
            .gate = &gate,
            .no_progress = bench.common.no_progress,
            .messages_done = 0,
            .bytes_done = 0,
        };
        threads[index] = try std.Thread.spawn(.{}, benchPublishWorker, .{&contexts[index]});
    }

    waitForGate(&gate, @intCast(client_count));
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = BenchTotals{};
    for (threads) |thread| thread.join();
    for (contexts) |ctx| {
        totals.messages += ctx.messages_done;
        totals.bytes += ctx.bytes_done;
    }
    const elapsed = std.time.nanoTimestamp() - start;
    printBenchTotals("pub", totals, elapsed);
}

const PubWorker = struct {
    allocator: std.mem.Allocator,
    server: []const u8,
    subject: []const u8,
    payload: []const u8,
    messages: u64,
    sleep_ns: u64,
    multi_subject: bool,
    multi_subject_max: u64,
    gate: *StartGate,
    no_progress: bool,
    messages_done: u64,
    bytes_done: u64,
};

fn benchPublishWorker(ctx: *PubWorker) void {
    var client = connectClient(ctx.allocator, ctx.server) catch |err| fatal(err);
    defer client.deinit();
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var subject_buf: [256]u8 = undefined;
    var i: u64 = 0;
    while (i < ctx.messages) : (i += 1) {
        const subject = if (ctx.multi_subject) blk: {
            const suffix = i % ctx.multi_subject_max;
            break :blk std.fmt.bufPrint(&subject_buf, "{s}.{d}", .{ ctx.subject, suffix }) catch ctx.subject;
        } else ctx.subject;
        client.publish(subject, null, ctx.payload) catch |err| fatal(err);
        if (ctx.sleep_ns > 0) std.Thread.sleep(ctx.sleep_ns);
    }
    ctx.messages_done = ctx.messages;
    ctx.bytes_done = ctx.messages * ctx.payload.len;
}

fn runBenchSubscribe(allocator: std.mem.Allocator, bench: BenchSubscribe) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    var gate = StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(SubWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .subject = bench.subject,
            .messages = bench.common.msgs,
            .sleep_ns = bench.common.sleep_ns,
            .gate = &gate,
            .no_progress = bench.common.no_progress,
            .messages_done = 0,
            .bytes_done = 0,
        };
        threads[index] = try std.Thread.spawn(.{}, benchSubscribeWorker, .{&contexts[index]});
    }

    waitForGate(&gate, @intCast(client_count));
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = BenchTotals{};
    for (threads) |thread| thread.join();
    for (contexts) |ctx| {
        totals.messages += ctx.messages_done;
        totals.bytes += ctx.bytes_done;
    }
    const elapsed = std.time.nanoTimestamp() - start;
    printBenchTotals("sub", totals, elapsed);
}

const SubWorker = struct {
    allocator: std.mem.Allocator,
    server: []const u8,
    subject: []const u8,
    messages: u64,
    sleep_ns: u64,
    gate: *StartGate,
    no_progress: bool,
    messages_done: u64,
    bytes_done: u64,
};

fn benchSubscribeWorker(ctx: *SubWorker) void {
    var client = connectClient(ctx.allocator, ctx.server) catch |err| fatal(err);
    defer client.deinit();
    tryOrFatal(client.subscribe(ctx.subject, null, 1));
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var seen: u64 = 0;
    while (seen < ctx.messages) {
        const maybe_msg = client.nextMessage() catch |err| fatal(err);
        const msg = maybe_msg orelse fatal(error.UnexpectedEndOfStream);
        if (!std.mem.eql(u8, msg.subject, ctx.subject)) continue;
        seen += 1;
        ctx.bytes_done += msg.payload.len;
        if (ctx.sleep_ns > 0) std.Thread.sleep(ctx.sleep_ns);
    }
    ctx.messages_done = seen;
}

fn runBenchRequest(allocator: std.mem.Allocator, bench: BenchRequest) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    const payload = try allocator.alloc(u8, bench.common.size);
    defer allocator.free(payload);
    @memset(payload, 'q');

    var gate = StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(RequestWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .subject = bench.subject,
            .payload = payload,
            .messages = splitCount(bench.common.msgs, index, client_count),
            .sleep_ns = bench.common.sleep_ns,
            .gate = &gate,
            .messages_done = 0,
            .bytes_done = 0,
        };
        threads[index] = try std.Thread.spawn(.{}, benchRequestWorker, .{&contexts[index]});
    }

    waitForGate(&gate, @intCast(client_count));
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = BenchTotals{};
    for (threads) |thread| thread.join();
    for (contexts) |ctx| {
        totals.messages += ctx.messages_done;
        totals.bytes += ctx.bytes_done;
    }
    const elapsed = std.time.nanoTimestamp() - start;
    printBenchTotals("request", totals, elapsed);
}

const RequestWorker = struct {
    allocator: std.mem.Allocator,
    server: []const u8,
    subject: []const u8,
    payload: []const u8,
    messages: u64,
    sleep_ns: u64,
    gate: *StartGate,
    messages_done: u64,
    bytes_done: u64,
};

fn benchRequestWorker(ctx: *RequestWorker) void {
    var client = connectClient(ctx.allocator, ctx.server) catch |err| fatal(err);
    defer client.deinit();
    const inbox = client_mod.makeInbox(ctx.allocator, "bench") catch |err| fatal(err);
    defer ctx.allocator.free(inbox);
    tryOrFatal(client.subscribe(inbox, null, 1));
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var i: u64 = 0;
    while (i < ctx.messages) : (i += 1) {
        tryOrFatal(client.publish(ctx.subject, inbox, ctx.payload));
        _ = client.waitForMessage(inbox) catch |err| fatal(err);
        if (ctx.sleep_ns > 0) std.Thread.sleep(ctx.sleep_ns);
    }
    ctx.messages_done = ctx.messages;
    ctx.bytes_done = ctx.messages * ctx.payload.len;
}

fn runBenchReply(allocator: std.mem.Allocator, bench: BenchReply) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    const payload = try allocator.alloc(u8, bench.common.size);
    defer allocator.free(payload);
    @memset(payload, 'r');

    var gate = StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(ReplyWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .subject = bench.subject,
            .queue = bench.queue,
            .payload = payload,
            .messages = splitCount(bench.common.msgs, index, client_count),
            .sleep_ns = bench.common.sleep_ns,
            .gate = &gate,
            .messages_done = 0,
            .bytes_done = 0,
        };
        threads[index] = try std.Thread.spawn(.{}, benchReplyWorker, .{&contexts[index]});
    }

    waitForGate(&gate, @intCast(client_count));
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = BenchTotals{};
    for (threads) |thread| thread.join();
    for (contexts) |ctx| {
        totals.messages += ctx.messages_done;
        totals.bytes += ctx.bytes_done;
    }
    const elapsed = std.time.nanoTimestamp() - start;
    printBenchTotals("reply", totals, elapsed);
}

const ReplyWorker = struct {
    allocator: std.mem.Allocator,
    server: []const u8,
    subject: []const u8,
    queue: []const u8,
    payload: []const u8,
    messages: u64,
    sleep_ns: u64,
    gate: *StartGate,
    messages_done: u64,
    bytes_done: u64,
};

fn benchReplyWorker(ctx: *ReplyWorker) void {
    var client = connectClient(ctx.allocator, ctx.server) catch |err| fatal(err);
    defer client.deinit();
    tryOrFatal(client.subscribe(ctx.subject, ctx.queue, 1));
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var seen: u64 = 0;
    while (seen < ctx.messages) {
        const maybe_msg = client.nextMessage() catch |err| fatal(err);
        const msg = maybe_msg orelse fatal(error.UnexpectedEndOfStream);
        if (!std.mem.eql(u8, msg.subject, ctx.subject)) continue;
        if (msg.reply) |reply_to| {
            tryOrFatal(client.publish(reply_to, null, ctx.payload));
        }
        seen += 1;
        if (ctx.sleep_ns > 0) std.Thread.sleep(ctx.sleep_ns);
    }
    ctx.messages_done = seen;
    ctx.bytes_done = seen * ctx.payload.len;
}

fn runBenchLatency(allocator: std.mem.Allocator, bench: BenchLatency) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    var gate = StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(LatencyWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .messages = splitCount(bench.common.msgs, index, client_count),
            .sleep_ns = bench.common.sleep_ns,
            .gate = &gate,
            .total_ns = 0,
            .messages_done = 0,
        };
        threads[index] = try std.Thread.spawn(.{}, benchLatencyWorker, .{&contexts[index]});
    }

    waitForGate(&gate, @intCast(client_count));
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = BenchTotals{};
    var total_ns: u128 = 0;
    for (threads) |thread| thread.join();
    for (contexts) |ctx| {
        totals.messages += ctx.messages_done;
        total_ns += ctx.total_ns;
    }
    const elapsed = std.time.nanoTimestamp() - start;
    printBenchTotals("latency", totals, elapsed);
    if (totals.messages > 0) {
        const avg = @as(f64, @floatFromInt(total_ns)) / @as(f64, @floatFromInt(totals.messages));
        std.debug.print("average ping RTT: {d:.3} us\n", .{avg / 1000.0});
    }
}

const LatencyWorker = struct {
    allocator: std.mem.Allocator,
    server: []const u8,
    messages: u64,
    sleep_ns: u64,
    gate: *StartGate,
    total_ns: u128,
    messages_done: u64,
};

fn benchLatencyWorker(ctx: *LatencyWorker) void {
    var client = connectClient(ctx.allocator, ctx.server) catch |err| fatal(err);
    defer client.deinit();
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var seen: u64 = 0;
    var total_ns: u128 = 0;
    while (seen < ctx.messages) : (seen += 1) {
        const start = std.time.nanoTimestamp();
        tryOrFatal(client.ping());
        total_ns += @as(u128, @intCast(std.time.nanoTimestamp() - start));
        if (ctx.sleep_ns > 0) std.Thread.sleep(ctx.sleep_ns);
    }
    ctx.messages_done = seen;
    ctx.total_ns = total_ns;
}

fn waitForGate(gate: *StartGate, clients: u32) void {
    while (gate.ready.load(.acquire) < clients) {
        std.Thread.sleep(std.time.ns_per_ms);
    }
}

fn signalReady(gate: *StartGate) void {
    _ = gate.ready.fetchAdd(1, .release);
}

fn waitForGo(gate: *StartGate) void {
    while (!gate.go.load(.acquire)) {
        std.Thread.sleep(std.time.ns_per_ms);
    }
}

fn splitCount(total: u64, index: usize, clients: usize) u64 {
    const base = total / clients;
    const remainder = total % clients;
    return base + @as(u64, if (index < remainder) 1 else 0);
}

fn printBenchTotals(label: []const u8, totals: BenchTotals, elapsed_ns: i128) void {
    const seconds = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    const msg_rate = if (seconds > 0) @as(f64, @floatFromInt(totals.messages)) / seconds else 0;
    const byte_rate = if (seconds > 0) @as(f64, @floatFromInt(totals.bytes)) / seconds else 0;
    const avg_us = if (totals.messages > 0) @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(totals.messages)) / 1000.0 else 0;
    std.debug.print("{s} stats: {d:.3} msgs/sec ~ {d:.3} MiB/sec ~ {d:.3} us/msg\n", .{
        label,
        msg_rate,
        byte_rate / 1024.0 / 1024.0,
        avg_us,
    });
}

fn tryOrFatal(result: anytype) void {
    result catch |err| fatal(err);
}

fn fatal(err: anyerror) noreturn {
    std.debug.print("fatal: {s}\n", .{@errorName(err)});
    std.process.exit(1);
}

test "parse publish command" {
    const argv = [_][]const u8{ "zigbee-cli", "pub", "--server", "127.0.0.1:4222", "foo", "hello", "world" };
    const cmd = try parseCommand(std.testing.allocator, argv[0..]);
    defer std.testing.allocator.free(cmd.publish.payload);
    try std.testing.expect(cmd == .publish);
    try std.testing.expectEqualStrings("foo", cmd.publish.subject);
    try std.testing.expectEqualStrings("hello world", cmd.publish.payload);
    try std.testing.expectEqualStrings("127.0.0.1:4222", cmd.publish.server);
}

test "parse bench request command" {
    const argv = [_][]const u8{ "zigbee-cli", "bench", "request", "--clients", "4", "--msgs", "1000", "foo" };
    const cmd = try parseCommand(std.testing.allocator, argv[0..]);
    try std.testing.expect(cmd == .bench);
    try std.testing.expect(cmd.bench == .request);
    try std.testing.expectEqual(@as(usize, 4), cmd.bench.request.common.clients);
    try std.testing.expectEqual(@as(u64, 1000), cmd.bench.request.common.msgs);
    try std.testing.expectEqualStrings("foo", cmd.bench.request.subject);
}
