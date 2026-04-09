// SPDX-License-Identifier: MIT
const std = @import("std");
const client_mod = @import("common_client");
const types = @import("cli_types");

pub fn runBench(allocator: std.mem.Allocator, cmd: types.BenchCommand, auth: ?client_mod.Auth) !void {
    switch (cmd) {
        .publish => |bench| try runBenchPublish(allocator, bench, auth),
        .subscribe => |bench| try runBenchSubscribe(allocator, bench, auth),
        .request => |bench| try runBenchRequest(allocator, bench, auth),
        .reply => |bench| try runBenchReply(allocator, bench, auth),
        .latency => |bench| try runBenchLatency(allocator, bench, auth),
    }
}

fn runBenchPublish(allocator: std.mem.Allocator, bench: types.BenchPublish, auth: ?client_mod.Auth) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    const payload = try allocator.alloc(u8, bench.common.size);
    defer allocator.free(payload);
    @memset(payload, 'x');

    var gate = types.StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(PubWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .auth = auth,
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

    waitForGate(&gate, client_count);
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = types.BenchTotals{};
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
    auth: ?client_mod.Auth,
    subject: []const u8,
    payload: []const u8,
    messages: usize,
    sleep_ns: u64,
    multi_subject: bool,
    multi_subject_max: usize,
    gate: *types.StartGate,
    no_progress: bool,
    messages_done: usize,
    bytes_done: usize,
};

fn benchPublishWorker(ctx: *PubWorker) void {
    var client = connectClient(ctx.allocator, ctx.server, ctx.auth) catch |err| fatal(err);
    defer client.deinit();
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var subject_buf: [256]u8 = undefined;
    var i: usize = 0;
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

fn runBenchSubscribe(allocator: std.mem.Allocator, bench: types.BenchSubscribe, auth: ?client_mod.Auth) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    var gate = types.StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(SubWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .auth = auth,
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

    waitForGate(&gate, client_count);
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = types.BenchTotals{};
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
    auth: ?client_mod.Auth,
    subject: []const u8,
    messages: usize,
    sleep_ns: u64,
    gate: *types.StartGate,
    no_progress: bool,
    messages_done: usize,
    bytes_done: usize,
};

fn benchSubscribeWorker(ctx: *SubWorker) void {
    var client = connectClient(ctx.allocator, ctx.server, ctx.auth) catch |err| fatal(err);
    defer client.deinit();
    tryOrFatal(client.subscribe(ctx.subject, null, 1));
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var seen: usize = 0;
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

fn runBenchRequest(allocator: std.mem.Allocator, bench: types.BenchRequest, auth: ?client_mod.Auth) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    const payload = try allocator.alloc(u8, bench.common.size);
    defer allocator.free(payload);
    @memset(payload, 'q');

    var gate = types.StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(RequestWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .auth = auth,
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

    waitForGate(&gate, client_count);
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = types.BenchTotals{};
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
    auth: ?client_mod.Auth,
    subject: []const u8,
    payload: []const u8,
    messages: usize,
    sleep_ns: u64,
    gate: *types.StartGate,
    messages_done: usize,
    bytes_done: usize,
};

fn benchRequestWorker(ctx: *RequestWorker) void {
    var client = connectClient(ctx.allocator, ctx.server, ctx.auth) catch |err| fatal(err);
    defer client.deinit();
    const inbox = client_mod.makeInbox(ctx.allocator, "bench") catch |err| fatal(err);
    defer ctx.allocator.free(inbox);
    tryOrFatal(client.subscribe(inbox, null, 1));
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var i: usize = 0;
    while (i < ctx.messages) : (i += 1) {
        tryOrFatal(client.publish(ctx.subject, inbox, ctx.payload));
        _ = client.waitForMessage(inbox) catch |err| fatal(err);
        if (ctx.sleep_ns > 0) std.Thread.sleep(ctx.sleep_ns);
    }
    ctx.messages_done = ctx.messages;
    ctx.bytes_done = ctx.messages * ctx.payload.len;
}

fn runBenchReply(allocator: std.mem.Allocator, bench: types.BenchReply, auth: ?client_mod.Auth) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    const payload = try allocator.alloc(u8, bench.common.size);
    defer allocator.free(payload);
    @memset(payload, 'r');

    var gate = types.StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(ReplyWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .auth = auth,
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

    waitForGate(&gate, client_count);
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = types.BenchTotals{};
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
    auth: ?client_mod.Auth,
    subject: []const u8,
    queue: []const u8,
    payload: []const u8,
    messages: usize,
    sleep_ns: u64,
    gate: *types.StartGate,
    messages_done: usize,
    bytes_done: usize,
};

fn benchReplyWorker(ctx: *ReplyWorker) void {
    var client = connectClient(ctx.allocator, ctx.server, ctx.auth) catch |err| fatal(err);
    defer client.deinit();
    tryOrFatal(client.subscribe(ctx.subject, ctx.queue, 1));
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var seen: usize = 0;
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

fn runBenchLatency(allocator: std.mem.Allocator, bench: types.BenchLatency, auth: ?client_mod.Auth) !void {
    const client_count = if (bench.common.clients > 0) bench.common.clients else 1;
    var gate = types.StartGate{};
    var threads = try allocator.alloc(std.Thread, client_count);
    defer allocator.free(threads);
    var contexts = try allocator.alloc(LatencyWorker, client_count);
    defer allocator.free(contexts);

    var index: usize = 0;
    while (index < client_count) : (index += 1) {
        contexts[index] = .{
            .allocator = allocator,
            .server = bench.common.server,
            .auth = auth,
            .messages = splitCount(bench.common.msgs, index, client_count),
            .sleep_ns = bench.common.sleep_ns,
            .gate = &gate,
            .messages_done = 0,
        };
        threads[index] = try std.Thread.spawn(.{}, benchLatencyWorker, .{&contexts[index]});
    }

    waitForGate(&gate, @intCast(client_count));
    const start = std.time.nanoTimestamp();
    gate.go.store(true, .release);

    var totals = types.BenchTotals{};
    for (threads) |thread| thread.join();
    for (contexts) |ctx| {
        totals.messages += ctx.messages_done;
    }
    const elapsed = std.time.nanoTimestamp() - start;
    printBenchTotals("latency", totals, elapsed);
}

const LatencyWorker = struct {
    allocator: std.mem.Allocator,
    server: []const u8,
    auth: ?client_mod.Auth,
    messages: usize,
    sleep_ns: u64,
    gate: *types.StartGate,
    messages_done: usize,
};

fn benchLatencyWorker(ctx: *LatencyWorker) void {
    var client = connectClient(ctx.allocator, ctx.server, ctx.auth) catch |err| fatal(err);
    defer client.deinit();
    signalReady(ctx.gate);
    waitForGo(ctx.gate);

    var seen: usize = 0;
    while (seen < ctx.messages) : (seen += 1) {
        tryOrFatal(client.ping());
        if (ctx.sleep_ns > 0) std.Thread.sleep(ctx.sleep_ns);
    }
    ctx.messages_done = seen;
}

fn connectClient(allocator: std.mem.Allocator, server: []const u8, auth: ?client_mod.Auth) !client_mod.Client {
    const address = try client_mod.parseServerAddress(server);
    return try client_mod.Client.connect(allocator, address, auth);
}

fn waitForGate(gate: *types.StartGate, clients: usize) void {
    while (gate.ready.load(.acquire) < clients) {
        std.Thread.sleep(std.time.ns_per_ms);
    }
}

fn signalReady(gate: *types.StartGate) void {
    _ = gate.ready.fetchAdd(1, .release);
}

fn waitForGo(gate: *types.StartGate) void {
    while (!gate.go.load(.acquire)) {
        std.Thread.sleep(std.time.ns_per_ms);
    }
}

fn splitCount(total: usize, index: usize, clients: usize) usize {
    const base = total / clients;
    const remainder = total % clients;
    return base + @as(usize, if (index < remainder) 1 else 0);
}

fn printBenchTotals(label: []const u8, totals: types.BenchTotals, elapsed_ns: i128) void {
    const seconds = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    const msg_rate = if (seconds > 0) @as(f64, @floatFromInt(totals.messages)) / seconds else 0;
    const byte_rate = if (seconds > 0) @as(f64, @floatFromInt(totals.bytes)) / seconds else 0;
    std.debug.print("{s} stats: {d:.3} msgs/sec ~ {d:.3} MiB/sec\n", .{
        label,
        msg_rate,
        byte_rate / 1024.0 / 1024.0,
    });
}

fn tryOrFatal(result: anytype) void {
    result catch |err| fatal(err);
}

fn fatal(err: anyerror) noreturn {
    std.debug.print("fatal: {s}\n", .{@errorName(err)});
    std.process.exit(1);
}
