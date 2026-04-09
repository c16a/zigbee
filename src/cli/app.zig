// SPDX-License-Identifier: MIT
const std = @import("std");
const client_mod = @import("common_client");
const protocol_mod = client_mod.protocol_mod;
const cli_types = @import("cli_types");
const cli_parse = @import("cli_parse");
const cli_bench = @import("cli_bench");

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}).init;
    defer _ = gpa.deinit();

    var thread_safe = std.heap.ThreadSafeAllocator{
        .child_allocator = gpa.allocator(),
    };
    const allocator = thread_safe.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const parsed = cli_parse.parseCommand(allocator, args) catch |err| switch (err) {
        error.HelpRequested => return,
        error.MissingCommand, error.UnknownCommand => {
            cli_parse.printUsage();
            return err;
        },
        else => return err,
    };
    defer deinitParsedCommand(allocator, parsed.command);

    const auth = if (parsed.zkey_seed_path) |path| try client_mod.loadAuthFromSeedFile(allocator, path) else null;
    try runCommand(allocator, parsed.command, auth);
}

fn runCommand(allocator: std.mem.Allocator, command: cli_types.Command, auth: ?client_mod.Auth) !void {
    switch (command) {
        .client => |cmd| switch (cmd) {
            .publish => |payload| try runPublish(allocator, payload, auth),
            .subscribe => |payload| try runSubscribe(allocator, payload, auth),
            .unsubscribe => |payload| try runUnsubscribe(allocator, payload, auth),
            .request => |payload| try runRequest(allocator, payload, auth),
            .reply => |payload| try runReply(allocator, payload, auth),
            .ping => |payload| try runPing(allocator, payload, auth),
            .session => |payload| try runSession(allocator, payload, auth),
        },
        .bench => |cmd| try cli_bench.runBench(allocator, cmd, auth),
    }
}

fn deinitParsedCommand(allocator: std.mem.Allocator, command: cli_types.Command) void {
    switch (command) {
        .client => |cmd| switch (cmd) {
            .publish => |payload| allocator.free(payload.payload),
            .request => |payload| allocator.free(payload.payload),
            .reply => |payload| allocator.free(payload.payload),
            else => {},
        },
        .bench => {},
    }
}

fn runPublish(allocator: std.mem.Allocator, cmd: protocol_mod.ClientPublish, auth: ?client_mod.Auth) !void {
    var client = try connectClient(allocator, cmd.server, auth);
    defer client.deinit();
    try client.publish(cmd.subject, cmd.reply, cmd.payload);
    std.debug.print("published {d} bytes to \"{s}\"\n", .{ cmd.payload.len, cmd.subject });
}

fn runSubscribe(allocator: std.mem.Allocator, cmd: protocol_mod.ClientSubscribe, auth: ?client_mod.Auth) !void {
    var client = try connectClient(allocator, cmd.server, auth);
    defer client.deinit();
    try client.subscribe(cmd.subject, cmd.queue, cmd.sid);
    var seen: usize = 0;
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

fn runUnsubscribe(allocator: std.mem.Allocator, cmd: protocol_mod.ClientUnsubscribe, auth: ?client_mod.Auth) !void {
    var client = try connectClient(allocator, cmd.server, auth);
    defer client.deinit();
    try client.unsubscribe(cmd.sid, cmd.max);
    std.debug.print("sent UNSUB for sid {d}\n", .{cmd.sid});
}

fn runRequest(allocator: std.mem.Allocator, cmd: protocol_mod.ClientRequest, auth: ?client_mod.Auth) !void {
    var client = try connectClient(allocator, cmd.server, auth);
    defer client.deinit();
    const inbox = try client_mod.makeInbox(allocator, "request");
    defer allocator.free(inbox);
    try client.subscribe(inbox, null, 1);
    try client.publish(cmd.subject, inbox, cmd.payload);
    const msg = try client.waitForMessage(inbox);
    std.debug.print("{s}\n", .{msg.payload});
}

fn runReply(allocator: std.mem.Allocator, cmd: protocol_mod.ClientReply, auth: ?client_mod.Auth) !void {
    var client = try connectClient(allocator, cmd.server, auth);
    defer client.deinit();
    try client.subscribe(cmd.subject, cmd.queue, cmd.sid);
    var seen: usize = 0;
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

fn runPing(allocator: std.mem.Allocator, cmd: protocol_mod.ClientPing, auth: ?client_mod.Auth) !void {
    var client = try connectClient(allocator, cmd.server, auth);
    defer client.deinit();
    var i: usize = 0;
    while (i < cmd.count) : (i += 1) {
        try client.ping();
        std.debug.print("PONG\n", .{});
    }
}

fn runSession(allocator: std.mem.Allocator, cmd: protocol_mod.ClientSession, auth: ?client_mod.Auth) !void {
    const address = try client_mod.parseServerAddress(cmd.server);
    try client_mod.runSession(allocator, address, auth);
}

fn connectClient(allocator: std.mem.Allocator, server: []const u8, auth: ?client_mod.Auth) !client_mod.Client {
    const address = try client_mod.parseServerAddress(server);
    return try client_mod.Client.connect(allocator, address, auth);
}

fn printMessage(prefix: []const u8, msg: client_mod.Message) void {
    if (msg.reply) |reply_to| {
        std.debug.print("{s} on \"{s}\" sid={d} reply=\"{s}\"\n", .{ prefix, msg.subject, msg.sid, reply_to });
    } else {
        std.debug.print("{s} on \"{s}\" sid={d}\n", .{ prefix, msg.subject, msg.sid });
    }
    std.debug.print("{s}\n", .{msg.payload});
}
