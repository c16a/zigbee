// SPDX-License-Identifier: MIT
const std = @import("std");
const protocol = @import("protocol.zig");

pub const Session = struct {
    id: u64,
    stream: std.net.Stream,
    write_mutex: std.Thread.Mutex = .{},
    close_mutex: std.Thread.Mutex = .{},
    closed: bool = false,

    pub fn close(self: *Session) void {
        self.close_mutex.lock();
        defer self.close_mutex.unlock();
        if (self.closed) return;
        self.closed = true;
        self.stream.close();
    }
};

pub const Delivery = struct {
    client_id: u64,
    sid: u64,
};

const Subscription = struct {
    client_id: u64,
    sid: u64,
    subject: []u8,
    queue: ?[]u8,
    remaining: ?u64 = null,
};

const QueueGroup = struct {
    state_index: usize,
    members: std.ArrayListUnmanaged(usize) = .{},
};

const QueueState = struct {
    subject: []u8,
    queue: []u8,
    cursor: usize = 0,
};

pub const Broker = struct {
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex = .{},
    subscriptions: std.ArrayListUnmanaged(Subscription) = .{},
    queue_states: std.ArrayListUnmanaged(QueueState) = .{},

    pub fn init(allocator: std.mem.Allocator) Broker {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Broker) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.subscriptions.items) |sub| {
            self.allocator.free(sub.subject);
            if (sub.queue) |queue| self.allocator.free(queue);
        }
        for (self.queue_states.items) |state| {
            self.allocator.free(state.subject);
            self.allocator.free(state.queue);
        }
        self.subscriptions.deinit(self.allocator);
        self.queue_states.deinit(self.allocator);
    }

    pub fn subscribe(self: *Broker, client_id: u64, sid: u64, subject: []const u8, queue: ?[]const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        _ = self.removeBySidLocked(client_id, sid);

        const subscription: Subscription = .{
            .client_id = client_id,
            .sid = sid,
            .subject = try self.allocator.dupe(u8, subject),
            .queue = if (queue) |q| try self.allocator.dupe(u8, q) else null,
        };
        try self.subscriptions.append(self.allocator, subscription);
    }

    pub fn unsubscribe(self: *Broker, client_id: u64, sid: u64, max: ?u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var index: usize = 0;
        while (index < self.subscriptions.items.len) : (index += 1) {
            if (self.subscriptions.items[index].client_id != client_id or self.subscriptions.items[index].sid != sid) {
                continue;
            }

            if (max) |remaining| {
                if (remaining == 0) {
                    self.removeAtLocked(index);
                } else {
                    self.subscriptions.items[index].remaining = remaining;
                }
            } else {
                self.removeAtLocked(index);
            }
            return;
        }
    }

    pub fn unsubscribeSession(self: *Broker, client_id: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var index: usize = 0;
        while (index < self.subscriptions.items.len) {
            if (self.subscriptions.items[index].client_id == client_id) {
                self.removeAtLocked(index);
                continue;
            }
            index += 1;
        }
    }

    pub fn publish(self: *Broker, subject: []const u8) ![]Delivery {
        self.mutex.lock();
        defer self.mutex.unlock();

        var deliveries: std.ArrayList(Delivery) = .empty;
        errdefer deliveries.deinit(self.allocator);

        var auto_unsubs: std.ArrayList(u64) = .empty;
        defer auto_unsubs.deinit(self.allocator);

        var groups: std.ArrayList(QueueGroup) = .empty;
        defer {
            for (groups.items) |*group| {
                group.members.deinit(self.allocator);
            }
            groups.deinit(self.allocator);
        }

        for (self.subscriptions.items, 0..) |*sub, index| {
            if (!protocol.matchesSubject(sub.subject, subject)) continue;

            if (sub.queue == null) {
                try deliveries.append(self.allocator, .{ .client_id = sub.client_id, .sid = sub.sid });
                if (sub.remaining) |*remaining| {
                    if (remaining.* > 0) remaining.* -= 1;
                    if (remaining.* == 0) try auto_unsubs.append(self.allocator, sub.sid);
                }
                continue;
            }

            const queue = sub.queue.?;
            const state_index = try self.queueStateIndex(sub.subject, queue);

            var found = false;
            for (groups.items) |*group| {
                if (group.state_index == state_index) {
                    try group.members.append(self.allocator, index);
                    found = true;
                    break;
                }
            }

            if (!found) {
                var group: QueueGroup = .{ .state_index = state_index };
                try group.members.append(self.allocator, index);
                try groups.append(self.allocator, group);
            }
        }

        for (groups.items) |*group| {
            if (group.members.items.len == 0) continue;
            const state = &self.queue_states.items[group.state_index];
            const chosen_index = group.members.items[state.cursor % group.members.items.len];
            state.cursor +%= 1;
            const sub = &self.subscriptions.items[chosen_index];
            try deliveries.append(self.allocator, .{ .client_id = sub.client_id, .sid = sub.sid });
            if (sub.remaining) |*remaining| {
                if (remaining.* > 0) remaining.* -= 1;
                if (remaining.* == 0) try auto_unsubs.append(self.allocator, sub.sid);
            }
        }

        for (auto_unsubs.items) |sid| {
            _ = self.removeBySidLockedAnyClient(sid);
        }

        return try deliveries.toOwnedSlice(self.allocator);
    }

    fn queueStateIndex(self: *Broker, subject: []const u8, queue: []const u8) !usize {
        for (self.queue_states.items, 0..) |state, index| {
            if (std.mem.eql(u8, state.subject, subject) and std.mem.eql(u8, state.queue, queue)) {
                return index;
            }
        }

        const state: QueueState = .{
            .subject = try self.allocator.dupe(u8, subject),
            .queue = try self.allocator.dupe(u8, queue),
        };
        try self.queue_states.append(self.allocator, state);
        return self.queue_states.items.len - 1;
    }

    fn removeBySidLocked(self: *Broker, client_id: u64, sid: u64) bool {
        var index: usize = 0;
        while (index < self.subscriptions.items.len) : (index += 1) {
            const sub = self.subscriptions.items[index];
            if (sub.client_id == client_id and sub.sid == sid) {
                self.removeAtLocked(index);
                return true;
            }
        }
        return false;
    }

    fn removeBySidLockedAnyClient(self: *Broker, sid: u64) bool {
        var index: usize = 0;
        while (index < self.subscriptions.items.len) : (index += 1) {
            const sub = self.subscriptions.items[index];
            if (sub.sid == sid) {
                self.removeAtLocked(index);
                return true;
            }
        }
        return false;
    }

    fn removeAtLocked(self: *Broker, index: usize) void {
        const removed = self.subscriptions.swapRemove(index);
        self.allocator.free(removed.subject);
        if (removed.queue) |queue| self.allocator.free(queue);
    }
};

test "queue groups choose one member" {
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();

    try broker.subscribe(1, 1, "foo", "q");
    try broker.subscribe(2, 2, "foo", "q");

    const first = try broker.publish("foo");
    defer std.testing.allocator.free(first);
    try std.testing.expectEqual(@as(usize, 1), first.len);
    try std.testing.expectEqual(@as(u64, 1), first[0].sid);

    const second = try broker.publish("foo");
    defer std.testing.allocator.free(second);
    try std.testing.expectEqual(@as(usize, 1), second.len);
    try std.testing.expectEqual(@as(u64, 2), second[0].sid);
}
