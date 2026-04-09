// SPDX-License-Identifier: MIT
const std = @import("std");
const posix = std.posix;
const config_mod = @import("config.zig");
const common_mod = @import("common_client");
const crypto_mod = common_mod.crypto_mod;
const protocol = common_mod.protocol_mod;

pub const Delivery = struct {
    session_id: u64,
    sid: u64,
};

const Subscription = struct {
    session_id: u64,
    sid: u64,
    subject: []u8,
    queue: ?[]u8,
    remaining: ?u64 = null,
};

const QueueCursor = struct {
    key: u64,
    cursor: usize,
};

const PeerState = struct {
    peer_id: u64,
    address: std.net.Address,
    last_seen_ns: i128 = 0,
    subscriptions: std.ArrayListUnmanaged(Subscription) = .{},
    queue_cursors: std.ArrayListUnmanaged(QueueCursor) = .{},
};

const GroupEntry = struct {
    subject: []const u8,
    queue: []const u8,
    eligible_peer_ids: std.ArrayListUnmanaged(u64) = .{},
};

const Route = struct {
    local: std.ArrayListUnmanaged(Delivery) = .{},
    remote_peer_ids: std.ArrayListUnmanaged(u64) = .{},
};

const PacketKey = struct {
    origin_id: u64,
    seq: u64,
};

const PendingPacket = struct {
    dest: std.net.Address,
    key: PacketKey,
    bytes: []u8,
    next_retry_ns: i128,
    attempts: u32 = 0,
};

pub const Cluster = struct {
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex = .{},
    socket_fd: posix.socket_t,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    thread: ?std.Thread = null,
    next_seq: u64 = 1,
    self_id: u64,
    seed_addresses: []std.net.Address,
    local_peer: PeerState,
    remote_peers: std.ArrayListUnmanaged(PeerState) = .{},
    seen_packets: std.ArrayListUnmanaged(PacketKey) = .{},
    pending_packets: std.ArrayListUnmanaged(PendingPacket) = .{},
    deliver_local_ctx: ?*anyopaque,
    deliver_local_fn: ?*const fn (*anyopaque, Delivery, []const u8, ?[]const u8, []const u8) void,
    last_heartbeat_ns: i128 = 0,
    heartbeat_interval_ns: i128,

    pub fn init(
        allocator: std.mem.Allocator,
        cfg: config_mod.Cluster,
        deliver_local_ctx: ?*anyopaque,
        deliver_local_fn: ?*const fn (*anyopaque, Delivery, []const u8, ?[]const u8, []const u8) void,
    ) !Cluster {
        const bind_address_text = cfg.bind_address orelse "0.0.0.0:4333";
        const bind_address = try common_mod.parseServerAddress(bind_address_text);
        const socket_fd = try posix.socket(
            bind_address.any.family,
            posix.SOCK.DGRAM | posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK,
            posix.IPPROTO.UDP,
        );
        errdefer posix.close(socket_fd);
        try posix.bind(socket_fd, &bind_address.any, bind_address.getOsSockLen());

        var seeds = try allocator.alloc(std.net.Address, cfg.peer_addresses.len);
        errdefer allocator.free(seeds);
        for (cfg.peer_addresses, 0..) |peer_address, index| {
            seeds[index] = try common_mod.parseServerAddress(peer_address);
        }

        var self_id = std.crypto.random.int(u64);
        while (self_id == 0) self_id = std.crypto.random.int(u64);
        if (cfg.heartbeat_interval_ms == 0) return error.InvalidHeartbeatInterval;

        const cluster = Cluster{
            .allocator = allocator,
            .socket_fd = socket_fd,
            .self_id = self_id,
            .seed_addresses = seeds,
            .heartbeat_interval_ns = @as(i128, cfg.heartbeat_interval_ms) * std.time.ns_per_ms,
            .local_peer = .{
                .peer_id = self_id,
                .address = bind_address,
            },
            .deliver_local_ctx = deliver_local_ctx,
            .deliver_local_fn = deliver_local_fn,
        };

        return cluster;
    }

    pub fn startThread(self: *Cluster) !void {
        std.debug.assert(self.thread == null);
        self.thread = try std.Thread.spawn(.{}, runThread, .{ self });
        try self.sendHelloToSeeds();
    }

    pub fn deinit(self: *Cluster) void {
        self.stop.store(true, .release);
        posix.close(self.socket_fd);
        if (self.thread) |thread| thread.join();

        self.mutex.lock();
        defer self.mutex.unlock();

        freePeerState(self.allocator, &self.local_peer);
        for (self.remote_peers.items) |*peer| {
            freePeerState(self.allocator, peer);
        }
        for (self.pending_packets.items) |pending| {
            self.allocator.free(pending.bytes);
        }
        self.remote_peers.deinit(self.allocator);
        self.seen_packets.deinit(self.allocator);
        self.pending_packets.deinit(self.allocator);
        self.allocator.free(self.seed_addresses);
    }

    pub fn subscribe(self: *Cluster, session_id: u64, sid: u64, subject: []const u8, queue: ?[]const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        _ = self.removeLocalBySidLocked(session_id, sid);
        try self.local_peer.subscriptions.append(self.allocator, .{
            .session_id = session_id,
            .sid = sid,
            .subject = try self.allocator.dupe(u8, subject),
            .queue = if (queue) |q| try self.allocator.dupe(u8, q) else null,
        });

        const seq = self.nextSeq();
        const packet = try self.makePacketBytes(.{
            .kind = "sub",
            .origin_id = self.self_id,
            .seq = seq,
            .subscription = .{
                .session_id = session_id,
                .sid = sid,
                .subject = subject,
                .queue = queue,
            },
        });
        defer self.allocator.free(packet);
        const recipients = try self.recipientAddressesLocked();
        defer self.allocator.free(recipients);
        try self.queuePacketToRecipientsLocked(packet, .{ .origin_id = self.self_id, .seq = seq }, recipients);
    }

    pub fn unsubscribe(self: *Cluster, session_id: u64, sid: u64, max: ?u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (max) |remaining| {
            if (!self.updateLocalRemainingLocked(session_id, sid, remaining)) return;
        } else {
            _ = self.removeLocalBySidLocked(session_id, sid);
        }

        const seq = self.nextSeq();
        const packet = try self.makePacketBytes(.{
            .kind = "unsub",
            .origin_id = self.self_id,
            .seq = seq,
            .unsubscribe_sid = sid,
            .unsubscribe_max = max,
        });
        defer self.allocator.free(packet);
        const recipients = try self.recipientAddressesLocked();
        defer self.allocator.free(recipients);
        try self.queuePacketToRecipientsLocked(packet, .{ .origin_id = self.self_id, .seq = seq }, recipients);
    }

    pub fn unsubscribeSession(self: *Cluster, session_id: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var index: usize = 0;
        while (index < self.local_peer.subscriptions.items.len) {
            if (self.local_peer.subscriptions.items[index].session_id == session_id) {
                self.removeLocalAtLocked(index);
                continue;
            }
            index += 1;
        }

        try self.broadcastSnapshotLocked();
    }

    pub fn publish(self: *Cluster, subject: []const u8, reply: ?[]const u8, payload: []const u8) ![]Delivery {
        self.mutex.lock();
        defer self.mutex.unlock();

        var route = try self.collectRouteLocked(subject);
        defer route.remote_peer_ids.deinit(self.allocator);

        const payload_b64 = try crypto_mod.encodeBase64(self.allocator, payload);
        defer self.allocator.free(payload_b64);

        const seq = self.nextSeq();
        const packet = try self.makePacketBytes(.{
            .kind = "publish",
            .origin_id = self.self_id,
            .seq = seq,
            .publish = .{
                .subject = subject,
                .reply = reply,
                .payload_b64 = payload_b64,
            },
        });
        defer self.allocator.free(packet);

        const remote_ids = try route.remote_peer_ids.toOwnedSlice(self.allocator);
        defer self.allocator.free(remote_ids);

        const recipients = try self.remoteAddressesLocked(remote_ids);
        defer self.allocator.free(recipients);

        const local = try route.local.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(local);

        try self.queuePacketToRecipientsLocked(packet, .{ .origin_id = self.self_id, .seq = seq }, recipients);
        return local;
    }

    fn runThread(self: *Cluster) void {
        var scratch: [65535]u8 = undefined;

        while (!self.stop.load(.acquire)) {
            while (true) {
                var src_addr: std.net.Address = undefined;
                var src_len: posix.socklen_t = @sizeOf(posix.sockaddr);
                const len = posix.recvfrom(self.socket_fd, scratch[0..], 0, &src_addr.any, &src_len) catch |err| switch (err) {
                    error.WouldBlock => break,
                    else => break,
                };
                self.handlePacket(src_addr, scratch[0..len]) catch {};
            }

            const now = std.time.nanoTimestamp();
            self.mutex.lock();
            self.processRetriesLocked(now);
            self.pruneStalePeers(now);
            if (now - self.last_heartbeat_ns >= self.heartbeat_interval_ns) {
                self.last_heartbeat_ns = now;
                self.sendHeartbeatLocked() catch {};
            }
            self.mutex.unlock();

            std.Thread.sleep(10 * std.time.ns_per_ms);
        }
    }

    fn handlePacket(self: *Cluster, source: std.net.Address, bytes: []const u8) !void {
        const parsed = try std.json.parseFromSlice(protocol.ClusterPacket, self.allocator, bytes, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        const packet = parsed.value;

        self.mutex.lock();
        defer self.mutex.unlock();

        if (std.mem.eql(u8, packet.kind, "ack")) {
            if (packet.ack_origin_id) |origin_id| {
                if (packet.ack_seq) |seq| {
                    _ = self.ackPendingLocked(source, origin_id, seq);
                }
            }
            return;
        }

        const peer = try self.ensureRemotePeerLocked(packet.origin_id, source);
        peer.last_seen_ns = std.time.nanoTimestamp();
        if (packet.port) |port| peer.address.setPort(port);

        if (self.isDuplicateLocked(packet.origin_id, packet.seq)) {
            try self.sendAckLocked(source, packet.origin_id, packet.seq);
            return;
        }
        self.recordSeenLocked(packet.origin_id, packet.seq);

        if (std.mem.eql(u8, packet.kind, "hello")) {
            if (packet.port) |port| peer.address.setPort(port);
            try self.broadcastSnapshotLocked();
        } else if (std.mem.eql(u8, packet.kind, "snapshot")) {
            if (packet.snapshot) |snapshot| {
                try self.mergeSnapshotLocked(packet.origin_id, snapshot);
            }
        } else if (std.mem.eql(u8, packet.kind, "sub")) {
            if (packet.subscription) |sub| {
                try self.mergeRemoteSubscribeLocked(packet.origin_id, source, sub);
            }
        } else if (std.mem.eql(u8, packet.kind, "unsub")) {
            if (packet.unsubscribe_sid) |sid| {
                try self.mergeRemoteUnsubscribeLocked(packet.origin_id, sid, packet.unsubscribe_max);
            }
        } else if (std.mem.eql(u8, packet.kind, "publish")) {
            if (packet.publish) |publish_msg| {
                const payload = try decodeBase64Alloc(self.allocator, publish_msg.payload_b64);
                defer self.allocator.free(payload);

                var local = try self.collectLocalDeliveriesLocked(publish_msg.subject);
                defer local.deinit(self.allocator);

                const local_slice = try local.toOwnedSlice(self.allocator);
                errdefer self.allocator.free(local_slice);
                self.mutex.unlock();
                defer self.mutex.lock();
                self.deliverLocal(local_slice, publish_msg.subject, publish_msg.reply, payload);
                self.allocator.free(local_slice);
            }
        } else if (std.mem.eql(u8, packet.kind, "heartbeat")) {
            // Keepalive only.
        }

        try self.sendAckLocked(source, packet.origin_id, packet.seq);
    }

    fn deliverLocal(self: *Cluster, deliveries: []const Delivery, subject: []const u8, reply: ?[]const u8, payload: []const u8) void {
        const deliver_fn = self.deliver_local_fn orelse return;
        const ctx = self.deliver_local_ctx orelse return;
        for (deliveries) |delivery| {
            deliver_fn(ctx, delivery, subject, reply, payload);
        }
    }

    fn collectRouteLocked(self: *Cluster, subject: []const u8) !Route {
        var route: Route = .{};
        try self.collectLocalDirectLocked(&route, subject);
        try self.collectQueueRouteLocked(&route, subject);
        return route;
    }

    fn collectLocalDeliveriesLocked(self: *Cluster, subject: []const u8) !std.ArrayListUnmanaged(Delivery) {
        var route = try self.collectRouteLocked(subject);
        defer route.remote_peer_ids.deinit(self.allocator);
        return route.local;
    }

    fn collectLocalDirectLocked(self: *Cluster, route: *Route, subject: []const u8) !void {
        var index: usize = 0;
        while (index < self.local_peer.subscriptions.items.len) : (index += 1) {
            const sub = self.local_peer.subscriptions.items[index];
            if (sub.queue != null) continue;
            if (!protocol.matchesSubject(sub.subject, subject)) continue;
            try route.local.append(self.allocator, .{ .session_id = sub.session_id, .sid = sub.sid });
            const current = &self.local_peer.subscriptions.items[index];
            if (current.remaining) |*remaining| {
                if (remaining.* > 0) remaining.* -= 1;
                if (remaining.* == 0) {
                    self.removeLocalAtLocked(index);
                    index -= 1;
                }
            }
        }
    }

    fn collectQueueRouteLocked(self: *Cluster, route: *Route, subject: []const u8) !void {
        var groups = std.ArrayList(GroupEntry).empty;
        defer {
            for (groups.items) |*group| group.eligible_peer_ids.deinit(self.allocator);
            groups.deinit(self.allocator);
        }

        try self.collectGroupsForPeerLocked(&groups, &self.local_peer, subject);
        for (self.remote_peers.items) |*peer| {
            try self.collectGroupsForPeerLocked(&groups, peer, subject);
        }

        for (groups.items) |*group| {
            if (group.eligible_peer_ids.items.len == 0) continue;
            const winner = chooseWinnerPeerId(queueCursorKey(group.subject, group.queue), group.eligible_peer_ids.items);
            if (winner == self.self_id) {
                if (try self.deliverQueueLocallyLocked(route, group.subject, group.queue)) continue;
            } else {
                try appendUniqueU64(self.allocator, &route.remote_peer_ids, winner);
            }
        }
    }

    fn collectGroupsForPeerLocked(self: *Cluster, groups: *std.ArrayList(GroupEntry), peer: *PeerState, subject: []const u8) !void {
        for (peer.subscriptions.items) |sub| {
            if (sub.queue == null) continue;
            if (!protocol.matchesSubject(sub.subject, subject)) continue;
            const key = GroupKey{ .subject = sub.subject, .queue = sub.queue.? };
            const group = try findOrCreateGroup(self.allocator, groups, key);
            try appendUniqueU64(self.allocator, &group.eligible_peer_ids, peer.peer_id);
        }
    }

    fn deliverQueueLocallyLocked(self: *Cluster, route: *Route, subject: []const u8, queue: []const u8) !bool {
        var candidate_count: usize = 0;
        for (self.local_peer.subscriptions.items) |sub| {
            if (sub.queue == null) continue;
            if (!std.mem.eql(u8, sub.queue.?, queue)) continue;
            if (!protocol.matchesSubject(sub.subject, subject)) continue;
            candidate_count += 1;
        }
        if (candidate_count == 0) return false;

        const key = queueCursorKey(subject, queue);
        const cursor = self.queueCursorLocked(key);
        const chosen = cursor % candidate_count;
        self.setQueueCursorLocked(key, cursor + 1);

        var seen: usize = 0;
        for (self.local_peer.subscriptions.items, 0..) |sub, index| {
            if (sub.queue == null) continue;
            if (!std.mem.eql(u8, sub.queue.?, queue)) continue;
            if (!protocol.matchesSubject(sub.subject, subject)) continue;
            if (seen != chosen) {
                seen += 1;
                continue;
            }
            try route.local.append(self.allocator, .{ .session_id = sub.session_id, .sid = sub.sid });
            const current = &self.local_peer.subscriptions.items[index];
            if (current.remaining) |*remaining| {
                if (remaining.* > 0) remaining.* -= 1;
                if (remaining.* == 0) {
                    self.removeLocalAtLocked(index);
                }
            }
            return true;
        }
        return false;
    }

    fn sendHelloToSeeds(self: *Cluster) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const seq = self.nextSeq();
        const packet = try self.makePacketBytes(.{
            .kind = "hello",
            .origin_id = self.self_id,
            .seq = seq,
            .port = self.local_peer.address.getPort(),
        });
        defer self.allocator.free(packet);
        try self.queuePacketToRecipientsLocked(packet, .{ .origin_id = self.self_id, .seq = seq }, self.seed_addresses);
    }

    fn sendHeartbeatLocked(self: *Cluster) !void {
        const seq = self.nextSeq();
        const packet = try self.makePacketBytes(.{
            .kind = "heartbeat",
            .origin_id = self.self_id,
            .seq = seq,
            .port = self.local_peer.address.getPort(),
        });
        defer self.allocator.free(packet);
        const recipients = try self.recipientAddressesLocked();
        defer self.allocator.free(recipients);
        try self.queuePacketToRecipientsLocked(packet, .{ .origin_id = self.self_id, .seq = seq }, recipients);
    }

    fn broadcastSnapshot(self: *Cluster) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.broadcastSnapshotLocked();
    }

    fn broadcastSnapshotLocked(self: *Cluster) !void {
        const seq = self.nextSeq();
        const packet = try self.snapshotPacketBytesLocked(seq);
        defer self.allocator.free(packet);
        const recipients = try self.recipientAddressesLocked();
        defer self.allocator.free(recipients);
        try self.queuePacketToRecipientsLocked(packet, .{ .origin_id = self.self_id, .seq = seq }, recipients);
    }

    fn sendSnapshotToPeerLocked(self: *Cluster, dest: std.net.Address) !void {
        const seq = self.nextSeq();
        const packet = try self.snapshotPacketBytesLocked(seq);
        defer self.allocator.free(packet);
        try self.queuePacketToRecipientsLocked(packet, .{ .origin_id = self.self_id, .seq = seq }, &[_]std.net.Address{dest});
    }

    fn snapshotPacketBytesLocked(self: *Cluster, seq: u64) ![]u8 {
        var peers = std.ArrayList(protocol.ClusterPeerSnapshot).empty;
        defer peers.deinit(self.allocator);

        try self.appendPeerSnapshotLocked(&peers, &self.local_peer, false);
        for (self.remote_peers.items) |*peer| {
            try self.appendPeerSnapshotLocked(&peers, peer, true);
        }

        const snapshot = try peers.toOwnedSlice(self.allocator);
        errdefer self.freeSnapshotSlice(snapshot);
        const packet = try self.makePacketBytes(.{
            .kind = "snapshot",
            .origin_id = self.self_id,
            .seq = seq,
            .snapshot = snapshot,
        });
        self.freeSnapshotSlice(snapshot);
        return packet;
    }

    fn freeSnapshotSlice(self: *Cluster, snapshot: []const protocol.ClusterPeerSnapshot) void {
        for (snapshot) |peer| {
            self.allocator.free(peer.ip);
            self.allocator.free(peer.subscriptions);
        }
        self.allocator.free(snapshot);
    }

    fn appendPeerSnapshotLocked(self: *Cluster, list: *std.ArrayList(protocol.ClusterPeerSnapshot), peer: *PeerState, include_address: bool) !void {
        var subs = try self.allocator.alloc(protocol.ClusterSubscription, peer.subscriptions.items.len);
        errdefer self.allocator.free(subs);
        for (peer.subscriptions.items, 0..) |sub, index| {
            subs[index] = .{
                .session_id = sub.session_id,
                .sid = sub.sid,
                .subject = sub.subject,
                .queue = sub.queue,
                .remaining = sub.remaining,
            };
        }

        const ip_text = if (include_address) try addressIpText(self.allocator, peer.address) else try self.allocator.dupe(u8, "127.0.0.1");
        errdefer self.allocator.free(ip_text);
        try list.append(self.allocator, .{
            .peer_id = peer.peer_id,
            .ip = ip_text,
            .port = peer.address.getPort(),
            .subscriptions = subs,
        });
    }

    fn mergeSnapshotLocked(self: *Cluster, origin_id: u64, snapshot: []const protocol.ClusterPeerSnapshot) !void {
        for (snapshot) |peer_snapshot| {
            if (peer_snapshot.peer_id == self.self_id or peer_snapshot.peer_id == origin_id) continue;
            const peer_addr_text = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ peer_snapshot.ip, peer_snapshot.port });
            defer self.allocator.free(peer_addr_text);
            const peer = try self.ensureRemotePeerLocked(peer_snapshot.peer_id, try common_mod.parseServerAddress(peer_addr_text));
            peer.last_seen_ns = std.time.nanoTimestamp();
            peer.subscriptions.deinit(self.allocator);
            peer.subscriptions = .{};
            for (peer_snapshot.subscriptions) |sub| {
                try peer.subscriptions.append(self.allocator, .{
                    .session_id = sub.session_id,
                    .sid = sub.sid,
                    .subject = try self.allocator.dupe(u8, sub.subject),
                    .queue = if (sub.queue) |q| try self.allocator.dupe(u8, q) else null,
                    .remaining = sub.remaining,
                });
            }
        }
    }

    fn mergeRemoteSubscribeLocked(self: *Cluster, peer_id: u64, source: std.net.Address, sub: protocol.ClusterSubscription) !void {
        const peer = try self.ensureRemotePeerLocked(peer_id, source);
        peer.last_seen_ns = std.time.nanoTimestamp();
        _ = self.removeRemoteBySidLocked(peer, sub.sid);
        try peer.subscriptions.append(self.allocator, .{
            .session_id = sub.session_id,
            .sid = sub.sid,
            .subject = try self.allocator.dupe(u8, sub.subject),
            .queue = if (sub.queue) |q| try self.allocator.dupe(u8, q) else null,
            .remaining = sub.remaining,
        });
    }

    fn mergeRemoteUnsubscribeLocked(self: *Cluster, peer_id: u64, sid: u64, max: ?u64) !void {
        const peer = self.findRemotePeerLocked(peer_id) orelse return;
        if (max) |remaining| {
            _ = self.updateRemoteRemainingLocked(peer, sid, remaining);
        } else {
            _ = self.removeRemoteBySidAnyLocked(peer, sid);
        }
    }

    fn recipientAddressesLocked(self: *Cluster) ![]std.net.Address {
        var list = std.ArrayList(std.net.Address).empty;
        defer list.deinit(self.allocator);

        for (self.seed_addresses) |address| {
            try appendUniqueAddress(self.allocator, &list, address);
        }
        for (self.remote_peers.items) |peer| {
            try appendUniqueAddress(self.allocator, &list, peer.address);
        }
        return try list.toOwnedSlice(self.allocator);
    }

    fn remoteAddressesLocked(self: *Cluster, peer_ids: []const u64) ![]std.net.Address {
        var list = std.ArrayList(std.net.Address).empty;
        defer list.deinit(self.allocator);

        for (peer_ids) |peer_id| {
            if (self.findRemotePeerLocked(peer_id)) |peer| {
                try appendUniqueAddress(self.allocator, &list, peer.address);
            }
        }
        return try list.toOwnedSlice(self.allocator);
    }

    fn queuePacketToRecipientsLocked(self: *Cluster, packet: []const u8, key: PacketKey, recipients: []const std.net.Address) !void {
        const now = std.time.nanoTimestamp();
        for (recipients) |dest| {
            const bytes = try self.allocator.dupe(u8, packet);
            try self.pending_packets.append(self.allocator, .{
                .dest = dest,
                .key = key,
                .bytes = bytes,
                .next_retry_ns = now,
                .attempts = 1,
            });
            const pending_index = self.pending_packets.items.len - 1;
            if (posix.sendto(self.socket_fd, bytes, 0, &dest.any, dest.getOsSockLen())) |_| {
                self.pending_packets.items[pending_index].next_retry_ns = now + retryDelayNs(1);
            } else |err| {
                self.allocator.free(bytes);
                _ = self.pending_packets.swapRemove(pending_index);
                return err;
            }
        }
    }

    fn processRetriesLocked(self: *Cluster, now: i128) void {
        var index: usize = 0;
        while (index < self.pending_packets.items.len) {
            const pending = &self.pending_packets.items[index];
            if (now < pending.next_retry_ns) {
                index += 1;
                continue;
            }

            const delay = retryDelayNs(pending.attempts);
            if (posix.sendto(self.socket_fd, pending.bytes, 0, &pending.dest.any, pending.dest.getOsSockLen())) |_| {
                pending.next_retry_ns = now + delay;
                pending.attempts += 1;
                index += 1;
            } else |_| {
                pending.next_retry_ns = now + delay;
                pending.attempts += 1;
                index += 1;
            }
        }
    }

    fn ackPendingLocked(self: *Cluster, source: std.net.Address, origin_id: u64, seq: u64) bool {
        var index: usize = 0;
        while (index < self.pending_packets.items.len) : (index += 1) {
            const pending = self.pending_packets.items[index];
            if (pending.key.origin_id == origin_id and pending.key.seq == seq and std.net.Address.eql(pending.dest, source)) {
                self.allocator.free(pending.bytes);
                _ = self.pending_packets.swapRemove(index);
                return true;
            }
        }
        return false;
    }

    fn sendAckLocked(self: *Cluster, dest: std.net.Address, origin_id: u64, seq: u64) !void {
        const ack_seq = self.nextSeq();
        const packet = try self.makePacketBytes(.{
            .kind = "ack",
            .origin_id = self.self_id,
            .seq = ack_seq,
            .ack_origin_id = origin_id,
            .ack_seq = seq,
        });
        defer self.allocator.free(packet);
        _ = try posix.sendto(self.socket_fd, packet, 0, &dest.any, dest.getOsSockLen());
    }

    fn isDuplicateLocked(self: *Cluster, origin_id: u64, seq: u64) bool {
        for (self.seen_packets.items) |seen| {
            if (seen.origin_id == origin_id and seen.seq == seq) return true;
        }
        return false;
    }

    fn recordSeenLocked(self: *Cluster, origin_id: u64, seq: u64) void {
        self.seen_packets.append(self.allocator, .{ .origin_id = origin_id, .seq = seq }) catch @panic("OOM");
        if (self.seen_packets.items.len > 1024) {
            _ = self.seen_packets.swapRemove(0);
        }
    }

    fn ensureRemotePeerLocked(self: *Cluster, peer_id: u64, address: std.net.Address) !*PeerState {
        if (self.findRemotePeerLocked(peer_id)) |peer| {
            peer.address = address;
            return peer;
        }
        try self.remote_peers.append(self.allocator, .{
            .peer_id = peer_id,
            .address = address,
            .last_seen_ns = std.time.nanoTimestamp(),
        });
        try self.logPeerJoinedLocked(peer_id, address);
        return &self.remote_peers.items[self.remote_peers.items.len - 1];
    }

    fn findRemotePeerLocked(self: *Cluster, peer_id: u64) ?*PeerState {
        for (self.remote_peers.items) |*peer| {
            if (peer.peer_id == peer_id) return peer;
        }
        return null;
    }

    fn pruneStalePeers(self: *Cluster, now: i128) void {
        var index: usize = 0;
        while (index < self.remote_peers.items.len) {
            const peer = self.remote_peers.items[index];
            if (now - peer.last_seen_ns > 15 * std.time.ns_per_s) {
                self.logPeerLeftLocked(peer.peer_id, peer.address);
                self.removePendingForAddressLocked(peer.address);
                freePeerState(self.allocator, &self.remote_peers.items[index]);
                _ = self.remote_peers.swapRemove(index);
                continue;
            }
            index += 1;
        }
    }

    fn updateLocalRemainingLocked(self: *Cluster, session_id: u64, sid: u64, remaining: u64) bool {
        for (self.local_peer.subscriptions.items) |*sub| {
            if (sub.session_id == session_id and sub.sid == sid) {
                sub.remaining = remaining;
                return true;
            }
        }
        return false;
    }

    fn removeLocalBySidLocked(self: *Cluster, session_id: u64, sid: u64) bool {
        var index: usize = 0;
        while (index < self.local_peer.subscriptions.items.len) : (index += 1) {
            const sub = self.local_peer.subscriptions.items[index];
            if (sub.session_id == session_id and sub.sid == sid) {
                self.removeLocalAtLocked(index);
                return true;
            }
        }
        return false;
    }

    fn removeLocalAtLocked(self: *Cluster, index: usize) void {
        const removed = self.local_peer.subscriptions.swapRemove(index);
        self.allocator.free(removed.subject);
        if (removed.queue) |queue| self.allocator.free(queue);
    }

    fn queueCursorLocked(self: *Cluster, key: u64) usize {
        for (self.local_peer.queue_cursors.items) |cursor| {
            if (cursor.key == key) return cursor.cursor;
        }
        return 0;
    }

    fn setQueueCursorLocked(self: *Cluster, key: u64, cursor: usize) void {
        for (self.local_peer.queue_cursors.items) |*item| {
            if (item.key == key) {
                item.cursor = cursor;
                return;
            }
        }
        self.local_peer.queue_cursors.append(self.allocator, .{ .key = key, .cursor = cursor }) catch @panic("OOM");
    }

    fn removeRemoteBySidLocked(self: *Cluster, peer: *PeerState, sid: u64) bool {
        var index: usize = 0;
        while (index < peer.subscriptions.items.len) : (index += 1) {
            if (peer.subscriptions.items[index].sid == sid) {
                self.removeRemoteAt(peer, index);
                return true;
            }
        }
        return false;
    }

    fn removeRemoteBySidAnyLocked(self: *Cluster, peer: *PeerState, sid: u64) bool {
        return self.removeRemoteBySidLocked(peer, sid);
    }

    fn updateRemoteRemainingLocked(self: *Cluster, peer: *PeerState, sid: u64, remaining: u64) bool {
        _ = self;
        for (peer.subscriptions.items) |*sub| {
            if (sub.sid == sid) {
                sub.remaining = remaining;
                return true;
            }
        }
        return false;
    }

    fn removeRemoteAt(self: *Cluster, peer: *PeerState, index: usize) void {
        const removed = peer.subscriptions.swapRemove(index);
        self.allocator.free(removed.subject);
        if (removed.queue) |queue| self.allocator.free(queue);
    }

    fn removePendingForAddressLocked(self: *Cluster, address: std.net.Address) void {
        var index: usize = 0;
        while (index < self.pending_packets.items.len) {
            if (std.net.Address.eql(self.pending_packets.items[index].dest, address)) {
                self.allocator.free(self.pending_packets.items[index].bytes);
                _ = self.pending_packets.swapRemove(index);
                continue;
            }
            index += 1;
        }
    }

    fn makePacketBytes(self: *Cluster, packet: protocol.ClusterPacket) ![]u8 {
        return try protocol.formatClusterPacketJson(self.allocator, packet);
    }

    fn nextSeq(self: *Cluster) u64 {
        const seq = self.next_seq;
        self.next_seq += 1;
        return seq;
    }

    fn logPeerJoinedLocked(self: *Cluster, peer_id: u64, address: std.net.Address) !void {
        const address_text = try addressIpText(self.allocator, address);
        defer self.allocator.free(address_text);
        std.log.info("cluster peer joined: id={d} addr={s}", .{ peer_id, address_text });
    }

    fn logPeerLeftLocked(self: *Cluster, peer_id: u64, address: std.net.Address) void {
        const address_text = addressIpText(self.allocator, address) catch return;
        defer self.allocator.free(address_text);
        std.log.info("cluster peer left: id={d} addr={s}", .{ peer_id, address_text });
    }
};

const GroupKey = struct {
    subject: []const u8,
    queue: []const u8,
};

fn findOrCreateGroup(allocator: std.mem.Allocator, groups: *std.ArrayList(GroupEntry), key: GroupKey) !*GroupEntry {
    for (groups.items) |*group| {
        if (std.mem.eql(u8, group.subject, key.subject) and std.mem.eql(u8, group.queue, key.queue)) return group;
    }
    try groups.append(allocator, .{
        .subject = key.subject,
        .queue = key.queue,
    });
    return &groups.items[groups.items.len - 1];
}

fn chooseWinnerPeerId(seed: u64, peer_ids: []const u64) u64 {
    var winner = peer_ids[0];
    var winner_score = winnerScore(seed, winner);
    for (peer_ids[1..]) |peer_id| {
        const score = winnerScore(seed, peer_id);
        if (score < winner_score or (score == winner_score and peer_id < winner)) {
            winner = peer_id;
            winner_score = score;
        }
    }
    return winner;
}

fn winnerScore(seed: u64, peer_id: u64) u64 {
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], seed, .little);
    std.mem.writeInt(u64, bytes[8..16], peer_id, .little);
    return std.hash.Wyhash.hash(0, &bytes);
}

fn queueCursorKey(subject: []const u8, queue: []const u8) u64 {
    var h = std.hash.Wyhash.init(0);
    h.update(subject);
    h.update(queue);
    return h.final();
}

fn appendUniqueU64(allocator: std.mem.Allocator, list: *std.ArrayListUnmanaged(u64), value: u64) !void {
    for (list.items) |existing| {
        if (existing == value) return;
    }
    try list.append(allocator, value);
}

fn appendUniqueAddress(allocator: std.mem.Allocator, list: *std.ArrayList(std.net.Address), address: std.net.Address) !void {
    for (list.items) |existing| {
        if (std.net.Address.eql(existing, address)) return;
    }
    try list.append(allocator, address);
}

fn addressIpText(allocator: std.mem.Allocator, address: std.net.Address) ![]u8 {
    switch (address.any.family) {
        std.posix.AF.INET => {
            const bytes: *const [4]u8 = @ptrCast(&address.in.sa.addr);
            return try std.fmt.allocPrint(allocator, "{d}.{d}.{d}.{d}:{d}", .{
                bytes[0],
                bytes[1],
                bytes[2],
                bytes[3],
                address.in.getPort(),
            });
        },
        std.posix.AF.INET6 => {
            const parts = .{
                std.mem.readInt(u16, @as(*const [2]u8, @ptrCast(&address.in6.sa.addr[0])), .big),
                std.mem.readInt(u16, @as(*const [2]u8, @ptrCast(&address.in6.sa.addr[2])), .big),
                std.mem.readInt(u16, @as(*const [2]u8, @ptrCast(&address.in6.sa.addr[4])), .big),
                std.mem.readInt(u16, @as(*const [2]u8, @ptrCast(&address.in6.sa.addr[6])), .big),
                std.mem.readInt(u16, @as(*const [2]u8, @ptrCast(&address.in6.sa.addr[8])), .big),
                std.mem.readInt(u16, @as(*const [2]u8, @ptrCast(&address.in6.sa.addr[10])), .big),
                std.mem.readInt(u16, @as(*const [2]u8, @ptrCast(&address.in6.sa.addr[12])), .big),
                std.mem.readInt(u16, @as(*const [2]u8, @ptrCast(&address.in6.sa.addr[14])), .big),
            };
            return try std.fmt.allocPrint(allocator, "[{x}:{x}:{x}:{x}:{x}:{x}:{x}:{x}]:{d}", .{
                parts[0],
                parts[1],
                parts[2],
                parts[3],
                parts[4],
                parts[5],
                parts[6],
                parts[7],
                address.in6.getPort(),
            });
        },
        else => return error.UnsupportedAddressFamily,
    }
}

fn decodeBase64Alloc(allocator: std.mem.Allocator, text: []const u8) ![]u8 {
    const out_len = std.base64.standard.Decoder.calcSizeForSlice(text) catch |err| switch (err) {
        error.InvalidCharacter, error.InvalidPadding => return error.InvalidBase64,
        else => return err,
    };
    const out = try allocator.alloc(u8, out_len);
    try std.base64.standard.Decoder.decode(out, text);
    return out;
}

fn freePeerState(allocator: std.mem.Allocator, peer: *PeerState) void {
    for (peer.subscriptions.items) |sub| {
        allocator.free(sub.subject);
        if (sub.queue) |queue| allocator.free(queue);
    }
    peer.subscriptions.deinit(allocator);
    peer.queue_cursors.deinit(allocator);
}

fn retryDelayNs(attempts: u32) i128 {
    const base: i128 = 100 * std.time.ns_per_ms;
    const exponent = @min(attempts, 5);
    const delay = base << @intCast(exponent);
    return @min(delay, 5 * std.time.ns_per_s);
}
