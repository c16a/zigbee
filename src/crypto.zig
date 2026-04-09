// SPDX-License-Identifier: MIT
const std = @import("std");

pub const Ed25519 = std.crypto.sign.Ed25519;

pub const seed_length = Ed25519.KeyPair.seed_length;
pub const public_key_length = Ed25519.PublicKey.encoded_length;
pub const signature_length = Ed25519.Signature.encoded_length;

pub fn keyPairFromSeed(seed: [seed_length]u8) !Ed25519.KeyPair {
    return try Ed25519.KeyPair.generateDeterministic(seed);
}

pub fn signWithSeed(seed: [seed_length]u8, msg: []const u8) !Ed25519.Signature {
    const key_pair = try keyPairFromSeed(seed);
    return try key_pair.sign(msg, null);
}

pub fn verifySignature(public_key: Ed25519.PublicKey, msg: []const u8, signature: Ed25519.Signature) bool {
    signature.verify(msg, public_key) catch return false;
    return true;
}

pub fn encodeBase64(allocator: std.mem.Allocator, bytes: []const u8) ![]u8 {
    const len = std.base64.standard.Encoder.calcSize(bytes.len);
    const out = try allocator.alloc(u8, len);
    const encoded = std.base64.standard.Encoder.encode(out, bytes);
    return out[0..encoded.len];
}

pub fn decodeBase64Fixed(comptime N: usize, text: []const u8) ![N]u8 {
    const out_len = std.base64.standard.Decoder.calcSizeForSlice(text) catch |err| switch (err) {
        error.InvalidCharacter, error.InvalidPadding => return error.InvalidBase64,
        else => return err,
    };
    if (out_len != N) return error.InvalidLength;

    var out: [N]u8 = undefined;
    try std.base64.standard.Decoder.decode(&out, text);
    return out;
}
