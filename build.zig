// SPDX-License-Identifier: MIT
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // Default to a release-safe build so `zig build` produces optimized binaries
    // without needing `-Doptimize=...` on the command line.
    const optimize = .ReleaseSafe;

    const exe = b.addExecutable(.{
        .name = "zigbee",
        .root_module = b.createModule(.{
            .root_source_file = b.path("main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_tests.step);

    const cross_step = b.step("cross", "Build all cross targets");
    addCrossTarget(b, cross_step, optimize, .{
        .step_name = "linux-amd64",
        .description = "Build Linux AMD64",
        .target_triple = "x86_64-linux-musl",
        .output_dir = "linux-amd64",
    });
    addCrossTarget(b, cross_step, optimize, .{
        .step_name = "linux-arm64",
        .description = "Build Linux ARM64",
        .target_triple = "aarch64-linux-musl",
        .output_dir = "linux-arm64",
    });
    addCrossTarget(b, cross_step, optimize, .{
        .step_name = "macos-arm64",
        .description = "Build macOS ARM64",
        .target_triple = "aarch64-macos-none",
        .output_dir = "macos-arm64",
    });
    addCrossTarget(b, cross_step, optimize, .{
        .step_name = "windows-amd64",
        .description = "Build Windows AMD64",
        .target_triple = "x86_64-windows-gnu",
        .output_dir = "windows-amd64",
    });
    addCrossTarget(b, cross_step, optimize, .{
        .step_name = "windows-arm64",
        .description = "Build Windows ARM64",
        .target_triple = "aarch64-windows-gnu",
        .output_dir = "windows-arm64",
    });
}

const CrossTarget = struct {
    step_name: []const u8,
    description: []const u8,
    target_triple: []const u8,
    output_dir: []const u8,
};

fn addCrossTarget(b: *std.Build, cross_step: *std.Build.Step, optimize: std.builtin.OptimizeMode, spec: CrossTarget) void {
    const target_query = std.Build.parseTargetQuery(.{
        .arch_os_abi = spec.target_triple,
    }) catch @panic("invalid cross target");
    const resolved_target = b.resolveTargetQuery(target_query);

    const exe = b.addExecutable(.{
        .name = "zigbee",
        .root_module = b.createModule(.{
            .root_source_file = b.path("main.zig"),
            .target = resolved_target,
            .optimize = optimize,
        }),
    });

    const install = b.addInstallArtifact(exe, .{
        .dest_sub_path = b.fmt("{s}/{s}", .{ spec.output_dir, exe.out_filename }),
    });
    cross_step.dependOn(&install.step);

    const step = b.step(spec.step_name, spec.description);
    step.dependOn(&install.step);
}
