// SPDX-License-Identifier: MIT
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // Default to a release-safe build so `zig build` produces optimized binaries
    // without needing `-Doptimize=...` on the command line.
    const optimize = .ReleaseSafe;

    const common_client = b.createModule(.{
        .root_source_file = b.path("src/common/client.zig"),
        .target = target,
        .optimize = optimize,
    });
    const cli_modules = makeCliModules(b, target, optimize, common_client);
    const server_exe = makeExecutable(b, "zigbee", "src/server/main.zig", target, optimize, false);
    server_exe.root_module.addImport("common_client", common_client);
    const cli_main_module = b.createModule(.{
        .root_source_file = b.path("src/cli/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    cli_main_module.addImport("cli_app", cli_modules.app);
    const cli_exe = b.addExecutable(.{
        .name = "zigbee-cli",
        .root_module = cli_main_module,
    });

    b.installArtifact(server_exe);
    b.installArtifact(cli_exe);

    const run_step = b.step("run", "Run the server");
    const run_cmd = b.addRunArtifact(server_exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_cli_step = b.step("run-cli", "Run the CLI");
    const run_cli_cmd = b.addRunArtifact(cli_exe);
    run_cli_step.dependOn(&run_cli_cmd.step);
    run_cli_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cli_cmd.addArgs(args);
    }

    const tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tests.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    tests.root_module.addImport("common_client", common_client);
    server_exe.root_module.addImport("common_client", common_client);
    cli_main_module.addImport("common_client", common_client);

    const cli_types_tests = b.addTest(.{
        .root_module = cli_modules.types,
    });
    const cli_parse_tests = b.addTest(.{
        .root_module = cli_modules.parse,
    });
    const run_tests = b.step("test", "Run tests");
    const run_server_tests = b.addRunArtifact(tests);
    const run_cli_types_tests = b.addRunArtifact(cli_types_tests);
    const run_cli_parse_tests = b.addRunArtifact(cli_parse_tests);
    run_cli_types_tests.step.dependOn(&run_server_tests.step);
    run_cli_parse_tests.step.dependOn(&run_cli_types_tests.step);
    run_tests.dependOn(&run_cli_parse_tests.step);

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
        .step_name = "linux-arm32",
        .description = "Build Linux ARM32",
        .target_triple = "arm-linux-musleabihf",
        .output_dir = "linux-arm32",
    });
    addCrossTarget(b, cross_step, optimize, .{
        .step_name = "linux-riscv64",
        .description = "Build Linux RISCV64",
        .target_triple = "riscv64-linux-musl",
        .output_dir = "linux-riscv64",
    });
    addCrossTarget(b, cross_step, optimize, .{
        .step_name = "linux-ppc64",
        .description = "Build Linux PowerPC64",
        .target_triple = "powerpc64le-linux-musl",
        .output_dir = "linux-ppc64",
    });
    addCrossTarget(b, cross_step, optimize, .{
        .step_name = "linux-s390x",
        .description = "Build Linux s390x",
        .target_triple = "s390x-linux-musl",
        .output_dir = "linux-s390x",
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

fn makeExecutable(b: *std.Build, name: []const u8, root_source: []const u8, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, link_libc: bool) *std.Build.Step.Compile {
    return b.addExecutable(.{
        .name = name,
        .root_module = b.createModule(.{
            .root_source_file = b.path(root_source),
            .target = target,
            .optimize = optimize,
            .link_libc = link_libc,
        }),
    });
}

const CliModules = struct {
    app: *std.Build.Module,
    types: *std.Build.Module,
    parse: *std.Build.Module,
    bench: *std.Build.Module,
};

fn makeCliModules(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, common_client: *std.Build.Module) CliModules {
    const types = b.createModule(.{
        .root_source_file = b.path("src/cli/types.zig"),
        .target = target,
        .optimize = optimize,
    });
    types.addImport("common_client", common_client);

    const parse = b.createModule(.{
        .root_source_file = b.path("src/cli/parse.zig"),
        .target = target,
        .optimize = optimize,
    });
    parse.addImport("common_client", common_client);
    parse.addImport("cli_types", types);

    const bench = b.createModule(.{
        .root_source_file = b.path("src/cli/bench.zig"),
        .target = target,
        .optimize = optimize,
    });
    bench.addImport("common_client", common_client);
    bench.addImport("cli_types", types);

    const app = b.createModule(.{
        .root_source_file = b.path("src/cli/app.zig"),
        .target = target,
        .optimize = optimize,
    });
    app.addImport("common_client", common_client);
    app.addImport("cli_types", types);
    app.addImport("cli_parse", parse);
    app.addImport("cli_bench", bench);

    return .{
        .app = app,
        .types = types,
        .parse = parse,
        .bench = bench,
    };
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

    const common_client = b.createModule(.{
        .root_source_file = b.path("src/common/client.zig"),
        .target = resolved_target,
        .optimize = optimize,
    });
    const needs_libc = resolved_target.result.os.tag == .windows;
    const server_exe = makeExecutable(b, "zigbee", "src/server/main.zig", resolved_target, optimize, needs_libc);
    server_exe.root_module.addImport("common_client", common_client);
    const cli_modules = makeCliModules(b, resolved_target, optimize, common_client);
    const cli_main_module = b.createModule(.{
        .root_source_file = b.path("src/cli/main.zig"),
        .target = resolved_target,
        .optimize = optimize,
        .link_libc = needs_libc,
    });
    cli_main_module.addImport("cli_app", cli_modules.app);
    const cli_exe = b.addExecutable(.{
        .name = "zigbee-cli",
        .root_module = cli_main_module,
    });

    const server_install = b.addInstallArtifact(server_exe, .{
        .dest_sub_path = b.fmt("{s}/{s}", .{ spec.output_dir, server_exe.out_filename }),
    });
    const cli_install = b.addInstallArtifact(cli_exe, .{
        .dest_sub_path = b.fmt("{s}/{s}", .{ spec.output_dir, cli_exe.out_filename }),
    });
    cross_step.dependOn(&server_install.step);
    cross_step.dependOn(&cli_install.step);

    const step = b.step(spec.step_name, spec.description);
    step.dependOn(&server_install.step);
    step.dependOn(&cli_install.step);
}
