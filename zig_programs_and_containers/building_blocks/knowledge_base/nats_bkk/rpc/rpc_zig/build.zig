const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const c_lib_path: []const u8 = b.option(
        []const u8,
        "c-lib",
        "Path to rpc_c/build (compiled .so/.a files)",
    ) orelse "../rpc_c/build";

    // Library module
    const lib_mod = b.addModule("rpc_zig", .{
        .root_source_file = b.path("src/root.zig"),
    });

    // Shared library
    const shared_lib = b.addSharedLibrary(.{
        .name = "rpc_zig",
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    linkCLibs(shared_lib, c_lib_path);
    b.installArtifact(shared_lib);

    // Static library
    const static_lib = b.addStaticLibrary(.{
        .name = "rpc_zig",
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    linkCLibs(static_lib, c_lib_path);
    b.installArtifact(static_lib);

    // Unit tests (no NATS needed)
    const unit_tests = b.addTest(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    linkCLibs(unit_tests, c_lib_path);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&b.addRunArtifact(unit_tests).step);

    // Integration tests (needs NATS server)
    const integration = b.addTest(.{
        .root_source_file = b.path("test/integration_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    linkCLibs(integration, c_lib_path);
    integration.root_module.addImport("rpc_zig", lib_mod);
    const integ_step = b.step("integration", "Run integration tests (needs NATS)");
    integ_step.dependOn(&b.addRunArtifact(integration).step);

    // Example binary
    const example = b.addExecutable(.{
        .name = "example",
        .root_source_file = b.path("test/example.zig"),
        .target = target,
        .optimize = optimize,
    });
    linkCLibs(example, c_lib_path);
    example.root_module.addImport("rpc_zig", lib_mod);
    b.installArtifact(example);
    const example_step = b.step("example", "Run example (needs NATS)");
    example_step.dependOn(&b.addRunArtifact(example).step);
}

fn linkCLibs(step: *std.Build.Step.Compile, c_lib_path: []const u8) void {
    step.addLibraryPath(step.step.owner.path(c_lib_path));
    step.linkSystemLibrary("nats_rpc");
    step.linkSystemLibrary("nats");
    step.linkSystemLibrary("cjson");
    step.linkLibC();
    step.addRPath(step.step.owner.path(c_lib_path));
}