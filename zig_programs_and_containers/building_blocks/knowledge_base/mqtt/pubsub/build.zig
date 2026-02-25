const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Static library ───────────────────────────────────────────────
    const static_lib = b.addStaticLibrary(.{
        .name = "mqtt_pubsub",
        .root_source_file = .{ .cwd_relative = "src/root.zig" },
        .target = target,
        .optimize = optimize,
    });
    static_lib.linkSystemLibrary("mosquitto");
    static_lib.linkLibC();
    b.installArtifact(static_lib);

    // ── Shared library ───────────────────────────────────────────────
    const shared_lib = b.addSharedLibrary(.{
        .name = "mqtt_pubsub",
        .root_source_file = .{ .cwd_relative = "src/root.zig" },
        .target = target,
        .optimize = optimize,
    });
    shared_lib.linkSystemLibrary("mosquitto");
    shared_lib.linkLibC();
    b.installArtifact(shared_lib);

    // ── Unit tests (no broker needed) ────────────────────────────────
    const unit_tests = b.addTest(.{
        .root_source_file = .{ .cwd_relative = "src/root.zig" },
        .target = target,
        .optimize = optimize,
    });
    unit_tests.linkSystemLibrary("mosquitto");
    unit_tests.linkLibC();

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);

    // ── Integration test executable ──────────────────────────────────
    const test_exe = b.addExecutable(.{
        .name = "pubsub_test",
        .root_source_file = .{ .cwd_relative = "test/pubsub_test.zig" },
        .target = target,
        .optimize = optimize,
    });
    test_exe.root_module.addImport("mqtt_pubsub", &static_lib.root_module);
    test_exe.linkSystemLibrary("mosquitto");
    test_exe.linkLibC();
    b.installArtifact(test_exe);

    const run_test = b.addRunArtifact(test_exe);
    run_test.step.dependOn(b.getInstallStep());
    const run_test_step = b.step("run-test", "Run integration test (needs broker)");
    run_test_step.dependOn(&run_test.step);
}