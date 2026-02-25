const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const root_source = b.path("src/root.zig");

    // ── Static library (.a) ──────────────────────────────────────────

    const static_lib = b.addStaticLibrary(.{
        .name = "mqtt_queue",
        .root_source_file = root_source,
        .target = target,
        .optimize = optimize,
    });
    static_lib.linkSystemLibrary("mosquitto");
    static_lib.linkLibC();
    b.installArtifact(static_lib);

    // ── Shared library (.so) ─────────────────────────────────────────

    const shared_lib = b.addSharedLibrary(.{
        .name = "mqtt_queue",
        .root_source_file = root_source,
        .target = target,
        .optimize = optimize,
    });
    shared_lib.linkSystemLibrary("mosquitto");
    shared_lib.linkLibC();
    b.installArtifact(shared_lib);

    // ── Unit tests (zig build test) ─────────────────────────────────

    const lib_tests = b.addTest(.{
        .root_source_file = root_source,
        .target = target,
        .optimize = optimize,
    });
    lib_tests.linkSystemLibrary("mosquitto");
    lib_tests.linkLibC();

    const run_tests = b.addRunArtifact(lib_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_tests.step);

    // ── Integration test executable (zig build run-test) ─────────────

    const test_exe = b.addExecutable(.{
        .name = "mqtt_queue_test",
        .root_source_file = b.path("test/mqtt_queue_test.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_exe.root_module.addImport("mqtt_queue", &static_lib.root_module);
    test_exe.linkSystemLibrary("mosquitto");
    test_exe.linkLibC();
    b.installArtifact(test_exe);

    const run_test_exe = b.addRunArtifact(test_exe);
    run_test_exe.step.dependOn(b.getInstallStep());
    const run_test_step = b.step("run-test", "Run the integration test driver");
    run_test_step.dependOn(&run_test_exe.step);
}