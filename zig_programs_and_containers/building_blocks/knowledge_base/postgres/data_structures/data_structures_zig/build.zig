const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Path configuration ──────────────────────────────────────────
    const c_root = b.option([]const u8, "c_root", "Path to data_structures_c") orelse "../data_structures_c";

    const c_include: std.Build.LazyPath = .{ .cwd_relative = b.pathJoin(&.{ c_root, "include" }) };
    const c_lib_dir: std.Build.LazyPath = .{ .cwd_relative = c_root };

    // ── PostgreSQL include path ─────────────────────────────────────
    const pg_include = b.option([]const u8, "pg_include", "Path to PostgreSQL headers (libpq-fe.h)") orelse "/usr/include/postgresql";
    const pg_include_path: std.Build.LazyPath = .{ .cwd_relative = pg_include };

    // ── Helper: configure C linkage on a compile step ───────────────
    const configureLink = struct {
        fn apply(step: *std.Build.Step.Compile, inc: std.Build.LazyPath, lib_dir: std.Build.LazyPath, pg_inc: std.Build.LazyPath) void {
            step.addIncludePath(inc);
            step.addIncludePath(pg_inc);
            step.addLibraryPath(lib_dir);
            step.linkSystemLibrary("kb_data_structures");
            step.linkSystemLibrary("pq");
            step.linkSystemLibrary("cjson");
            step.linkLibC();
        }
    }.apply;

    // ── Zig wrapper module ──────────────────────────────────────────
    const kb_mod = b.addModule("kb", .{
        .root_source_file = b.path("src/kb.zig"),
        .target = target,
        .optimize = optimize,
    });
    kb_mod.addIncludePath(c_include);
    kb_mod.addIncludePath(pg_include_path);

    // ── Static library ──────────────────────────────────────────────
    const lib = b.addStaticLibrary(.{
        .name = "kb_pg_zig",
        .root_source_file = b.path("src/kb.zig"),
        .target = target,
        .optimize = optimize,
    });
    configureLink(lib, c_include, c_lib_dir, pg_include_path);
    b.installArtifact(lib);

    // ── Shared library (.so) ────────────────────────────────────────
    const shared_lib = b.addSharedLibrary(.{
        .name = "kb_pg_zig",
        .root_source_file = b.path("src/kb.zig"),
        .target = target,
        .optimize = optimize,
    });
    configureLink(shared_lib, c_include, c_lib_dir, pg_include_path);
    b.installArtifact(shared_lib);

    // ── Tests ───────────────────────────────────────────────────────

    // test: kb.zig internal tests
    const t_kb = b.addTest(.{
        .root_source_file = b.path("src/kb.zig"),
        .target = target,
        .optimize = optimize,
    });
    configureLink(t_kb, c_include, c_lib_dir, pg_include_path);
    const run_t_kb = b.addRunArtifact(t_kb);
    const step_test = b.step("test", "Run KB wrapper unit tests");
    step_test.dependOn(&run_t_kb.step);

    // test-driver: integration test (requires live PostgreSQL)
    const t_driver = b.addTest(.{
        .root_source_file = b.path("tests/test_driver.zig"),
        .target = target,
        .optimize = optimize,
    });
    t_driver.root_module.addImport("kb", kb_mod);
    configureLink(t_driver, c_include, c_lib_dir, pg_include_path);

    // Install test binary to zig-out/bin/test_driver
    const install_t_driver = b.addInstallArtifact(t_driver, .{
        .dest_sub_path = "test_driver",
    });

    const run_t_driver = b.addRunArtifact(t_driver);
    const step_driver = b.step("test-driver", "Run integration tests (requires PostgreSQL)");
    step_driver.dependOn(&run_t_driver.step);

    // build-test-driver: just compile + install, don't run
    const step_build_driver = b.step("build-test-driver", "Build test driver to zig-out/bin/");
    step_build_driver.dependOn(&install_t_driver.step);

    // test-all
    const step_all = b.step("test-all", "Run all tests");
    step_all.dependOn(&run_t_kb.step);
    step_all.dependOn(&run_t_driver.step);

    // ── Example executable ──────────────────────────────────────────
    const example = b.addExecutable(.{
        .name = "kb_pg_example",
        .root_source_file = b.path("src/example.zig"),
        .target = target,
        .optimize = optimize,
    });
    example.root_module.addImport("kb", kb_mod);
    configureLink(example, c_include, c_lib_dir, pg_include_path);
    b.installArtifact(example);

    const run_example = b.addRunArtifact(example);
    const run_step = b.step("run", "Run the example (requires PostgreSQL)");
    run_step.dependOn(&run_example.step);
}