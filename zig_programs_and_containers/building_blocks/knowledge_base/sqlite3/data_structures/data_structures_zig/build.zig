const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Path configuration ──────────────────────────────────────────
    const c_root = b.option([]const u8, "c_root", "Path to data_structures_c") orelse "../data_structures_c";

    const c_include: std.Build.LazyPath = .{ .cwd_relative = b.pathJoin(&.{ c_root, "include" }) };
    const c_lib_dir: std.Build.LazyPath = .{ .cwd_relative = b.pathJoin(&.{ c_root, "lib" }) };

    // ── Helper: configure C linkage on a compile step ───────────────
    const configureLink = struct {
        fn apply(step: *std.Build.Step.Compile, inc: std.Build.LazyPath, lib_dir: std.Build.LazyPath) void {
            step.addIncludePath(inc);
            step.addLibraryPath(lib_dir);
            step.linkSystemLibrary("kb");
            step.linkSystemLibrary("sqlite3");
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

    // ── Static library ──────────────────────────────────────────────
    const lib = b.addStaticLibrary(.{
        .name = "kb_zig",
        .root_source_file = b.path("src/kb.zig"),
        .target = target,
        .optimize = optimize,
    });
    configureLink(lib, c_include, c_lib_dir);
    b.installArtifact(lib);

    // ── Shared library (.so) ────────────────────────────────────────
    const shared_lib = b.addSharedLibrary(.{
        .name = "kb_zig",
        .root_source_file = b.path("src/kb.zig"),
        .target = target,
        .optimize = optimize,
    });
    configureLink(shared_lib, c_include, c_lib_dir);
    b.installArtifact(shared_lib);

    // ── Tests ───────────────────────────────────────────────────────

    // test: kb.zig internal tests
    const t_kb = b.addTest(.{
        .root_source_file = b.path("src/kb.zig"),
        .target = target,
        .optimize = optimize,
    });
    configureLink(t_kb, c_include, c_lib_dir);
    const run_t_kb = b.addRunArtifact(t_kb);
    const step_test = b.step("test", "Run KB wrapper unit tests");
    step_test.dependOn(&run_t_kb.step);

    // test-sexpr: S-expression evaluator tests
    const t_sexpr = b.addTest(.{
        .root_source_file = b.path("tests/test_bit_s_expression.zig"),
        .target = target,
        .optimize = optimize,
    });
    t_sexpr.root_module.addImport("kb", kb_mod);
    configureLink(t_sexpr, c_include, c_lib_dir);
    const run_t_sexpr = b.addRunArtifact(t_sexpr);
    const step_sexpr = b.step("test-sexpr", "Run S-expression tests");
    step_sexpr.dependOn(&run_t_sexpr.step);

    // test-query: KB_Search query support tests
    const t_query = b.addTest(.{
        .root_source_file = b.path("tests/test_query_support.zig"),
        .target = target,
        .optimize = optimize,
    });
    t_query.root_module.addImport("kb", kb_mod);
    configureLink(t_query, c_include, c_lib_dir);
    const run_t_query = b.addRunArtifact(t_query);
    const step_query = b.step("test-query", "Run KB_Search query tests");
    step_query.dependOn(&run_t_query.step);

    // test-ds: integration tests
    const t_ds = b.addTest(.{
        .root_source_file = b.path("tests/test_data_structures.zig"),
        .target = target,
        .optimize = optimize,
    });
    t_ds.root_module.addImport("kb", kb_mod);
    configureLink(t_ds, c_include, c_lib_dir);
    const run_t_ds = b.addRunArtifact(t_ds);
    const step_ds = b.step("test-ds", "Run integration tests");
    step_ds.dependOn(&run_t_ds.step);

    // test-all: run everything
    const step_all = b.step("test-all", "Run all tests");
    step_all.dependOn(&run_t_kb.step);
    step_all.dependOn(&run_t_sexpr.step);
    step_all.dependOn(&run_t_query.step);
    step_all.dependOn(&run_t_ds.step);

    // ── Example executable ──────────────────────────────────────────
    const example = b.addExecutable(.{
        .name = "kb_example",
        .root_source_file = b.path("src/example.zig"),
        .target = target,
        .optimize = optimize,
    });
    example.root_module.addImport("kb", kb_mod);
    configureLink(example, c_include, c_lib_dir);
    b.installArtifact(example);

    const run_example = b.addRunArtifact(example);
    const run_step = b.step("run", "Run the example");
    run_step.dependOn(&run_example.step);
}