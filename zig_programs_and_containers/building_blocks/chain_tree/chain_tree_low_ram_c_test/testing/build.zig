const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "chaintree",
        .root_source_file = b.path("main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Include paths
    exe.addIncludePath(b.path("library_files"));
    exe.addIncludePath(b.path("generated_yaml_files"));
    exe.addIncludePath(b.path(".")); // for user_function_headers.h

    // Library C files
    const lib_sources = [_][]const u8{
        "library_files/CT_Tree_Walker.c",
        "library_files/cfl_boolean_functions.c",
        "library_files/cfl_common_functions.c",
        "library_files/cfl_engine.c",
        "library_files/cfl_event_queue.c",
        "library_files/cfl_exception.c",
        "library_files/cfl_exception_support.c",
        "library_files/cfl_heap.c",
        "library_files/cfl_heap_arena_allocate.c",
        "library_files/cfl_main_functions.c",
        "library_files/cfl_one_shot_functions.c",
        "library_files/cfl_perm.c",
        "library_files/cfl_runtime.c",
        "library_files/cfl_sm_functions.c",
        "library_files/cfl_supervisor_support.c",
        "library_files/cfl_timer_system.c",
        "library_files/chaintree_support.c",
        "library_files/json_node_decoder.c",
    };

    // Generated C files
    const gen_sources = [_][]const u8{
        "generated_yaml_files/test_header.c",
        "generated_yaml_files/test_header_bitmasks.c",
        "generated_yaml_files/test_header_events.c",
        "generated_yaml_files/test_header_functions.c",
        "generated_yaml_files/test_header_kb_info.c",
        "generated_yaml_files/test_header_links.c",
        "generated_yaml_files/test_header_node_data.c",
        "generated_yaml_files/test_header_nodes.c",
    };

    // User C files
    const user_sources = [_][]const u8{
        "user_boolean_functions.c",
        "user_main_functions.c",
        "user_one_shot_functions.c",
    };

    const c_flags = [_][]const u8{
        "-std=c11",
        "-D_GNU_SOURCE",  // <-- enables POSIX extensions
        "-Wall",
        "-Wextra",
        "-fno-strict-aliasing",
    };

    exe.addCSourceFiles(.{
        .files = &lib_sources,
        .flags = &c_flags,
    });
    exe.addCSourceFiles(.{
        .files = &gen_sources,
        .flags = &c_flags,
    });
    exe.addCSourceFiles(.{
        .files = &user_sources,
        .flags = &c_flags,
    });

    exe.linkLibC();

    b.installArtifact(exe);

    // Run step
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run chaintree");
    run_step.dependOn(&run_cmd.step);
}
