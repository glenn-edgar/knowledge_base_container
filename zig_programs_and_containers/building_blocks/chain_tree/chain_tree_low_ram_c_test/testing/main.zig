const std = @import("std");

const c = @cImport({
    @cInclude("cfl_runtime.h");
    @cInclude("chaintree_support.h");
    @cInclude("cfl_exception.h");
    @cInclude("cfl_heap.h");
    @cInclude("test_header.h");
});

// Static module-level variables
var perm: c.cfl_perm_t = undefined;
var perm_buffer: [0xffff]u8 = undefined;

// External symbol from generated C code
extern const g_test_header: c.chaintree_handle_t;

pub fn main() !u8 {
    c.setup_abort_handler();

    const test_handle: *const c.chaintree_handle_t = &g_test_header;

    // Validate test index is within bounds
    const test_index: u16 = 3;
    if (test_index >= test_handle.kb_count) {
        std.debug.print("Error: test_index {} >= kb_count {}\n", .{ test_index, test_handle.kb_count });
        return 1;
    }

    // Use the provided API function
    const params: *c.cfl_runtime_create_params_t = c.cfl_runtime_create_params_create() orelse {
        std.debug.print("Failed to allocate memory for params\n", .{});
        return 1;
    };
    defer c.cfl_runtime_create_params_destroy(params);

    params.perm = &perm;
    params.perm_buffer = &perm_buffer;
    params.perm_buffer_size = @intCast(perm_buffer.len);
    params.heap_size = 4096;
    params.max_allocator_count = c.cfl_calculate_arrena_number(test_handle);
    params.total_node_count = test_handle.node_count;
    std.debug.print("total_node_count: {}\n", .{params.total_node_count});

    // Check for overflow in allocator_0_size calculation
    const allocator_size: usize = 50;
    if (allocator_size > 65535) {
        std.debug.print("Error: allocator_0_size calculation overflow: {} > 65535\n", .{allocator_size});
        return 1;
    }
    params.allocator_0_size = @intCast(allocator_size);

    params.event_queue_high_priority_size = 8;
    params.event_queue_low_priority_size = 64;
    params.delta_time = 0.1;

    const handle: *c.cfl_runtime_handle_t = c.cfl_runtime_create(&perm, params, test_handle) orelse {
        std.debug.print("Failed to create runtime handle\n", .{});
        return 1;
    };

    c.cfl_runtime_reset(handle);

    // Uncomment tests as needed
    // _ = c.cfl_add_test_by_index(handle, 0);  // first test
    // _ = c.cfl_add_test_by_index(handle, 1);  // second test
    // _ = c.cfl_add_test_by_index(handle, 2);  // fourth test
    // _ = c.cfl_add_test_by_index(handle, 3);  // fifth test
    // _ = c.cfl_add_test_by_index(handle, 4);  // sixth test
    // _ = c.cfl_add_test_by_index(handle, 5);  // seventh test
    // _ = c.cfl_add_test_by_index(handle, 6);  // eighth test
    // _ = c.cfl_add_test_by_index(handle, 7);  // ninth test
    // _ = c.cfl_add_test_by_index(handle, 8);  // tenth test
    // _ = c.cfl_add_test_by_index(handle, 9);  // eleventh test
    // _ = c.cfl_add_test_by_index(handle, 10); // twelfth test
    // _ = c.cfl_add_test_by_index(handle, 11); // thirteenth test
    // _ = c.cfl_add_test_by_index(handle, 12); // fourteenth test
    // _ = c.cfl_add_test_by_index(handle, 13); // seventeenth test
    // _ = c.cfl_add_test_by_index(handle, 14); // eighteenth test
    // _ = c.cfl_add_test_by_index(handle, 15); // nineteenth test
    // _ = c.cfl_add_test_by_index(handle, 16); // twentieth test
     _ = c.cfl_add_test_by_index(handle, 17); // twenty-first test
    //_ = c.cfl_add_test_by_index(handle, 18); // twenty-second test

    std.debug.print("heap used bytes: {}\n", .{c.cfl_heap_used_bytes(handle.heap)});
    std.debug.print("heap free bytes: {}\n", .{c.cfl_heap_free_bytes(handle.heap)});

    const result = c.cfl_runtime_run(handle);
    std.debug.print("Runtime run result: {}\n", .{result});

    return if (result) 0 else 1;
}