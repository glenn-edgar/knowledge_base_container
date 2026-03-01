//! example.zig — PostgreSQL Knowledge Base usage example.
//!
//! Demonstrates all subsystems: search, status, job queue, stream,
//! bit structures, RPC server/client, link tables, and document table.
//!
//! Requires a live PostgreSQL database with the knowledge base constructed.
//!
//! Usage:
//!   POSTGRES_PASSWORD=secret zig build run

const std = @import("std");
const kb = @import("kb");

fn getEnv(key: [:0]const u8, default: [:0]const u8) [:0]const u8 {
    return std.posix.getenvZ(key) orelse default;
}

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    const password = std.posix.getenvZ("POSTGRES_PASSWORD") orelse {
        try stdout.print("Error: POSTGRES_PASSWORD environment variable required\n", .{});
        return;
    };

    // Connect
    var conn = try kb.Connection.connectParams(
        getEnv("POSTGRES_HOST", "localhost"),
        getEnv("POSTGRES_PORT", "5432"),
        getEnv("POSTGRES_DB", "knowledge_base"),
        getEnv("POSTGRES_USER", "gedgar"),
        password,
    );
    defer conn.disconnect();
    try stdout.print("Connected to PostgreSQL.\n", .{});

    const database = getEnv("KB_DATABASE", "knowledge_base");

    // ── Search / Discovery ──────────────────────────────────────────
    var ks = try kb.Search.create(&conn, database);
    defer ks.destroy();

    try stdout.print("\n=== Discovery ===\n", .{});

    var status_pl = try ks.findStatusPaths();
    defer status_pl.deinit();
    try stdout.print("Status paths: {d}\n", .{status_pl.len()});

    var job_pl = try ks.findJobPaths();
    defer job_pl.deinit();
    try stdout.print("Job paths: {d}\n", .{job_pl.len()});

    var stream_pl = try ks.findStreamPaths();
    defer stream_pl.deinit();
    try stdout.print("Stream paths: {d}\n", .{stream_pl.len()});

    // ── Status ──────────────────────────────────────────────────────
    if (status_pl.len() > 0) {
        if (status_pl.get(0)) |path| {
            try stdout.print("\n=== Status: {s} ===\n", .{path});
            const status = kb.Status.init(&conn, database);
            try status.setDefault(path, "{\"temp\":72.5}");
            if (try status.get(path)) |data| {
                try stdout.print("  data: {s}\n", .{data});
                kb.freeCStr(@ptrCast(@constCast(data.ptr)));
            }
        }
    }

    // ── Job Queue ───────────────────────────────────────────────────
    if (job_pl.len() > 0) {
        if (job_pl.get(0)) |path| {
            try stdout.print("\n=== Job Queue: {s} ===\n", .{path});
            const jq = kb.JobQueue.init(&conn, database);

            try jq.clearDefault(path);
            try jq.pushDefault(path, "{\"task\":\"example\"}");

            var info = try jq.peekDefault(path);
            defer info.deinit();
            if (info.data) |d| {
                try stdout.print("  peeked: {s}\n", .{d});
                try jq.completeDefault(info.id);
            }
        }
    }

    // ── Stream ──────────────────────────────────────────────────────
    if (stream_pl.len() > 0) {
        if (stream_pl.get(0)) |path| {
            try stdout.print("\n=== Stream: {s} ===\n", .{path});
            const stream = kb.Stream.init(&conn, database);

            try stream.clearDefault(path);
            try stream.pushDefault(path, "{\"value\":42}");

            const cnt = try stream.count(path);
            try stdout.print("  valid entries: {d}\n", .{cnt});

            try stream.clearDefault(path);
        }
    }

    try stdout.print("\nDone.\n", .{});
}