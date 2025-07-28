//! By convention, main.zig is where your main function lives in the case that
//! you are building an executable. If you are making a library, the convention
//! is to delete this file and start with root.zig instead.

const std = @import("std");
const zuckdb = @import("zuckdb");

// Benchmark configuration
const BenchmarkConfig = struct {
    iterations: usize = 100000,
    warmup_iterations: usize = 1000,
    batch_size: usize = 100,
};

// Statistics tracking
const Stats = struct {
    min_ns: u64 = std.math.maxInt(u64),
    max_ns: u64 = 0,
    total_ns: u64 = 0,
    count: usize = 0,

    pub fn addMeasurement(self: *Stats, duration_ns: u64) void {
        self.min_ns = @min(self.min_ns, duration_ns);
        self.max_ns = @max(self.max_ns, duration_ns);
        self.total_ns += duration_ns;
        self.count += 1;
    }

    pub fn mean(self: Stats) f64 {
        if (self.count == 0) return 0.0;
        return @as(f64, @floatFromInt(self.total_ns)) / @as(f64, @floatFromInt(self.count));
    }

    pub fn print(self: Stats, name: []const u8) void {
        const stdout = std.io.getStdOut().writer();
        stdout.print("\n=== {s} ===\n", .{name}) catch {};
        stdout.print("Count: {d}\n", .{self.count}) catch {};
        stdout.print("Min: {d:.2} μs\n", .{@as(f64, @floatFromInt(self.min_ns)) / 1000.0}) catch {};
        stdout.print("Max: {d:.2} μs\n", .{@as(f64, @floatFromInt(self.max_ns)) / 1000.0}) catch {};
        stdout.print("Mean: {d:.2} μs\n", .{self.mean() / 1000.0}) catch {};
        stdout.print("Total: {d:.2} ms\n", .{@as(f64, @floatFromInt(self.total_ns)) / 1000000.0}) catch {};
    }
};

// Benchmark runner
const BenchmarkRunner = struct {
    config: BenchmarkConfig,
    db: ?zuckdb.DB = null,
    conn: ?zuckdb.Conn = null,

    pub fn init() !BenchmarkRunner {
        return BenchmarkRunner{
            .config = .{},
        };
    }

    pub fn deinit(self: *BenchmarkRunner) void {
        if (self.conn) |*conn| {
            conn.deinit();
        }
        if (self.db) |*db| {
            db.deinit();
        }
    }

    pub fn setup(self: *BenchmarkRunner) !void {
        // Create in-memory database
        self.db = try zuckdb.DB.init(std.heap.page_allocator, ":memory:", .{});
        self.conn = try self.db.?.conn();

        // Enable prepared statements
        // _ = try self.conn.?.exec("PRAGMA enable_prepared_statements = true", .{});
    }

    pub fn runPreparedSelect1Benchmark(self: *BenchmarkRunner) !Stats {
        var stats = Stats{};
        var timer = try std.time.Timer.start();

        // Prepare the statement
        const stmt = try self.conn.?.prepare("SELECT 1", .{});
        defer stmt.deinit();

        // Warmup
        for (0..self.config.warmup_iterations) |_| {
            var result = try stmt.query(null);
            defer result.deinit();
            _ = try result.next();
        }

        // Actual benchmark
        for (0..self.config.iterations) |_| {
            timer.reset();
            var result = try stmt.query(null);
            defer result.deinit();
            _ = try result.next();
            const duration = timer.read();
            stats.addMeasurement(duration);
        }

        return stats;
    }

    pub fn runUnpreparedSelect1Benchmark(self: *BenchmarkRunner) !Stats {
        var stats = Stats{};
        var timer = try std.time.Timer.start();

        // Warmup
        for (0..self.config.warmup_iterations) |_| {
            var result = try self.conn.?.query("SELECT 1", .{});
            defer result.deinit();
            _ = try result.next();
        }

        // Actual benchmark
        for (0..self.config.iterations) |_| {
            timer.reset();
            var result = try self.conn.?.query("SELECT 1", .{});
            defer result.deinit();
            _ = try result.next();
            const duration = timer.read();
            stats.addMeasurement(duration);
        }

        return stats;
    }
};

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

    try stdout.print("DuckDB Zig Benchmark Suite\n", .{});
    try stdout.print("==========================\n\n", .{});

    var runner = try BenchmarkRunner.init();
    defer runner.deinit();

    try stdout.print("Setting up database connection...\n", .{});
    try runner.setup();
    try stdout.print("Database connection established.\n\n", .{});

    try stdout.print("Running benchmarks...\n", .{});
    try stdout.print("Configuration: {d} iterations, {d} warmup, batch size {d}\n\n", .{
        runner.config.iterations,
        runner.config.warmup_iterations,
        runner.config.batch_size,
    });

    // Run unprepared SELECT 1 benchmark
    // try stdout.print("Running unprepared SELECT 1 benchmark...\n", .{});
    // const unprepared_stats = try runner.runUnpreparedSelect1Benchmark();
    // unprepared_stats.print("Unprepared SELECT 1");

    // Run prepared SELECT 1 benchmark
    try stdout.print("\nRunning prepared SELECT 1 benchmark...\n", .{});
    const prepared_stats = try runner.runPreparedSelect1Benchmark();
    prepared_stats.print("Prepared SELECT 1");

    // // Run batch prepared SELECT 1 benchmark
    // try stdout.print("\nRunning batch prepared SELECT 1 benchmark...\n", .{});
    // const batch_stats = try runner.runBatchPreparedSelect1Benchmark();
    // batch_stats.print("Batch Prepared SELECT 1");

    // Calculate and display performance comparison
    // const unprepared_mean = unprepared_stats.mean();
    const prepared_mean = prepared_stats.mean();
    // const batch_mean = batch_stats.mean();

    try stdout.print("\n=== Performance Comparison ===\n", .{});
    // if (unprepared_mean > 0) {
    //     const speedup = unprepared_mean / prepared_mean;
    //     try stdout.print("Prepared vs Unprepared speedup: {d:.2}x\n", .{speedup});
    // }
    try stdout.print("Prepared mean: {d:.2} μs\n", .{prepared_mean / 1000.0});
    // if (batch_mean > 0) {
    //     const batch_efficiency = (batch_mean / @as(f64, @floatFromInt(runner.config.batch_size))) / prepared_mean;
    //     try stdout.print("Batch efficiency: {d:.2}x\n", .{batch_efficiency});
    // }

    try stdout.print("\nBenchmark completed successfully!\n", .{});
}

test "basic structure" {
    try std.testing.expect(true);
}

test "stats calculation" {
    var stats = Stats{};
    stats.addMeasurement(1000);
    stats.addMeasurement(2000);
    stats.addMeasurement(3000);

    try std.testing.expect(stats.count == 3);
    try std.testing.expect(stats.min_ns == 1000);
    try std.testing.expect(stats.max_ns == 3000);
    try std.testing.expect(stats.total_ns == 6000);
    try std.testing.expect(stats.mean() == 2000.0);
}
