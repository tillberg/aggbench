const std = @import("std");
const testing = std.testing;

// SalesRecord is equivalent to the Go struct
// region, product, amount, quantity
pub const SalesRecord = struct {
    region: []const u8,
    product: []const u8,
    amount: f64,
    quantity: i64,
};

pub const AggResult = struct {
    total_sales: i64,
    total_amount: f64,
    total_quantity: i64,
};

pub const GroupKey = struct {
    region: []const u8,
    product: []const u8,
};

const GroupKeyContext = struct {
    pub fn hash(_: GroupKeyContext, key: GroupKey) u64 {
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(key.region);
        hasher.update(key.product);
        return hasher.final();
    }

    pub fn eql(_: GroupKeyContext, a: GroupKey, b: GroupKey) bool {
        return std.mem.eql(u8, a.region, b.region) and std.mem.eql(u8, a.product, b.product);
    }
};

fn aggregateSales(records: []const SalesRecord) struct { region: []const u8, product: []const u8, agg: AggResult } {
    // Use fixed-size array since we have only 4 regions × 5 products = 20 combinations
    var groups: [4][5]AggResult = undefined;
    for (0..4) |r| {
        for (0..5) |p| {
            groups[r][p] = AggResult{ .total_sales = 0, .total_amount = 0.0, .total_quantity = 0 };
        }
    }

    for (records) |record| {
        const region_idx = getRegionIndex(record.region);
        const product_idx = getProductIndex(record.product);
        groups[region_idx][product_idx].total_sales += 1;
        groups[region_idx][product_idx].total_amount += record.amount;
        groups[region_idx][product_idx].total_quantity += record.quantity;
    }

    // Find the group with the highest total_amount
    var max_region: usize = 0;
    var max_product: usize = 0;
    var max_amount: f64 = 0.0;

    for (0..4) |r| {
        for (0..5) |p| {
            if (groups[r][p].total_amount > max_amount) {
                max_amount = groups[r][p].total_amount;
                max_region = r;
                max_product = p;
            }
        }
    }

    return .{
        .region = regions[max_region],
        .product = products[max_product],
        .agg = groups[max_region][max_product],
    };
}

const TOTAL_RECORDS: usize = 1_000_000;
const EXPECTED_TOTAL_SALES: i64 = TOTAL_RECORDS / 20;
const EXPECTED_TOTAL_AMOUNT: f64 = 2500950000.0;
const EXPECTED_AVG_AMOUNT: f64 = 500190.0;
const EXPECTED_TOTAL_QUANTITY: i64 = TOTAL_RECORDS / 2;
const EXPECTED_AVG_QUANTITY: f64 = 10.0;

const regions = [_][]const u8{ "North", "South", "East", "West" };
const products = [_][]const u8{ "Laptop", "Phone", "Tablet", "Monitor", "Keyboard" };

fn getRegionIndex(region: []const u8) u8 {
    for (regions, 0..) |r, i| {
        if (std.mem.eql(u8, r, region)) return @intCast(i);
    }
    unreachable;
}

fn getProductIndex(product: []const u8) u8 {
    for (products, 0..) |p, i| {
        if (std.mem.eql(u8, p, product)) return @intCast(i);
    }
    unreachable;
}

fn createSampleData(allocator: std.mem.Allocator) ![]SalesRecord {
    var records = std.ArrayList(SalesRecord).init(allocator);
    defer records.deinit();

    var i: usize = 0;
    while (i < TOTAL_RECORDS) : (i += 1) {
        const region = regions[i % regions.len];
        const product = products[i % products.len];
        const amount = 100.0 + 10.0 * @as(f64, @floatFromInt(i));
        const quantity = 1 + @as(i64, @intCast(i % 10));

        try records.append(SalesRecord{
            .region = region,
            .product = product,
            .amount = amount,
            .quantity = quantity,
        });
    }

    // Shuffle the records using Fisher-Yates algorithm
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();
    var n: usize = records.items.len;
    while (n > 1) {
        n -= 1;
        const m = random.intRangeAtMost(usize, 0, n);
        const temp = records.items[n];
        records.items[n] = records.items[m];
        records.items[m] = temp;
    }

    return records.toOwnedSlice();
}

test "ZigStructAggregation" {
    const allocator = testing.allocator;
    const sample_data = try createSampleData(allocator);
    defer allocator.free(sample_data);

    const result = aggregateSales(sample_data);

    // Verify results
    try testing.expectEqualStrings("West", result.region);
    try testing.expectEqualStrings("Keyboard", result.product);
    try testing.expectEqual(EXPECTED_TOTAL_SALES, result.agg.total_sales);
    try testing.expectEqual(EXPECTED_TOTAL_QUANTITY, result.agg.total_quantity);

    const avg_quantity = @as(f64, @floatFromInt(result.agg.total_quantity)) / @as(f64, @floatFromInt(result.agg.total_sales));
    try testing.expectApproxEqAbs(EXPECTED_AVG_QUANTITY, avg_quantity, 0.001);
}

test "BenchmarkZigStructAggregation" {
    const allocator = testing.allocator;
    const sample_data = try createSampleData(allocator);
    defer allocator.free(sample_data);

    const iterations = 20;
    var total_time: u64 = 0;
    var min_time: u64 = std.math.maxInt(u64);
    var max_time: u64 = 0;

    std.debug.print("Running benchmark with {d} million records...\n", .{TOTAL_RECORDS / 1_000_000});

    for (0..iterations) |i| {
        const start = std.time.microTimestamp();
        const result = aggregateSales(sample_data);
        const end = std.time.microTimestamp();
        const elapsed = @as(u64, @intCast(end - start));

        total_time += elapsed;
        min_time = @min(min_time, elapsed);
        max_time = @max(max_time, elapsed);

        // Verify results
        try testing.expectEqualStrings("West", result.region);
        try testing.expectEqualStrings("Keyboard", result.product);
        try testing.expectEqual(EXPECTED_TOTAL_SALES, result.agg.total_sales);
        try testing.expectEqual(EXPECTED_TOTAL_QUANTITY, result.agg.total_quantity);

        if (i % 5 == 0) {
            std.debug.print("Iteration {d}: {d}μs\n", .{ i + 1, elapsed });
        }
    }

    const avg_time = total_time / iterations;
    std.debug.print("\nBenchmark Results:\n", .{});
    std.debug.print("  Average: {d}μs ({d:.2}ms)\n", .{ avg_time, @as(f64, @floatFromInt(avg_time)) / 1000.0 });
    std.debug.print("  Min: {d}μs ({d:.2}ms)\n", .{ min_time, @as(f64, @floatFromInt(min_time)) / 1000.0 });
    std.debug.print("  Max: {d}μs ({d:.2}ms)\n", .{ max_time, @as(f64, @floatFromInt(max_time)) / 1000.0 });
    std.debug.print("  Throughput: {d:.2} records/μs\n", .{@as(f64, @floatFromInt(TOTAL_RECORDS)) / @as(f64, @floatFromInt(avg_time))});
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const sample_data = try createSampleData(allocator);
    defer allocator.free(sample_data);

    const result = aggregateSales(sample_data);
    std.info.print("Region: {s}, Product: {s}\n", .{ result.region, result.product });
    std.info.print("Total Sales: {d}\n", .{result.agg.total_sales});
    std.info.print("Total Amount: {d}\n", .{result.agg.total_amount});
    std.info.print("Total Quantity: {d}\n", .{result.agg.total_quantity});
}
