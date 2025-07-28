package salesbench

import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit
import kotlin.math.abs

@State(Scope.Benchmark)
open class SalesAggregationBenchmark {
    data class SalesRecord(
        val region: String,
        val product: String,
        val amount: Double,
        val quantity: Long,
        val date: String
    )

    data class AggResult(
        var totalSales: Long = 0,
        var totalAmount: Double = 0.0,
        var totalQuantity: Long = 0
    )

    companion object {
        const val TOTAL_RECORDS = 1_000_000
        const val EXPECTED_TOTAL_SALES: Long = TOTAL_RECORDS / 20L
        const val EXPECTED_TOTAL_AMOUNT: Double = 2_500_950_000.0
        const val EXPECTED_AVG_AMOUNT: Double = 500_190.0
        const val EXPECTED_TOTAL_QUANTITY: Long = TOTAL_RECORDS / 2L
        const val EXPECTED_AVG_QUANTITY: Double = 10.0
        
        // Pre-computed lookup tables for ultra-fast character to index mapping
        private val REGION_LOOKUP = IntArray(256) { -1 }.apply {
            this['N'.code] = 0 // North
            this['S'.code] = 1 // South
            this['E'.code] = 2 // East
            this['W'.code] = 3 // West
        }
        
        private val PRODUCT_LOOKUP = IntArray(256) { -1 }.apply {
            this['L'.code] = 0 // Laptop
            this['P'.code] = 1 // Phone
            this['T'.code] = 2 // Tablet
            this['M'.code] = 3 // Monitor
            this['K'.code] = 4 // Keyboard
        }
        
    }

    lateinit var sampleData: List<SalesRecord>

    @Setup
    fun setup() {
        sampleData = createSampleData()
    }

    private fun createSampleData(): List<SalesRecord> {
        val regions = listOf("North", "South", "East", "West")
        val products = listOf("Laptop", "Phone", "Tablet", "Monitor", "Keyboard")
        var l = List(TOTAL_RECORDS) { i ->
            val region = regions[i % regions.size]
            val product = products[i % products.size]
            val amount = 100.0 + 10.0 * i
            val quantity = 1L + (i % 10)
            val date = String.format("2024-%02d-%02d", 1 + (i % 12), 1 + (i % 28))
            SalesRecord(region, product, amount, quantity, date)
        }
        // Shuffle the list
        l = l.shuffled()
        return l
    }

    private fun aggregateSales(records: List<SalesRecord>): Pair<Pair<String, String>, AggResult> {
        // Fixed arrays for regions and products
        val regions = arrayOf("North", "South", "East", "West")
        val products = arrayOf("Laptop", "Phone", "Tablet", "Monitor", "Keyboard")
        
        // Fixed-size array for all combinations (4 regions * 5 products = 20)
        val groups = Array(20) { AggResult() }
        
        for (r in records) {
            // Ultra-fast lookup using pre-computed table - single array access
            val regionIdx = REGION_LOOKUP[r.region[0].code]
            val productIdx = PRODUCT_LOOKUP[r.product[0].code]
            
            // Calculate array index: regionIdx * 5 + productIdx
            val idx = regionIdx * 5 + productIdx
            val agg = groups[idx]
            agg.totalSales += 1
            agg.totalAmount += r.amount
            agg.totalQuantity += r.quantity
        }
        
        // Find the group with the highest total_amount - manual loop for better performance
        var maxIdx = 0
        var maxAmount = groups[0].totalAmount
        
        for (i in 1 until groups.size) {
            if (groups[i].totalAmount > maxAmount) {
                maxAmount = groups[i].totalAmount
                maxIdx = i
            }
        }
        
        val regionIdx = maxIdx / 5
        val productIdx = maxIdx % 5
        
        return Pair(
            Pair(regions[regionIdx], products[productIdx]),
            groups[maxIdx]
        )
    }
    
    // Alternative optimized version using when expressions (cleaner but slightly slower)
    private fun aggregateSalesWhen(records: List<SalesRecord>): Pair<Pair<String, String>, AggResult> {
        val regions = arrayOf("North", "South", "East", "West")
        val products = arrayOf("Laptop", "Phone", "Tablet", "Monitor", "Keyboard")
        val groups = Array(20) { AggResult() }
        
        for (r in records) {
            val regionIdx = when (r.region[0]) {
                'N' -> 0; 'S' -> 1; 'E' -> 2; 'W' -> 3
                else -> throw IllegalArgumentException("Invalid region: ${r.region}")
            }
            
            val productIdx = when (r.product[0]) {
                'L' -> 0; 'P' -> 1; 'T' -> 2; 'M' -> 3; 'K' -> 4
                else -> throw IllegalArgumentException("Invalid product: ${r.product}")
            }
            
            val idx = regionIdx * 5 + productIdx
            groups[idx].run {
                totalSales += 1
                totalAmount += r.amount
                totalQuantity += r.quantity
            }
        }
        
        val maxIdx = groups.indices.maxByOrNull { groups[it].totalAmount }!!
        val regionIdx = maxIdx / 5
        val productIdx = maxIdx % 5
        
        return Pair(
            Pair(regions[regionIdx], products[productIdx]),
            groups[maxIdx]
        )
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    fun kotlinStructAggregation() {
        val (key, agg) = aggregateSales(sampleData)
        val (region, product) = key
        val avgQuantity = agg.totalQuantity.toDouble() / agg.totalSales

        check(region == "West") { "region: $region" }
        check(product == "Keyboard") { "product: $product" }
        check(agg.totalSales == EXPECTED_TOTAL_SALES) { "totalSales: ${agg.totalSales}" }
        check(agg.totalQuantity == EXPECTED_TOTAL_QUANTITY) { "totalQuantity: ${agg.totalQuantity}" }
        check(abs(avgQuantity - EXPECTED_AVG_QUANTITY) < 1e-6) { "avgQuantity: $avgQuantity" }
    }
    
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    fun kotlinStructAggregationWhen() {
        val (key, agg) = aggregateSalesWhen(sampleData)
        val (region, product) = key
        val avgQuantity = agg.totalQuantity.toDouble() / agg.totalSales

        check(region == "West") { "region: $region" }
        check(product == "Keyboard") { "product: $product" }
        check(agg.totalSales == EXPECTED_TOTAL_SALES) { "totalSales: ${agg.totalSales}" }
        check(agg.totalQuantity == EXPECTED_TOTAL_QUANTITY) { "totalQuantity: ${agg.totalQuantity}" }
        check(abs(avgQuantity - EXPECTED_AVG_QUANTITY) < 1e-6) { "avgQuantity: $avgQuantity" }
    }
}
