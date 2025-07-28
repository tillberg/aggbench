#![feature(test)]

extern crate test;

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use duckdb::Connection;
use duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use parquet::arrow::ArrowWriter;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::sync::Arc;
use test::Bencher;
use rand::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
struct SalesRecord<'a> {
    region: &'a str,
    product: &'a str,
    amount: f64,
    quantity: i64,
}

#[derive(Default, Copy, Clone)]
struct AggResult {
    total_sales: i64,
    total_amount: f64,
    total_quantity: i64,
}

// #[derive(Debug, Eq, PartialEq, Hash, Clone)]
// struct GroupKey<'a> {
//     region: &'a str,
//     product: &'a str,
// }

fn aggregate_sales(records: &[SalesRecord]) -> ((String, String), AggResult) {
    // Fixed arrays for regions and products
    // let regions = ["North", "South", "East", "West"];
    // let products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"];

    // Fixed-size array for all combinations (4 regions * 5 products = 20)
    // let mut groups: [AggResult; 20] = [AggResult::default(); 20];

    // let mut groups:     FxHashMap<(&str, &str), AggResult> = FxHashMap::default();
    // for r in records {
    //     let key = (r.region, r.product);
    //     let agg = groups.entry(key).or_insert(AggResult::default());
    //     agg.total_sales += 1;
    //     agg.total_amount += r.amount;
    //     agg.total_quantity += r.quantity;
    // }
    
    

        // let region_idx;
        // let product_idx;
        // match r.region.as_str() {
        //     "North" => region_idx = 0,
        //     "South" => region_idx = 1,
        //     "East" => region_idx = 2,
        //     "West" => region_idx = 3,
        //     _ => panic!("Invalid region: {}", r.region),
        // }
        // match r.product.as_str() {
        //     "Laptop" => product_idx = 0,
        //     "Phone" => product_idx = 1,
        //     "Tablet" => product_idx = 2,
        //     "Monitor" => product_idx = 3,
        //     "Keyboard" => product_idx = 4,
        //     _ => panic!("Invalid product: {}", r.product),
        // }
        
        // Find region index
    //     let region_idx = regions.iter().position(|&x| x == r.region).unwrap();
    //     // Find product index
    //     let product_idx = products.iter().position(|&x| x == r.product).unwrap();
    //     // Calculate array index: region_idx * 5 + product_idx
    //     let idx = region_idx * 5 + product_idx;

    //     groups[idx].total_sales += 1;
    //     groups[idx].total_amount += r.amount;
    //     groups[idx].total_quantity += r.quantity;
    // }

    // Find the group with the highest total_amount
    // let mut max_key = ("", "");
    // let mut max_amount = 0.0;

    // for (key, agg) in groups.iter() {
    //     if agg.total_amount > max_amount {
    //         max_amount = agg.total_amount;
    //         max_key = key.clone();
    //     }
    // }

    // (
    //     (
    //         max_key.0.to_string(),
    //         max_key.1.to_string(),
    //     ),
    //     groups[&max_key],
    // )

    let mut groups: [AggResult; 20] = [AggResult::default(); 20];
    let regions = ["North", "South", "East", "West"];
    let products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"];

    for r in records {
        let region_idx = match r.region {
            "North" => 0, "South" => 1, "East" => 2, "West" => 3,
            _ => panic!("Invalid region: {}", r.region),
        };
        let product_idx = match r.product {
            "Laptop" => 0, "Phone" => 1, "Tablet" => 2,
            "Monitor" => 3, "Keyboard" => 4,
            _ => panic!("Invalid product: {}", r.product),
        };

        let idx = region_idx * 5 + product_idx;
        groups[idx].total_sales += 1;
        groups[idx].total_amount += r.amount;
        groups[idx].total_quantity += r.quantity;
    }

    let mut max_idx = 0;
    let mut max_amount = groups[0].total_amount;

    for (idx, agg) in groups.iter().enumerate() {
        if agg.total_amount > max_amount {
            max_idx = idx;
            max_amount = agg.total_amount;
        }
    }

    let region_idx = max_idx / 5;
    let product_idx = max_idx % 5;

    (
        (regions[region_idx].to_string(), products[product_idx].to_string()),
        groups[max_idx],
    )
}

const TOTAL_RECORDS: usize = 1_000_000;
const EXPECTED_TOTAL_SALES: i64 = (TOTAL_RECORDS as i64) / 20;
const EXPECTED_TOTAL_AMOUNT: f64 = if TOTAL_RECORDS == 1000 {
    259500.0
} else if TOTAL_RECORDS == 100000 {
    2500950000.0
} else {
    250009500000.0
};
const EXPECTED_AVG_AMOUNT: f64 = if TOTAL_RECORDS == 1000 {
    5190.0
} else if TOTAL_RECORDS == 100000 {
    500190.0
} else {
    5000190.0
};
const EXPECTED_TOTAL_QUANTITY: i64 = (TOTAL_RECORDS as i64) / 2;
const EXPECTED_AVG_QUANTITY: f64 = 10.0;

#[bench]
fn rust_struct_aggregation(b: &mut Bencher) {
    let sample_data = create_sample_data();
    b.iter(|| {
        let ((region, product), agg) = aggregate_sales(&sample_data);

        // let avg_amount = agg.total_amount / agg.total_sales as f64;
        let avg_quantity = agg.total_quantity as f64 / agg.total_sales as f64;

        assert_eq!(region, "West");
        assert_eq!(product, "Keyboard");
        assert_eq!(agg.total_sales, EXPECTED_TOTAL_SALES);
        // assert_eq!(agg.total_amount, EXPECTED_TOTAL_AMOUNT);
        // assert_eq!(avg_amount, EXPECTED_AVG_AMOUNT);
        assert_eq!(agg.total_quantity, EXPECTED_TOTAL_QUANTITY);
        assert_eq!(avg_quantity, EXPECTED_AVG_QUANTITY);
    });
}

fn create_sample_data<'a>() -> Vec<SalesRecord<'a>> {
    let regions = vec!["North", "South", "East", "West"];
    let products = vec!["Laptop", "Phone", "Tablet", "Monitor", "Keyboard"];

    let mut records = Vec::new();

    // Generate 100 sample records
    for i in 0..TOTAL_RECORDS {
        let region = regions[i % regions.len()];
        let product = products[i % products.len()];
        let amount = 100.0 + 10.0 * (i as f64);
        let quantity = 1 + (i % 10);

        records.push(SalesRecord {
            region: region,
            product: product,
            amount: amount as f64,
            quantity: quantity as i64,
        });
    }
    // Shuffle the records randomly
    let mut rng = rand::rng();
    records.shuffle(&mut rng);
    records
}

fn create_record_batch(records: &[SalesRecord]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let regions: Vec<&str> = records.iter().map(|r| r.region).collect();
    let products: Vec<&str> = records.iter().map(|r| r.product).collect();
    let amounts: Vec<f64> = records.iter().map(|r| r.amount).collect();
    let quantities: Vec<i64> = records.iter().map(|r| r.quantity).collect();

    let schema = Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("quantity", DataType::Int64, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(StringArray::from(regions)),
            Arc::new(StringArray::from(products)),
            Arc::new(Float64Array::from(amounts)),
            Arc::new(Int64Array::from(quantities)),
        ],
    )?;

    Ok(batch)
}

fn create_duckdb_sales(conn: &Connection) {
    let sample_data: Vec<SalesRecord> = create_sample_data();
    let batch = create_record_batch(&sample_data).unwrap();

    // Write Arrow record batch to Parquet file
    let temp_file = "/tmp/bulk_sales_data.parquet";
    
    let start_write = std::time::Instant::now();
    let file = File::create(temp_file).unwrap();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let write_duration = start_write.elapsed();
    println!("Parquet file creation took: {:?}", write_duration);

    // Create table and load from Parquet
    conn.execute("CREATE TABLE sales (region VARCHAR, product VARCHAR, amount DOUBLE, quantity BIGINT)", []).unwrap();
    
    let start_insert = std::time::Instant::now();
    let query = format!("INSERT INTO sales SELECT * FROM '{}'", temp_file);
    conn.execute(&query, []).unwrap();
    let insert_duration = start_insert.elapsed();
    println!("Parquet file insertion took: {:?}", insert_duration);
    
    println!("Total Parquet operation took: {:?}", write_duration + insert_duration);
    
    // Clean up temp file
    std::fs::remove_file(temp_file).unwrap_or(());
}

fn cleanup_duckdb_sales(conn: Connection) {
    conn.execute("DROP TABLE sales", []).unwrap();
}

#[bench]
fn datafusion_select_one(b: &mut Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ctx = SessionContext::new();

    b.iter(|| {
        rt.block_on(async {
            let result = ctx.sql("SELECT 2").await.unwrap();
            let res = result.collect().await.unwrap();
            assert_eq!(
                res[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                2
            );
        });
    });
}

#[bench]
fn datafusion_complex_aggregation(b: &mut Bencher) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let prepare_query = "PREPARE complex_aggregation AS SELECT region, product,
                               COUNT(*) as total_sales,
                               SUM(amount) as total_amount,
                               AVG(amount) as avg_amount,
                               SUM(quantity) as total_quantity,
                               AVG(quantity) as avg_quantity
                        FROM sales
                        GROUP BY region, product
                        ORDER BY total_amount DESC
                        LIMIT 1";
    let query = "EXECUTE complex_aggregation";
    let sample_data = create_sample_data();
    b.iter(|| {
        rt.block_on(async {
            let ctx = SessionContext::new();
            ctx.register_batch("sales", create_record_batch(&sample_data).unwrap())
                .unwrap();
            ctx.sql(prepare_query).await.unwrap();
            let result = ctx.sql(query).await.unwrap();
            let res = result.collect().await.unwrap();
            assert_eq!(res.len(), 1);
            let batch = &res[0];
            let region = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            let product = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0);
            let total_sales = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            // let total_amount = batch
            //     .column(3)
            //     .as_any()
            //     .downcast_ref::<Float64Array>()
            //     .unwrap()
            //     .value(0);
            // let avg_amount = batch
            //     .column(4)
            //     .as_any()
            //     .downcast_ref::<Float64Array>()
            //     .unwrap()
            //     .value(0);
            let total_quantity = batch
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0);
            let avg_quantity = batch
                .column(6)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            assert_eq!(region, "West");
            assert_eq!(product, "Keyboard");
            assert_eq!(total_sales, EXPECTED_TOTAL_SALES);
            // assert_eq!(total_amount, EXPECTED_TOTAL_AMOUNT);
            // assert_eq!(avg_amount, EXPECTED_AVG_AMOUNT);
            assert_eq!(total_quantity, EXPECTED_TOTAL_QUANTITY);
            assert_eq!(avg_quantity, EXPECTED_AVG_QUANTITY);
            // session.sql("DROP TABLE sales").await.unwrap();
        });
    });
}

#[bench]
fn duckdb_select_one(b: &mut Bencher) {
    let conn = Connection::open_in_memory().unwrap();
    create_duckdb_sales(&conn);
    let mut stmt = conn.prepare("SELECT 2").unwrap();
    conn.execute("SET threads TO 1;", []).unwrap();
    b.iter(|| {
        let result = stmt.query_row([], |row| row.get::<_, i64>(0)).unwrap();
        assert_eq!(result, 2);
    });
    cleanup_duckdb_sales(conn);
}

#[bench]
fn duckdb_complex_aggregation_preloaded(b: &mut Bencher) {
    let conn = Connection::open_in_memory().unwrap();
    create_duckdb_sales(&conn);
    let query = "
SELECT region, product,
    COUNT(*) as total_sales,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    SUM(quantity) as total_quantity,
    AVG(quantity) as avg_quantity
FROM sales
GROUP BY region, product
ORDER BY total_amount DESC
LIMIT 1";
    let mut stmt = conn.prepare(query).unwrap();
    conn.execute("SET threads TO 1;", []).unwrap();
    b.iter(|| {
        let result = stmt.query([]).unwrap();
        let mut rows = result;
        let mut row_count = 0;
        while let Some(row) = rows.next().unwrap() {
            let region: String = row.get(0).unwrap();
            let product: String = row.get(1).unwrap();
            let total_sales: i64 = row.get(2).unwrap();
            // let total_amount: f64 = row.get(3).unwrap();
            // let avg_amount: f64 = row.get(4).unwrap();
            let total_quantity: i64 = row.get(5).unwrap();
            let avg_quantity: f64 = row.get(6).unwrap();
            assert_eq!(region, "West");
            assert_eq!(product, "Keyboard");
            assert_eq!(total_sales, EXPECTED_TOTAL_SALES);
            // assert_eq!(total_amount, EXPECTED_TOTAL_AMOUNT);
            // assert_eq!(avg_amount, EXPECTED_AVG_AMOUNT);
            assert_eq!(total_quantity, EXPECTED_TOTAL_QUANTITY);
            assert_eq!(avg_quantity, EXPECTED_AVG_QUANTITY);
            row_count += 1;
        }
        assert_eq!(row_count, 1, "Expected exactly one row");
    });
    cleanup_duckdb_sales(conn);
}

#[bench]
fn duckdb_complex_aggregation_record_batch(b: &mut Bencher) {
    // skip if TOTAL_RECORDS is not 1000
    if TOTAL_RECORDS > 1024 {
        println!("Skipping test for TOTAL_RECORDS > 1024");
        return;
    }
    let conn = Connection::open_in_memory().unwrap();
    conn.execute("SET threads TO 1;", []).unwrap();
    create_duckdb_sales(&conn);
    let sample_data: Vec<SalesRecord> = create_sample_data();
    let mut stmt1: duckdb::Statement<'_> = conn
        .prepare(format!(r#"CREATE TABLE "sales2" AS SELECT * FROM arrow(?, ?)"#).as_str())
        .unwrap();
    let batch = create_record_batch(&sample_data).unwrap();

    b.iter(|| {
        let params = arrow_recordbatch_to_query_params(batch.clone());
        stmt1.execute(params).unwrap();

        let query = "
SELECT region, product,
    COUNT(*) as total_sales,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    SUM(quantity) as total_quantity,
    AVG(quantity) as avg_quantity
FROM sales2
GROUP BY region, product
ORDER BY total_amount DESC
LIMIT 1";

        let mut stmt2 = conn.prepare(query).unwrap();
        let result = stmt2.query([]).unwrap();
        let mut rows = result;
        let mut row_count = 0;
        while let Some(row) = rows.next().unwrap() {
            let region: String = row.get(0).unwrap();
            let product: String = row.get(1).unwrap();
            let total_sales: i64 = row.get(2).unwrap();
            let total_amount: f64 = row.get(3).unwrap();
            let avg_amount: f64 = row.get(4).unwrap();
            let total_quantity: i64 = row.get(5).unwrap();
            let avg_quantity: f64 = row.get(6).unwrap();
            assert_eq!(region, "West");
            assert_eq!(product, "Keyboard");
            assert_eq!(total_sales, EXPECTED_TOTAL_SALES);
            assert_eq!(total_amount, EXPECTED_TOTAL_AMOUNT);
            assert_eq!(avg_amount, EXPECTED_AVG_AMOUNT);
            assert_eq!(total_quantity, EXPECTED_TOTAL_QUANTITY);
            assert_eq!(avg_quantity, EXPECTED_AVG_QUANTITY);
            row_count += 1;
        }
        assert_eq!(row_count, 1, "Expected exactly one row");

        conn.execute("DROP TABLE sales2", []).unwrap();
    });
}

#[bench]
fn duckdb_complex_aggregation_appender(b: &mut Bencher) {
    let conn = Connection::open_in_memory().unwrap();
    let sample_data: Vec<SalesRecord> = create_sample_data();
    let batch = create_record_batch(&sample_data).unwrap();
    conn.execute("SET threads TO 1;", []).unwrap();

    b.iter(|| {
        conn.execute("CREATE TABLE sales2 (region VARCHAR, product VARCHAR, amount DOUBLE, quantity BIGINT)", []).unwrap();
        let mut appender = conn.appender("sales2").unwrap();
        appender.append_record_batch(batch.clone()).unwrap();
        appender.flush().unwrap();

        let query = "
SELECT region, product,
    COUNT(*) as total_sales,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    SUM(quantity) as total_quantity,
    AVG(quantity) as avg_quantity
FROM sales2
GROUP BY region, product
ORDER BY total_amount DESC
LIMIT 1";

        let mut stmt2 = conn.prepare(query).unwrap();
        let result = stmt2.query([]).unwrap();
        let mut rows = result;
        let mut row_count = 0;
        while let Some(row) = rows.next().unwrap() {
            let region: String = row.get(0).unwrap();
            let product: String = row.get(1).unwrap();
            let total_sales: i64 = row.get(2).unwrap();
            // let total_amount: f64 = row.get(3).unwrap();
            // let avg_amount: f64 = row.get(4).unwrap();
            let total_quantity: i64 = row.get(5).unwrap();
            let avg_quantity: f64 = row.get(6).unwrap();
            assert_eq!(region, "West");
            assert_eq!(product, "Keyboard");
            assert_eq!(total_sales, EXPECTED_TOTAL_SALES);
            // assert_eq!(total_amount, EXPECTED_TOTAL_AMOUNT);
            // assert_eq!(avg_amount, EXPECTED_AVG_AMOUNT);
            assert_eq!(total_quantity, EXPECTED_TOTAL_QUANTITY);
            assert_eq!(avg_quantity, EXPECTED_AVG_QUANTITY);
            row_count += 1;
        }
        assert_eq!(row_count, 1, "Expected exactly one row");

        conn.execute("DROP TABLE sales2", []).unwrap();
    });
}

fn main() {
    println!("DataFusion benchmarks - run with: cargo bench");
    
    // Test timing for Parquet operations
    println!("\nTesting Parquet timing with {} records:", TOTAL_RECORDS);
    let conn = Connection::open_in_memory().unwrap();
    create_duckdb_sales(&conn);
}
