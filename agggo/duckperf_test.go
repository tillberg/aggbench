package agggo

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/tillberg/alog"
)

// GroupKey matches the Zig struct
type GroupKey struct {
	Region  string
	Product string
}

func createSampleRecords() arrow.Record {
	// Create Arrow schema matching the sales table
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "region", Type: arrow.BinaryTypes.String},
			{Name: "product", Type: arrow.BinaryTypes.String},
			{Name: "amount", Type: arrow.PrimitiveTypes.Float64},
			{Name: "quantity", Type: arrow.PrimitiveTypes.Int64},
		},
		nil,
	)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	regionBuilder := rb.Field(0).(*array.StringBuilder)
	productBuilder := rb.Field(1).(*array.StringBuilder)
	amountBuilder := rb.Field(2).(*array.Float64Builder)
	quantityBuilder := rb.Field(3).(*array.Int64Builder)

	// Build data directly into Arrow builders
	for i := 0; i < TOTAL_RECORDS; i++ {
		region := regions[i%len(regions)]
		product := products[i%len(products)]
		amount := 100.0 + 10.0*float64(i)
		quantity := 1 + int64(i%10)

		regionBuilder.Append(region)
		productBuilder.Append(product)
		amountBuilder.Append(amount)
		quantityBuilder.Append(quantity)
	}

	return rb.NewRecord()
}

func setupDatabase(db *sql.DB) error {
	// Create the sales table
	_, err := db.Exec(`
		CREATE TABLE sales (
			region VARCHAR,
			product VARCHAR,
			amount DOUBLE,
			quantity BIGINT
		)
	`)
	return err
}

func insertData(db *sql.DB, record arrow.Record) error {
	defer record.Release()
	timer := alog.NewTimer()
	// Write Arrow record directly to Parquet file
	tempFile := "/tmp/bulk_sales_data.parquet"

	// Create the parquet file
	file, err := os.Create(tempFile)
	if err != nil {
		return fmt.Errorf("failed to create parquet file: %w", err)
	}
	defer file.Close()
	defer os.Remove(tempFile)

	// Create Parquet writer
	writer, err := pqarrow.NewFileWriter(record.Schema(), file, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Write the Arrow record to Parquet
	err = writer.WriteBuffered(record)
	if err != nil {
		writer.Close()
		return fmt.Errorf("failed to write record to parquet: %w", err)
	}

	// Close the writer to flush data
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	// Close the file to ensure all data is written
	file.Close()

	// Use DuckDB's ability to read Parquet files directly
	query := fmt.Sprintf("INSERT INTO sales SELECT * FROM '%s'", tempFile)
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to execute INSERT FROM parquet: %w", err)
	}

	alog.Log("insertData took %s", timer.Elapsed())

	return nil
}

func aggregateSalesDuckDB(stmt *sql.Stmt) (*AggResult, *GroupKey, error) {
	var region, product string
	var totalSales int64
	var totalAmount float64
	var avgAmount float64
	var totalQuantity int64
	var avgQuantity float64

	err := stmt.QueryRow().Scan(&region, &product, &totalSales, &totalAmount, &avgAmount, &totalQuantity, &avgQuantity)
	if err != nil {
		return nil, nil, err
	}

	result := &AggResult{
		TotalSales:    totalSales,
		TotalAmount:   totalAmount,
		AvgAmount:     avgAmount,
		TotalQuantity: totalQuantity,
		AvgQuantity:   avgQuantity,
	}

	key := &GroupKey{
		Region:  region,
		Product: product,
	}

	return result, key, nil
}

func prepareDuckDBAggregation(db *sql.DB) *sql.Stmt {
	query := `
		SELECT 
			region,
			product,
			COUNT(*) as total_sales,
			SUM(amount) as total_amount,
			AVG(amount) as avg_amount,
			SUM(quantity) as total_quantity,
			AVG(quantity) as avg_quantity
		FROM sales
		GROUP BY region, product
		ORDER BY total_amount DESC
		LIMIT 1`
	stmt, err := db.Prepare(query)
	alog.BailIf(err)
	return stmt
}

func TestDuckDBAggregation(t *testing.T) {
	db := getDuckDBWithData()
	stmt := prepareDuckDBAggregation(db)
	defer stmt.Close()

	// Perform aggregation
	result, key, err := aggregateSalesDuckDB(stmt)
	alog.BailIf(err)

	// Verify results
	assert.Equal(t, key.Region, "West")
	assert.Equal(t, key.Product, "Keyboard")
	assert.EqualValues(t, result.TotalSales, EXPECTED_TOTAL_SALES)
	assert.EqualValues(t, result.TotalQuantity, EXPECTED_TOTAL_QUANTITY)

	avgQuantity := float64(result.TotalQuantity) / float64(result.TotalSales)
	assert.InDelta(t, avgQuantity, EXPECTED_AVG_QUANTITY, 0.001)
}

var duckDBWithData *sql.DB
var duckDBWithDataOnce sync.Once

func getDuckDBWithData() *sql.DB {
	duckDBWithDataOnce.Do(func() {
		db, err := sql.Open("duckdb", ":memory:")
		alog.BailIf(err)
		err = setupDatabase(db)
		alog.BailIf(err)
		sampleData := createSampleRecords()
		err = insertData(db, sampleData)
		alog.BailIf(err)
		db.Exec("SET threads TO 1;")
		duckDBWithData = db
	})
	return duckDBWithData
}

func BenchmarkDuckDBAggregation(b *testing.B) {
	db := getDuckDBWithData()
	stmt := prepareDuckDBAggregation(db)
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, key, err := aggregateSalesDuckDB(stmt)
		alog.BailIf(err)

		assert.Equal(b, key.Region, "West")
		assert.Equal(b, key.Product, "Keyboard")
		assert.EqualValues(b, result.TotalSales, EXPECTED_TOTAL_SALES)
		assert.EqualValues(b, result.TotalQuantity, EXPECTED_TOTAL_QUANTITY)
	}
}
