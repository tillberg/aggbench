package agggo

// SalesRecord matches the Zig struct
type SalesRecord struct {
	Region   string
	Product  string
	Amount   float64
	Quantity int64
}

// AggResult matches the Zig struct
type AggResult struct {
	TotalSales    int64
	TotalAmount   float64
	AvgAmount     float64
	TotalQuantity int64
	AvgQuantity   float64
}

const (
	TOTAL_RECORDS           = 1_000_000
	EXPECTED_TOTAL_SALES    = TOTAL_RECORDS / 20
	EXPECTED_TOTAL_AMOUNT   = 2500950000.0
	EXPECTED_AVG_AMOUNT     = 500190.0
	EXPECTED_TOTAL_QUANTITY = TOTAL_RECORDS / 2
	EXPECTED_AVG_QUANTITY   = 10.0
)

var regions = []string{"North", "South", "East", "West"}
var products = []string{"Laptop", "Phone", "Tablet", "Monitor", "Keyboard"}
