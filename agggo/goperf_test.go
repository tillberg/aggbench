package agggo

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func aggregateSalesStructs(records []SalesRecord) (region, product string, agg AggResult) {
	groups := make(map[[2]string]*AggResult)
	for _, r := range records {
		key := [2]string{r.Region, r.Product}
		g, ok := groups[key]
		if !ok {
			g = &AggResult{}
			groups[key] = g
		}
		g.TotalSales++
		g.TotalAmount += r.Amount
		g.TotalQuantity += r.Quantity
	}
	for _, v := range groups {
		v.AvgAmount = v.TotalAmount / float64(v.TotalSales)
		v.AvgQuantity = float64(v.TotalQuantity) / float64(v.TotalSales)
	}

	// Find the group with the highest total_amount
	var maxKey [2]string
	var maxAgg *AggResult
	for k, v := range groups {
		if maxAgg == nil || v.TotalAmount > maxAgg.TotalAmount {
			maxKey = k
			maxAgg = v
		}
	}
	return maxKey[0], maxKey[1], *maxAgg
}

func aggregateSalesStructsTuned(records []SalesRecord) (region, product string, agg AggResult) {
	// Use fixed-size array for known combinations (4 regions * 5 products = 20)
	var groups [20]*AggResult
	regionNames := []string{"North", "South", "East", "West"}
	productNames := []string{"Laptop", "Phone", "Tablet", "Monitor", "Keyboard"}

	for _, r := range records {
		// Inline the index calculation to avoid function call overhead
		var regionIdx, productIdx int
		switch r.Region {
		case "North":
			regionIdx = 0
		case "South":
			regionIdx = 1
		case "East":
			regionIdx = 2
		case "West":
			regionIdx = 3
		default:
			panic(fmt.Sprintf("Invalid region: %s", r.Region))
		}

		switch r.Product {
		case "Laptop":
			productIdx = 0
		case "Phone":
			productIdx = 1
		case "Tablet":
			productIdx = 2
		case "Monitor":
			productIdx = 3
		case "Keyboard":
			productIdx = 4
		default:
			panic(fmt.Sprintf("Invalid product: %s", r.Product))
		}

		idx := regionIdx*5 + productIdx

		if groups[idx] == nil {
			groups[idx] = &AggResult{}
		}
		g := groups[idx]
		g.TotalSales++
		g.TotalAmount += r.Amount
		g.TotalQuantity += r.Quantity
	}

	// Find max and calculate averages
	var maxIdx int = -1
	var maxAmount float64
	for i, g := range groups {
		if g != nil {
			g.AvgAmount = g.TotalAmount / float64(g.TotalSales)
			g.AvgQuantity = float64(g.TotalQuantity) / float64(g.TotalSales)

			if maxIdx == -1 || g.TotalAmount > maxAmount {
				maxIdx = i
				maxAmount = g.TotalAmount
			}
		}
	}

	regionIdx := maxIdx / 5
	productIdx := maxIdx % 5
	return regionNames[regionIdx], productNames[productIdx], *groups[maxIdx]
}

func createSampleStructs() []SalesRecord {
	records := make([]SalesRecord, 0, TOTAL_RECORDS)
	for i := 0; i < TOTAL_RECORDS; i++ {
		region := regions[i%len(regions)]
		product := products[i%len(products)]
		amount := 100.0 + 10.0*float64(i)
		quantity := int64(1 + (i % 10))
		records = append(records, SalesRecord{
			Region:   region,
			Product:  product,
			Amount:   amount,
			Quantity: quantity,
		})
	}
	rand.Shuffle(len(records), func(i, j int) {
		records[i], records[j] = records[j], records[i]
	})
	return records
}

func BenchmarkGoStructAggregation(b *testing.B) {
	sampleData := createSampleStructs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region, product, agg := aggregateSalesStructs(sampleData)
		validateAggResult(b, region, product, agg)
	}
}

func BenchmarkGoStructAggregationTuned(b *testing.B) {
	sampleData := createSampleStructs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region, product, agg := aggregateSalesStructsTuned(sampleData)
		validateAggResult(b, region, product, agg)
	}
}

func validateAggResult(b *testing.B, region, product string, agg AggResult) {
	assert.Equal(b, region, "West")
	assert.Equal(b, product, "Keyboard")
	assert.EqualValues(b, agg.TotalSales, EXPECTED_TOTAL_SALES)
	assert.EqualValues(b, agg.TotalQuantity, EXPECTED_TOTAL_QUANTITY)
	assert.EqualValues(b, agg.AvgQuantity, EXPECTED_AVG_QUANTITY)
}
