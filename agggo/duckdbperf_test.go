package agggo

import (
	"database/sql"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// BenchmarkSelect1SingleQuery benchmarks a single SELECT 1 query
func BenchmarkSelect1SingleQuery(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if result != 1 {
			b.Fatalf("Expected 1, got %d", result)
		}
	}
}

// BenchmarkSelect1PreparedStatement benchmarks SELECT 1 using prepared statements
func BenchmarkSelect1PreparedStatement(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		b.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result int
		res := stmt.QueryRow()
		err = res.Scan(&result)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
		if result != 1 {
			b.Fatalf("Expected 1, got %d", result)
		}
	}
}

// BenchmarkSelect1Concurrent benchmarks concurrent SELECT 1 queries
func BenchmarkSelect1Concurrent(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set max open connections
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var result int
			err := db.QueryRow("SELECT 1").Scan(&result)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			if result != 1 {
				b.Fatalf("Expected 1, got %d", result)
			}
		}
	})
}

// BenchmarkSelect1PreparedConcurrent benchmarks concurrent prepared SELECT 1 queries
func BenchmarkSelect1PreparedConcurrent(b *testing.B) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Set max open connections
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		b.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var result int
			err := stmt.QueryRow().Scan(&result)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
			if result != 1 {
				b.Fatalf("Expected 1, got %d", result)
			}
		}
	})
}

// TestSelect1Basic tests basic SELECT 1 functionality
func TestSelect1Basic(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	var result int
	err = db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result != 1 {
		t.Fatalf("Expected 1, got %d", result)
	}
}
