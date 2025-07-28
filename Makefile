
bench-go:
	cd agggo && go test -bench=. -benchmem

bench-clickhouse:
	echo "to benchmark clickhouse, paste aggclickhouse/clickhouse.sql into clickhouse-client"

bench-rust:
	# this requires the nightly rust toolchain
	cd aggrust && cargo bench --timings

bench-kotlin:
	cd aggkotlin && ./gradlew jmh

bench-zig:
	cd aggzig && zig build run -Doptimize=ReleaseFast
	cd aggzig && zig test src/sales.zig -O ReleaseFast

bench-all: bench-go bench-clickhouse bench-rust bench-kotlin bench-zig
