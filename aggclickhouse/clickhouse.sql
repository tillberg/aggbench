DROP TABLE IF EXISTS aggbench_test_sales;

CREATE TABLE IF NOT EXISTS aggbench_test_sales
(
    region LowCardinality(String),
    product LowCardinality(String),
    amount Float64,
    quantity Int64
) ENGINE = Memory;

INSERT INTO aggbench_test_sales
SELECT
    arrayElement(['North', 'South', 'East', 'West'], (rand() % 4) + 1) AS region,
    arrayElement(['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard'], (rand() % 5) + 1) AS product,
    100.0 + 10.0 * number AS amount,
    1 + (number % 10) AS quantity
FROM numbers(1000000);

SELECT region, product,
    COUNT() as total_sales,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    SUM(quantity) as total_quantity,
    AVG(quantity) as avg_quantity
FROM aggbench_test_sales
GROUP BY region, product
ORDER BY total_amount DESC
LIMIT 1 settings max_threads=1;
