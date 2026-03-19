# Sample Outputs

## Curated Output (Silver Layer)
The curated dataset is written as Parquet and partitioned by `order_date`.

Example partition structure:

- data/curated/order_date=2026-03-01/
- data/curated/order_date=2026-03-02/
- data/curated/order_date=2026-03-03/

## Rejected Output
Invalid records are written to `data/rejected/` as CSV files.

Examples of rejected rows:
- Negative quantity (`INVALID_QUANTITY`)
- Zero or negative unit price (`INVALID_UNIT_PRICE`)

## Pipeline Metrics
The PySpark transformation prints:
- raw row count
- curated row count
- rejected row count
