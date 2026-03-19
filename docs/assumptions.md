# Assumptions

- Each CSV file represents a daily batch of retail orders.
- `order_id` is treated as the business key for deduplication.
- Rows with null `order_id` are invalid.
- Rows with `quantity <= 0` are invalid.
- Rows with `unit_price <= 0` are invalid.
- Curated output is partitioned by `order_date` for query efficiency.
- Raw data is preserved unchanged in the Bronze layer.
