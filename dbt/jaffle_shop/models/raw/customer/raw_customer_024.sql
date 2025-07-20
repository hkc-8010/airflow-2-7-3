
-- raw_customer_024.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_5 as metric_1
FROM raw_source_customer_5
