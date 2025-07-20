
-- raw_customer_022.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_3 as metric_2
FROM raw_source_customer_3
