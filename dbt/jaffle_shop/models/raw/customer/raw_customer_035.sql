
-- raw_customer_035.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_1 as metric_3
FROM raw_source_customer_6
