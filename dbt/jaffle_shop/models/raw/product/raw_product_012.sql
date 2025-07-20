
-- raw_product_012.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_3 as metric_1
FROM raw_source_product_3
