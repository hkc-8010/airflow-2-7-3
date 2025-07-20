
-- raw_product_010.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_1 as metric_2
FROM raw_source_product_1
