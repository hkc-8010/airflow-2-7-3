
-- raw_product_006.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_2 as metric_1
FROM raw_source_product_7
