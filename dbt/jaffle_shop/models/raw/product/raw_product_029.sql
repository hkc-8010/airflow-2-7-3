
-- raw_product_029.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_5 as metric_3
FROM raw_source_product_10
