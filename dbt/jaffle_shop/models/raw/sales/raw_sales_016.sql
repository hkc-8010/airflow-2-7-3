
-- raw_sales_016.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_2 as metric_2
FROM raw_source_sales_7
