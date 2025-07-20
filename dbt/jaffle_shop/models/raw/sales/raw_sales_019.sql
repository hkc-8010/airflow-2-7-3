
-- raw_sales_019.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_5 as metric_2
FROM raw_source_sales_10
