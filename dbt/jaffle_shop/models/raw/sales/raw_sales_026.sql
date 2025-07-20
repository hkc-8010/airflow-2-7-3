
-- raw_sales_026.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_2 as metric_3
FROM raw_source_sales_7
