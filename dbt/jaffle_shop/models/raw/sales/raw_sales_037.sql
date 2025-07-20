
-- raw_sales_037.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_3 as metric_2
FROM raw_source_sales_8
