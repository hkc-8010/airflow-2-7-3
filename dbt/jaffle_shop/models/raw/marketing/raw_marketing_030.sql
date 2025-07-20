
-- raw_marketing_030.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_1 as metric_1
FROM raw_source_marketing_1
