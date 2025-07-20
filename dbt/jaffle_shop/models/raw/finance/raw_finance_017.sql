
-- raw_finance_017.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_3 as metric_3
FROM raw_source_finance_8
