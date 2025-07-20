
-- raw_finance_013.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_4 as metric_2
FROM raw_source_finance_4
