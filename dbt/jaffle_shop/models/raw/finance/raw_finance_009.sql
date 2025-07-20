
-- raw_finance_009.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_5 as metric_1
FROM raw_source_finance_10
