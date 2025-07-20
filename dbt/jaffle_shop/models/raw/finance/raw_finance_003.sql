
-- raw_finance_003.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_4 as metric_1
FROM raw_source_finance_4
