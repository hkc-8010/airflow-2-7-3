
-- raw_finance_031.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_2 as metric_2
FROM raw_source_finance_2
