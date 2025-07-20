
-- raw_customer_007.sql
-- Raw data extraction

SELECT
    id,
    name,
    created_at,
    updated_at,
    value_3 as metric_2
FROM {{ source('customer', 'raw_source_customer_8') }}

