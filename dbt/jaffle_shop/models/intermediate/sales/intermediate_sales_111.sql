
-- intermediate_sales_111.sql
-- Intermediate model

WITH source_data AS (
    SELECT id, name, created_at, total_metric_1, avg_metric_2 FROM {{ ref('staging_customer_043') }} UNION ALL SELECT id, name, created_at, total_metric_1, avg_metric_2 FROM {{ ref('staging_sales_066') }} UNION ALL SELECT id, name, created_at, total_metric_1, avg_metric_2 FROM {{ ref('staging_product_072') }}
)

SELECT
    id,
    name,
    created_at,
    SUM(total_metric_1) as enhanced_metric_1,
    AVG(avg_metric_2) as enhanced_metric_2,
    COUNT(*) as record_count
FROM source_data
GROUP BY 1, 2, 3
