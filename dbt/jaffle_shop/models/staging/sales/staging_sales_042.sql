
-- staging_sales_042.sql
-- Staging model

WITH source_data AS (
    SELECT id, name, created_at, metric_1, metric_2 FROM {{ ref('raw_sales_025') }}
)

SELECT
    id,
    name,
    created_at,
    SUM(metric_1) as total_metric_1,
    AVG(metric_2) as avg_metric_2
FROM source_data
GROUP BY 1, 2, 3
