
-- mart_finance_174.sql
-- Mart model

WITH source_data AS (
    SELECT id, name, created_at, enhanced_metric_1, enhanced_metric_2, record_count FROM {{ ref('intermediate_marketing_156') }} UNION ALL SELECT id, name, created_at, enhanced_metric_1, enhanced_metric_2, record_count FROM {{ ref('intermediate_marketing_123') }}
)

SELECT
    name,
    MAX(created_at) as latest_update,
    SUM(enhanced_metric_1) as total_metric_1,
    AVG(enhanced_metric_2) as avg_metric_2,
    SUM(record_count) as total_records
FROM source_data
GROUP BY 1
