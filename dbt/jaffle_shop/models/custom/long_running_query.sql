{{
    config(
        materialized='table',
        tags=['long_running']
    )
}}

/*
This model is designed to run for approximately 15 minutes on Snowflake.
It uses Snowflake's SYSTEM$WAIT function to create a controlled delay, combined with
complex operations that require significant processing time.

The query:
1. Creates a large dataset using generator functions
2. Performs multiple complex calculations and transformations
3. Uses explicit wait commands to ensure minimum runtime

Note: Actual runtime may vary based on Snowflake warehouse size and workload.
*/

-- Set variables for our long-running query
{% set number_of_iterations = 50 %}
{% set wait_time_per_iteration = 18 %}  -- seconds

WITH 
-- Generate a large dataset (millions of rows)
large_dataset AS (
    SELECT 
        seq4() as id,
        uniform(1, 1000000, random(42)) as random_value1,
        uniform(1, 1000000, random(43)) as random_value2,
        uniform(1, 100, random(44)) as group_id,
        random() as random_factor
    FROM table(generator(rowcount => 5000000))
),

-- First computation layer with wait
computation_1 AS (
    SELECT 
        id,
        random_value1,
        random_value2,
        group_id,
        SQRT(POW(random_value1, 2) + POW(random_value2, 2)) as vector_length,
        SIN(random_value1 / 1000) * COS(random_value2 / 1000) as trig_result,
        SYSTEM$WAIT({{ wait_time_per_iteration }}) as wait_result  -- explicit wait
    FROM large_dataset
),

-- Aggregate by groups with additional computation
grouped_data AS (
    SELECT 
        group_id,
        AVG(vector_length) as avg_length,
        STDDEV(vector_length) as std_length,
        SUM(trig_result) as sum_trig,
        COUNT(*) as group_count,
        MIN(random_value1) as min_val1,
        MAX(random_value2) as max_val2
    FROM computation_1
    GROUP BY group_id
),

-- Join back with original data for more complex operations
joined_data AS (
    SELECT 
        c.id,
        c.random_value1,
        c.random_value2,
        c.group_id,
        c.vector_length,
        c.trig_result,
        g.avg_length,
        g.std_length,
        g.sum_trig,
        g.group_count,
        (c.vector_length - g.avg_length) / NULLIF(g.std_length, 0) as z_score,
        SYSTEM$WAIT({{ wait_time_per_iteration }}) as wait_result  -- explicit wait
    FROM computation_1 c
    JOIN grouped_data g ON c.group_id = g.group_id
),

-- Additional iterations of processing to extend runtime
{% for i in range(number_of_iterations) %}
iteration_{{ i }} AS (
    SELECT 
        id,
        random_value1,
        random_value2,
        group_id,
        vector_length * (1 + random() / 10) as vector_length_adjusted,
        trig_result + SIN(z_score) as trig_result_adjusted,
        avg_length,
        std_length,
        z_score,
        {% if i % 5 == 0 %}
        SYSTEM$WAIT({{ wait_time_per_iteration / 2 }}) as wait_result,  -- periodic waits
        {% endif %}
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY z_score) as rank_in_group
    FROM {% if i == 0 %}joined_data{% else %}iteration_{{ i - 1 }}{% endif %}
    {% if i > 0 %}
    WHERE id % {{ 50 - i }} = 0  -- progressively filter to reduce data volume but keep processing complex
    {% endif %}
),
{% endfor %}

-- Final result with one more wait
final_result AS (
    SELECT 
        id,
        random_value1,
        random_value2,
        group_id,
        vector_length_adjusted,
        trig_result_adjusted,
        avg_length,
        std_length,
        z_score,
        rank_in_group,
        SYSTEM$WAIT({{ wait_time_per_iteration }}) as final_wait  -- final wait
    FROM iteration_{{ number_of_iterations - 1 }}
    WHERE rank_in_group <= 100  -- limit final result size
)

-- Return a manageable result set from our long process
SELECT 
    group_id,
    COUNT(*) as count_records,
    AVG(vector_length_adjusted) as avg_adjusted_length,
    AVG(z_score) as avg_z_score,
    MIN(trig_result_adjusted) as min_trig_adjusted,
    MAX(trig_result_adjusted) as max_trig_adjusted,
    SUM(rank_in_group) as sum_ranks
FROM final_result
GROUP BY group_id
ORDER BY group_id
LIMIT 1000