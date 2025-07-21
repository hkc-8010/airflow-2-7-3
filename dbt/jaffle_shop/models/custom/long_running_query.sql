{{
    config(
        materialized='table',
        tags=['long_running']
    )
}}

/*
This model is designed to run for approximately 15 minutes on Snowflake.
It uses a combination of complex queries, data processing, and explicit SYSTEM$WAIT() calls
to ensure a consistent execution time regardless of the underlying hardware.

The query:
1. Creates a moderate dataset using a sequence generator
2. Performs calculations and transformations on the data
3. Uses SYSTEM$WAIT() to add controlled delays throughout execution
*/

-- Set variables for our long-running query
{% set number_of_iterations = 6 %}
{% set sleep_time_per_iteration = 130 %}  -- Total ~780 seconds across iterations
{% set initial_sleep = 60 %}              -- Initial sleep 60 seconds
{% set final_sleep = 60 %}                -- Final sleep 60 seconds
                                          -- Total sleep: ~15 minutes (900 seconds)

WITH 
-- Start with an initial sleep to ensure minimum execution time
initial_wait AS (
    SELECT 
        SYSTEM$WAIT({{ initial_sleep }}) as initial_wait_result,
        1 as dummy_id
),

-- Generate a dataset using Snowflake's generator
large_dataset AS (
    SELECT 
        SEQ4() as id,
        UNIFORM(1, 1000000, RANDOM()) as random_value1,
        UNIFORM(1, 1000000, RANDOM()) as random_value2,
        UNIFORM(1, 100, RANDOM()) as group_id,
        RANDOM() as random_factor,
        i.dummy_id  -- Join with initial_wait to ensure it runs first
    FROM TABLE(GENERATOR(ROWCOUNT => 50000)) g
    CROSS JOIN initial_wait i
),

-- First computation layer with explicit sleep
computation_1 AS (
    SELECT 
        id,
        random_value1,
        random_value2,
        group_id,
        random_factor,
        SQRT(POWER(random_value1, 2) + POWER(random_value2, 2)) as vector_length,
        SIN(random_value1 / 1000.0) * COS(random_value2 / 1000.0) as trig_result,
        SYSTEM$WAIT({{ sleep_time_per_iteration // 3 }}) as wait_result  -- Use integer division //
    FROM large_dataset
    WHERE id <= 10000  -- Limit for performance
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
        MAX(random_value2) as max_val2,
        SYSTEM$WAIT({{ sleep_time_per_iteration // 3 }}) as wait_result  -- Use integer division //
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
        c.random_factor,
        c.vector_length,
        c.trig_result,
        g.avg_length,
        g.std_length,
        g.sum_trig,
        g.group_count,
        (c.vector_length - g.avg_length) / NULLIF(g.std_length, 0) as z_score,
        SYSTEM$WAIT({{ sleep_time_per_iteration // 3 }}) as wait_result  -- Use integer division //
    FROM computation_1 c
    JOIN grouped_data g ON c.group_id = g.group_id
),

{% for i in range(number_of_iterations) %}
iteration_{{ i }} AS (
    SELECT 
        id,
        random_value1,
        random_value2,
        group_id,
        random_factor,
        {% if i == 0 %}
        vector_length * (1 + RANDOM() / 10) as vector_length_adjusted,
        {% else %}
        vector_length_adjusted * (1 + RANDOM() / 20) as vector_length_adjusted,
        {% endif %}
        {% if i == 0 %}
        trig_result + SIN(NVL(z_score, 0)) as trig_result_adjusted,
        {% else %}
        trig_result_adjusted + SIN(NVL(z_score, 0)) as trig_result_adjusted,
        {% endif %}
        avg_length,
        std_length,
        z_score,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY z_score) as rank_in_group,
        {% if i < number_of_iterations - 1 %}
        SYSTEM$WAIT({{ (sleep_time_per_iteration // number_of_iterations) }}) as wait_result  -- Use integer division //
        {% else %}
        NULL as wait_result  -- Skip sleep in the last iteration as we'll have a final sleep
        {% endif %}
    FROM {% if i == 0 %}joined_data{% else %}iteration_{{ i - 1 }}{% endif %}
    {% if i > 0 %}
    WHERE MOD(id, {{ 20 - i if (20 - i) > 1 else 1 }}) = 0  -- progressively filter
    {% endif %}
),
{% endfor %}

-- Pre-final step with aggregation and another sleep
pre_final AS (
    SELECT 
        group_id,
        AVG(vector_length_adjusted) as avg_adjusted_length,
        AVG(z_score) as avg_z_score,
        MIN(trig_result_adjusted) as min_trig_adjusted,
        MAX(trig_result_adjusted) as max_trig_adjusted,
        SUM(rank_in_group) as sum_ranks,
        COUNT(*) as count_records,
        SYSTEM$WAIT({{ final_sleep }}) as final_wait_result  -- Final sleep
    FROM iteration_{{ number_of_iterations - 1 }}
    WHERE rank_in_group <= 100  -- limit final result size
    GROUP BY group_id
)

-- Return a manageable result set from our long process
SELECT 
    group_id,
    count_records,
    avg_adjusted_length,
    avg_z_score,
    min_trig_adjusted,
    max_trig_adjusted,
    sum_ranks
FROM pre_final
ORDER BY group_id
LIMIT 1000