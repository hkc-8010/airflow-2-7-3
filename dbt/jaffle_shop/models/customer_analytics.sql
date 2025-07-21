{{ config(
    tags=['analytics', 'customer_insights'],
    materialized='table'
) }}

-- Customer analytics model that combines customer and order data
-- This model performs comprehensive customer analysis including segmentation,
-- order trends, and lifetime value calculations

with customers as (
    select * from {{ ref('customers') }}
),

orders as (
    select * from {{ ref('orders') }}
),

-- Customer segmentation based on order patterns
customer_segments as (
    select 
        customer_id,
        case 
            when number_of_orders >= 10 then 'High Frequency'
            when number_of_orders >= 5 then 'Medium Frequency'
            when number_of_orders >= 2 then 'Low Frequency'
            else 'One Time'
        end as frequency_segment,
        
        case 
            when customer_lifetime_value >= 200 then 'High Value'
            when customer_lifetime_value >= 100 then 'Medium Value'
            when customer_lifetime_value >= 50 then 'Low Value'
            else 'Minimal Value'
        end as value_segment
    from customers
),

-- Order trends analysis with window functions
order_trends as (
    select 
        orders.*,
        lag(amount, 1) over (partition by customer_id order by order_date) as previous_order_amount,
        lead(amount, 1) over (partition by customer_id order by order_date) as next_order_amount,
        row_number() over (partition by customer_id order by order_date) as order_sequence,
        avg(amount) over (partition by customer_id order by order_date rows between 2 preceding and 2 following) as rolling_avg_amount
    from orders
),

-- Complex aggregations to simulate long-running operations
monthly_analytics as (
    select 
        date_trunc('month', order_date) as order_month,
        cs.frequency_segment,
        cs.value_segment,
        count(distinct ot.customer_id) as unique_customers,
        count(ot.order_id) as total_orders,
        sum(ot.amount) as total_revenue,
        avg(ot.amount) as avg_order_value,
        stddev(ot.amount) as order_value_stddev,
        min(ot.amount) as min_order_value,
        max(ot.amount) as max_order_value,
        percentile_cont(0.5) within group (order by ot.amount) as median_order_value,
        sum(case when ot.previous_order_amount is not null and ot.amount > ot.previous_order_amount then 1 else 0 end) as orders_with_increased_value,
        avg(ot.rolling_avg_amount) as avg_rolling_amount
    from order_trends ot
    join customer_segments cs on ot.customer_id = cs.customer_id
    group by 1, 2, 3
),

-- Customer lifetime analysis
customer_lifetime_analysis as (
    select 
        cs.customer_id,
        cs.frequency_segment,
        cs.value_segment,
        c.first_order,
        c.most_recent_order,
        c.number_of_orders,
        c.customer_lifetime_value,
        datediff('day', c.first_order, c.most_recent_order) as customer_lifespan_days,
        case when c.number_of_orders > 1 then 
            c.customer_lifetime_value / nullif(datediff('day', c.first_order, c.most_recent_order), 0)
        else null end as daily_value_rate,
        case when c.number_of_orders > 1 then 
            datediff('day', c.first_order, c.most_recent_order) / nullif((c.number_of_orders - 1), 0)
        else null end as avg_days_between_orders
    from customer_segments cs
    join customers c on cs.customer_id = c.customer_id
),

-- Final comprehensive analysis
final as (
    select 
        ma.order_month,
        ma.frequency_segment,
        ma.value_segment,
        ma.unique_customers,
        ma.total_orders,
        ma.total_revenue,
        ma.avg_order_value,
        ma.order_value_stddev,
        ma.min_order_value,
        ma.max_order_value,
        ma.median_order_value,
        ma.orders_with_increased_value,
        ma.avg_rolling_amount,
        
        -- Customer lifetime metrics aggregated by segment and month
        avg(cla.customer_lifespan_days) as avg_customer_lifespan_days,
        avg(cla.daily_value_rate) as avg_daily_value_rate,
        avg(cla.avg_days_between_orders) as avg_days_between_orders,
        
        -- Cohort analysis indicators
        count(case when cla.customer_lifespan_days <= 30 then 1 end) as new_customers_30_days,
        count(case when cla.customer_lifespan_days > 30 and cla.customer_lifespan_days <= 90 then 1 end) as customers_30_90_days,
        count(case when cla.customer_lifespan_days > 90 then 1 end) as customers_90_plus_days,
        
        -- Revenue concentration metrics
        sum(ma.total_revenue) over (partition by ma.order_month) as monthly_total_revenue,
        ma.total_revenue / sum(ma.total_revenue) over (partition by ma.order_month) as revenue_share_by_segment
        
    from monthly_analytics ma
    left join customer_lifetime_analysis cla 
        on ma.frequency_segment = cla.frequency_segment 
        and ma.value_segment = cla.value_segment
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select * from final
order by order_month desc, frequency_segment, value_segment 