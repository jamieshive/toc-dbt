with 

orders as (

    select
        *
    
    from {{ ref('int_orders_joined')}}
),

customers as (

    select 
        * 
        
    from {{ ref('int_customers_grouped') }}
),

orders_flattened as (

    select 
    distinct orders.order_id,
    orders.customer_id,
    orders.revenue_group,
    orders.revenue_segment,
    orders.address_concat,
    orders.ordered_on,
    orders.order_date_fiscal_year,
    orders.order_date_fiscal_period,
    orders.order_date_fiscal_week,
    orders.order_date_fiscal_day,
    orders.order_date_fiscal_year_period,
    orders.order_date_fiscal_year_period_week,
    orders.order_date_fiscal_year_period_week_day,
    orders.order_total,
    customers.cohort

    from orders

    left join customers
        on orders.customer_id = customers.customer_id

),

orders_adjusted as (

    select *,
    case 
        when cohort = '2011' 
        or cohort = '2012' 
        or cohort = '2013' 
        or cohort = '2014' 
        or cohort = '2015' 
        or cohort = '2016'
        or cohort = '2017'
        or cohort = '2018'
        or cohort = '2019'
            then 'Legacy'
        when cohort = '2020'
            then '2020'
        when cohort = '2021'
            then '2021'
        when cohort = '2022'
            then '2022'
        else 'unknown'
    end as cohort_group,
    case 
            when 
                revenue_group = 'Marketplaces'
            then 'All'
            else cohort_group
        end as cohort_adjusted

    from orders_flattened
),

cohort_size as (

    select
        cohort_adjusted as cohort,
        revenue_group,
        revenue_segment,
        count(distinct order_id) as num_of_customers
    
    from orders_adjusted

    group by 1,2,3

),

cohort_size_by_day as (

    select
        cohort_adjusted as cohort,
        revenue_group,
        revenue_segment,
        order_date_fiscal_day,
        count(distinct order_id) as num_of_customers_by_day
    
    from orders_adjusted

    group by 1,2,3,4
    order by 1,2,3,4

),

cohort_size_by_week as (

    select
        cohort_adjusted as cohort,
        revenue_group,
        revenue_segment,
        order_date_fiscal_week,
        count(distinct order_id) as num_of_customers_by_week
    
    from orders_adjusted

    group by 1,2,3,4
    order by 1,2,3,4

),

weights_by_day as (

    select
        cohort_size_by_day.cohort,
        cohort_size_by_day.revenue_group,
        cohort_size_by_day.revenue_segment,
        cohort_size_by_day.order_date_fiscal_day,
        num_of_customers_by_day::float/cohort_size.num_of_customers as percent_by_day
    
    from cohort_size_by_day

    left join cohort_size
        on cohort_size_by_day.cohort = cohort_size.cohort
        and cohort_size_by_day.revenue_group = cohort_size.revenue_group
        and cohort_size_by_day.revenue_segment = cohort_size.revenue_segment
    
    order by 1,2,3,4
),

weights_by_week as (

    select
        cohort_size_by_week.cohort,
        cohort_size_by_week.revenue_group,
        cohort_size_by_week.revenue_segment,
        cohort_size_by_week.order_date_fiscal_week,
        cohort_size_by_week.num_of_customers_by_week,
        cohort_size.num_of_customers,
        num_of_customers_by_week::float/cohort_size.num_of_customers as percent_by_week
    
    from cohort_size_by_week

    left join cohort_size
        on cohort_size_by_week.cohort = cohort_size.cohort
        and cohort_size_by_week.revenue_group = cohort_size.revenue_group
        and cohort_size_by_week.revenue_segment = cohort_size.revenue_segment
    
    order by 1,2,3,4
),

weights as (

    select
        weights_by_day.*,
        weights_by_week.order_date_fiscal_week,
        weights_by_week.percent_by_week

    from weights_by_day

    left join weights_by_week
        on weights_by_week.cohort = weights_by_day.cohort
        and weights_by_week.revenue_group = weights_by_day.revenue_group
        and weights_by_week.revenue_segment = weights_by_day.revenue_segment
    
    order by 1,2,3,4,6
)

select * from weights