with 

orders as (

    select
        *
    
    from {{ ref('int_orders_joined')}}

    where ordered_on > '2021-12-27'
),

orders_last as (

    select
        *
    
    from {{ ref('int_orders_joined')}}

    where ordered_on < '2021-12-27'
    and ordered_on > '2020-12-26'
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

orders_flattened_last as (

    select 
    distinct orders_last.order_id,
    orders_last.customer_id,
    orders_last.revenue_group,
    orders_last.revenue_segment,
    orders_last.ordered_on,
    orders_last.order_date_fiscal_year,
    orders_last.order_date_fiscal_period,
    orders_last.order_date_fiscal_week,
    orders_last.order_date_fiscal_day,
    orders_last.order_date_fiscal_year_period,
    orders_last.order_date_fiscal_year_period_week,
    orders_last.order_date_fiscal_year_period_week_day,
    orders_last.order_total,
    customers.cohort

    from orders_last

    left join customers
        on orders_last.customer_id = customers.customer_id

),

revenue_from_last_non_agg as (

    select
    case 
        when
            (A.revenue_group = 'Web' or A.revenue_group = 'Concierge')
                and (A.cohort = '2011' 
                    or A.cohort = '2012' 
                    or A.cohort = '2013' 
                    or A.cohort = '2014' 
                    or A.cohort = '2015' 
                    or A.cohort = '2016'
                    or A.cohort = '2017'
                    or A.cohort = '2018'
                    or A.cohort = '2019')
                    then 'Legacy'
        when 
            (A.revenue_group = 'Web' or A.revenue_group = 'Concierge')
            and A.cohort = '2020'
        then '2020'
        when 
            (A.revenue_group = 'Web' or A.revenue_group = 'Concierge')
            and A.cohort = '2021'
        then '2021'
        when 
            (A.revenue_group = 'Web' or A.revenue_group = 'Concierge')
            and A.cohort = '2022'
        then '2022'
        else
            'All'
        end as cohort_group,
        A.revenue_group,
        A.revenue_segment,
        A.order_date_fiscal_period,
        A.order_date_fiscal_week,
        A.order_date_fiscal_day,
        A.ordered_on,
        sum(A.order_total) as revenue_last_year

    from orders_flattened_last A
    
    where A.order_date_fiscal_year = '2021'

    group by 1, 2, 3, 4, 5, 6, 7
    order by 1, 2, 3
),

rolling_revenue_from_last as (

    select
        A.*,
            (
        select 
        sum(B.revenue_last_year)

        from revenue_from_last_non_agg B

        where B.ordered_on <= A.ordered_on
        and A.cohort_group = B.cohort_group
        and A.revenue_group = B.revenue_group
        and A.revenue_segment = B.revenue_segment
        and B.ordered_on > '2020-12-26'

    ) as rolling_actuals_last_year

    from revenue_from_last_non_agg A
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
    end as cohort_group

    from orders_flattened
    
),

actuals_web_and_concierge as (

    select 
        order_date_fiscal_year_period,
        order_date_fiscal_year_period_week,
        order_date_fiscal_year_period_week_day,
        revenue_group,
        revenue_segment,
        cohort_group as cohort,
        sum(order_total) as actuals,
        order_date_fiscal_year,
        order_date_fiscal_period,
        order_date_fiscal_week,
        order_date_fiscal_day

    from orders_adjusted
    where revenue_group = 'Web' or revenue_group = 'Concierge'
    group by 1, 2, 3, 4, 5, 6, 8, 9, 10, 11
    order by 1, 2, 3, 4, 5, 6, 8, 9, 10, 11

),

actuals_marketplaces_and_unknown_adjust as (

    select 
        order_date_fiscal_year_period,
        order_date_fiscal_year_period_week,
        order_date_fiscal_year_period_week_day,
        order_date_fiscal_year,
        order_date_fiscal_period,
        order_date_fiscal_week,
        order_date_fiscal_day,
        revenue_group,
        revenue_segment,
        case 
            when 
                revenue_group = 'Marketplaces'
                    and cohort_group = 'Legacy'
                        or cohort_group = '2020'
                        or cohort_group = '2021'
                        or cohort_group = '2022'
                        or cohort_group = 'unknown'
            then 'All'
            when 
                revenue_group = 'Unknown'
                    and cohort_group = 'Legacy'
                        or cohort_group = '2020'
                        or cohort_group = '2021'
                        or cohort_group = '2022'
                        or cohort_group = 'unknown'
            then 'All'
        end as cohort,
        order_total

    from orders_adjusted
    where revenue_group = 'Marketplaces' or revenue_group = 'Unknown'

),

actuals_marketplaces_and_unknown as (

    select 
        order_date_fiscal_year_period,
        order_date_fiscal_year_period_week,
        order_date_fiscal_year_period_week_day,
        revenue_group,
        revenue_segment,
        cohort,
        sum(order_total) as actuals,
        order_date_fiscal_year,
        order_date_fiscal_period,
        order_date_fiscal_week,
        order_date_fiscal_day

    from actuals_marketplaces_and_unknown_adjust
    group by 1, 2, 3, 4, 5, 6, 8, 9, 10, 11
    order by 1, 2, 3, 4, 5, 6, 8, 9, 10, 11

),

actuals as (
    
    select 
        *
    
    from actuals_web_and_concierge

    UNION ALL 

    select
        *
    
    from actuals_marketplaces_and_unknown

    order by 1, 2, 3, 4

),

forecast_calendar as (

    select
        *
    from {{ ref('utils_calendar__created')}}
    
),

all_dates_non as (

    select
    distinct
        actuals.cohort,
        actuals.revenue_group,
        actuals.revenue_segment,
        A.date_day
    
    from forecast_calendar A, actuals

    where A.date_day > '2021-12-28' and A.date_day < '2022-12-30'
),

all_dates as (

    select
        A.*,
        (select B.day_of_week from forecast_calendar B where A.date_day = B.date_day) as order_date_fiscal_day,
        (select B.week_of_period from forecast_calendar B where A.date_day = B.date_day) as order_date_fiscal_week,
        (select B.fiscal_period from forecast_calendar B where A.date_day = B.date_day) as order_date_fiscal_period,
        (select B.year from forecast_calendar B where A.date_day = B.date_day) as order_date_fiscal_year,
        (select B.year_period from forecast_calendar B where A.date_day = B.date_day) as order_date_fiscal_year_period,
        (select B.year_period_week from forecast_calendar B where A.date_day = B.date_day) as order_date_fiscal_year_period_week,
        (select B.year_period_week_day from forecast_calendar B where A.date_day = B.date_day) as order_date_fiscal_year_period_week_day
    
    from all_dates_non A
),

final_actuals as (

        select
        all_dates.cohort,
        all_dates.revenue_group,
        all_dates.revenue_segment,
        all_dates.date_day as ordered_on,
        all_dates.order_date_fiscal_year_period,
        all_dates.order_date_fiscal_year_period_week,
        all_dates.order_date_fiscal_year_period_week_day,
        isnull(actuals.actuals,0) as actual_revenue,
        all_dates.order_date_fiscal_year,
        all_dates.order_date_fiscal_period,
        all_dates.order_date_fiscal_week,
        all_dates.order_date_fiscal_day

        from all_dates

        left join actuals
            on all_dates.cohort = actuals.cohort
            and all_dates.revenue_group = actuals.revenue_group
            and all_dates.revenue_segment = actuals.revenue_segment
            and all_dates.order_date_fiscal_day = actuals.order_date_fiscal_day
            and all_dates.order_date_fiscal_week = actuals.order_date_fiscal_week
            and all_dates.order_date_fiscal_period = actuals.order_date_fiscal_period
            and all_dates.order_date_fiscal_year = actuals.order_date_fiscal_year
),

rolling_actuals as (

    select
    A.cohort,
    A.revenue_group,
    A.revenue_segment,
    A.ordered_on, 
    (
        select 
        sum(B.actual_revenue)

        from final_actuals B

        where B.ordered_on <= A.ordered_on
        and A.cohort = B.cohort
        and A.revenue_group = B.revenue_group
        and A.revenue_segment = B.revenue_segment
        and B.ordered_on > '2021-12-28'

    ) as rolling_actual_revenue,
    A.actual_revenue

    from final_actuals A

    where ordered_on > '2021-12-28'

    order by 1,2,3,4
),

final as (

    select
        final_actuals.*,
        rolling_actuals.rolling_actual_revenue,
        ISNULL(revenue_from_last_non_agg.revenue_last_year,0) as revenue_last,
        ISNULL(rolling_revenue_from_last.rolling_actuals_last_year,0) as rolling_revenue_last_year

    from final_actuals

    left join rolling_actuals
        on rolling_actuals.cohort = final_actuals.cohort
        and rolling_actuals.revenue_group = final_actuals.revenue_group
        and rolling_actuals.revenue_segment = final_actuals.revenue_segment
        and rolling_actuals.ordered_on = final_actuals.ordered_on

    left join revenue_from_last_non_agg 
        on revenue_from_last_non_agg.cohort_group = final_actuals.cohort
        and revenue_from_last_non_agg.revenue_group = final_actuals.revenue_group
        and revenue_from_last_non_agg.revenue_segment = final_actuals.revenue_segment
        and revenue_from_last_non_agg.order_date_fiscal_period = final_actuals.order_date_fiscal_period
        and revenue_from_last_non_agg.order_date_fiscal_week = final_actuals.order_date_fiscal_week
        and revenue_from_last_non_agg.order_date_fiscal_day = final_actuals.order_date_fiscal_day

    left join rolling_revenue_from_last
        on rolling_revenue_from_last.cohort_group = final_actuals.cohort
        and rolling_revenue_from_last.revenue_group = final_actuals.revenue_group
        and rolling_revenue_from_last.revenue_segment = final_actuals.revenue_segment
        and rolling_revenue_from_last.order_date_fiscal_period = final_actuals.order_date_fiscal_period
        and rolling_revenue_from_last.order_date_fiscal_week = final_actuals.order_date_fiscal_week
        and rolling_revenue_from_last.order_date_fiscal_day = final_actuals.order_date_fiscal_day

    order by 1,2,3,8
)


select * from final