{# Creates order table for use in BI that joins order table with forecasted data #}

with 

actuals_with_dates as (

    select 
        *
    
    from {{ ref('utils_cohorts__joined')}}

),

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

retentions as (

    select 
        *
    
    from {{ ref('utils_cohorts__retained')}}
),

forecast_calendar as (

    select
        *
    from {{ ref('utils_calendar__created')}}

    where year = '2022'
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

reforecast as (

    select
        *
    
    from {{ ref('base_csv__reforecast')}}
),

revenue_from_last as (

    select
    case 
        when
            (revenue_group = 'Web' or revenue_group = 'Concierge')
                and (cohort = '2011' 
                    or cohort = '2012' 
                    or cohort = '2013' 
                    or cohort = '2014' 
                    or cohort = '2015' 
                    or cohort = '2016'
                    or cohort = '2017'
                    or cohort = '2018'
                    or cohort = '2019')
                    then 'Legacy'
        when 
            (revenue_group = 'Web' or revenue_group = 'Concierge')
            and cohort = '2020'
        then '2020'
        when 
            (revenue_group = 'Web' or revenue_group = 'Concierge')
            and cohort = '2021'
        then '2021'
        when 
            (revenue_group = 'Web' or revenue_group = 'Concierge')
            and cohort = '2022'
        then '2022'
        else
            'All'
        end as cohort_group,
        revenue_group,
        revenue_segment,
        sum(order_total) as revenue_last_year

    from orders_flattened
    
    where order_date_fiscal_year = '2021'

    group by 1, 2, 3
    order by 1, 2, 3
),

seasonalities as (

    select
        *

    from public.seasonality
),


run_rates_for_new_year as (

    select
        cohort_group,
        revenue_group,
        revenue_segment,
        total_run_rate,
        count(*)

    from

    (select
        retentions.cohort_group,
        retentions.revenue_group,
        retentions.revenue_segment,
        r2.revenue_last_year,
        r2.revenue_last_year::float * retentions.forecasted_retentions::float as total_run_rate
    
    from retentions

    left join
        
    (select
        revenue_from_last.cohort_group,
        case
            when
                revenue_from_last.cohort_group = '2021'
            then '2022'
        end as new_cohort,
        revenue_from_last.revenue_group,
        revenue_from_last.revenue_segment,
        revenue_from_last.revenue_last_year
        
    from revenue_from_last
    
    where revenue_from_last.cohort_group = '2021') as r2

        on r2.new_cohort = retentions.cohort_group
        and r2.revenue_group = retentions.revenue_group
        and r2.revenue_segment = retentions.revenue_segment

    where retentions.cohort_group = '2022')

    group by 1, 2, 3, 4
),

run_rates as (

    select
        revenue_from_last.cohort_group,
        revenue_from_last.revenue_group,
        revenue_from_last.revenue_segment,
        revenue_from_last.revenue_last_year::float * retentions.forecasted_retentions::float as total_run_rate

    from revenue_from_last

    left join retentions
        on revenue_from_last.cohort_group = retentions.cohort_group
        and revenue_from_last.revenue_group = retentions.revenue_group
        and revenue_from_last.revenue_segment = retentions.revenue_segment

    UNION ALL

    select 
        cohort_group,
        revenue_group,
        revenue_segment,
        total_run_rate

    from run_rates_for_new_year

    where total_run_rate is NOT NULL 
),

int_run_rate_by_period as (

    select
        seasonalities.cohort_group as cohort,
        seasonalities.revenue_group,
        seasonalities.revenue_segment,
        seasonalities.order_date_fiscal_period,
        seasonalities.average_seasonality * run_rates.total_run_rate as predicted_run_rate_by_period,
        predicted_run_rate_by_period/4 as predicted_run_rate_by_week,
        predicted_run_rate_by_week/7 as predicted_run_rate_by_day

    from seasonalities

    left join run_rates
        on seasonalities.cohort_group = run_rates.cohort_group
        and seasonalities.revenue_group = run_rates.revenue_group
        and seasonalities.revenue_segment = run_rates.revenue_segment

    order by 1, 2, 3, 4
),

run_rate_by_period as (

    select int_run_rate_by_period.*,
    forecast_calendar.date_day as ordered_on,
    forecast_calendar.year_period as order_date_fiscal_year_period,
    forecast_calendar.year_period_week as order_date_fiscal_year_period_week,
    forecast_calendar.year_period_week_day as order_date_fiscal_year_period_week_day,
    forecast_calendar.year as order_date_fiscal_year,
    forecast_calendar.week_of_period as order_date_fiscal_week,
    forecast_calendar.day_of_week as order_date_fiscal_day

    from int_run_rate_by_period

    left join forecast_calendar 
    on forecast_calendar.fiscal_period = int_run_rate_by_period.order_date_fiscal_period
),

final_run as (

    select

        run_rate_by_period.*,
        ISNULL(actuals_with_dates.actual_revenue, 0) as actual_revenue,
        ISNULL(NULLIF(actuals_with_dates.revenue_last,0),.00001) as actuals_last_year,
        ISNULL(actuals_with_dates.rolling_actual_revenue, 0) as rolling_actual_revenue,
        ISNULL(NULLIF(actuals_with_dates.rolling_revenue_last_year,0),.00001) as rolling_actuals_last_year
    
    from run_rate_by_period

    left join actuals_with_dates 
        on actuals_with_dates.cohort = run_rate_by_period.cohort
        and actuals_with_dates.revenue_group = run_rate_by_period.revenue_group
        and actuals_with_dates.revenue_segment = run_rate_by_period.revenue_segment
        and actuals_with_dates.order_date_fiscal_period = run_rate_by_period.order_date_fiscal_period
        and actuals_with_dates.order_date_fiscal_year_period = run_rate_by_period.order_date_fiscal_year_period
        and actuals_with_dates.order_date_fiscal_year_period_week = run_rate_by_period.order_date_fiscal_year_period_week
        and actuals_with_dates.order_date_fiscal_year_period_week_day = run_rate_by_period.order_date_fiscal_year_period_week_day

    order by 1, 2, 3, 4, 9
),

weights as (

    select
        *
    
    from {{ ref('utils_cohorts_weighted')}}
),

april_reforecast as (

    select
        *

    from {{ ref('stg_csv__april_reforecast')}}
),

pre_final as (

    select 
        final_run.*,
        reforecast.forecast_by_period as budget_by_period,
        reforecast.forecast_by_week as budget_by_week,
        reforecast.forecast_by_day as budget_by_day,
        april_reforecast.forecast_by_period as reforecast_by_period,
        (april_reforecast.forecast_by_period::float / 4)::float as reforecast_by_week,
        (reforecast_by_week::float / 7)::float as reforecast_by_day

    from final_run

    left join reforecast
        on final_run.cohort = reforecast.cohort
        and final_run.revenue_group = reforecast.revenue_group
        and final_run.revenue_segment = reforecast.revenue_segment
        and final_run.order_date_fiscal_period = reforecast.period

    left join april_reforecast
        on final_run.cohort = april_reforecast.cohort
        and final_run.revenue_group = april_reforecast.revenue_group
        and final_run.revenue_segment = april_reforecast.revenue_segment
        and final_run.order_date_fiscal_period = april_reforecast.order_date_fiscal_period
),

final_non_web as (

    select
        pre_final.*,
        ISNULL(pre_final.reforecast_by_week::float * weights.percent_by_week::float * 4, pre_final.reforecast_by_week::float * .001 * 4) as weighted_forecast_by_week,
        ISNULL(pre_final.reforecast_by_day::float * weights.percent_by_day::float * 7, pre_final.reforecast_by_day::float * .001 * 7) as weighted_forecast_by_day,
        ISNULL(pre_final.budget_by_week::float * weights.percent_by_week::float * 4, pre_final.budget_by_week::float * .001 * 4) as weighted_budget_by_week,
        ISNULL(pre_final.budget_by_day::float * weights.percent_by_day::float * 7, pre_final.budget_by_day::float * .001 * 7) as weighted_budget_by_day
    
    from pre_final

    left join weights 
        on weights.cohort = pre_final.cohort
        and weights.revenue_group = pre_final.revenue_group
        and weights.revenue_segment = pre_final.revenue_segment
        and weights.order_date_fiscal_day = pre_final.order_date_fiscal_day
        and weights.order_date_fiscal_week = pre_final.order_date_fiscal_week

    where pre_final.revenue_group != 'Web'
),

web_weighted as (

    select 
        order_date_fiscal_period,
        order_date_fiscal_week,
        order_date_fiscal_day,
        "weight" as weight_by_day
  
    from {{ ref('web_weights')}}
),

web_weight_by_week as (

    select 
        order_date_fiscal_period,
        order_date_fiscal_week,
        sum(weight_by_day) as weight_by_week

    from web_weighted

    group by 1, 2

    order by 1, 2
),

web_weight as (

  select 
    web_weighted.order_date_fiscal_period,
    web_weighted.order_date_fiscal_week,
    web_weighted.order_date_fiscal_day,
    web_weighted.weight_by_day,
    web_weight_by_week.weight_by_week

  from web_weighted

  left join web_weight_by_week
    on web_weight_by_week.order_date_fiscal_week = web_weighted.order_date_fiscal_week
    and web_weight_by_week.order_date_fiscal_period = web_weighted.order_date_fiscal_period

  order by 1, 2, 3
),

final_web as (

    select
    pre_final.*,
    ISNULL((pre_final.reforecast_by_period::float * web_weight.weight_by_week::float), pre_final.reforecast_by_period::float * .0001) as weighted_forecast_by_week,
    ISNULL((pre_final.reforecast_by_period::float * web_weight.weight_by_day::float), pre_final.reforecast_by_period::float * .0001) as weighted_forecast_by_day,
    ISNULL((pre_final.budget_by_week::float * web_weight.weight_by_week::float), pre_final.budget_by_week::float * .0001) as weighted_budget_by_week,
    ISNULL((pre_final.budget_by_day::float * web_weight.weight_by_day::float), pre_final.budget_by_day::float * .0001) as weighted_budget_by_day
    
    from pre_final

    left join web_weight 
        on web_weight.order_date_fiscal_period = pre_final.order_date_fiscal_period
        and web_weight.order_date_fiscal_day = pre_final.order_date_fiscal_day
        and web_weight.order_date_fiscal_week = pre_final.order_date_fiscal_week

    where pre_final.revenue_group = 'Web'    
),

final_joined as (

    select
        *
    
    from final_non_web

    UNION

    select
        *

    from final_web
),

rolling_forecast as (

    select
    A.cohort,
    A.revenue_group,
    A.revenue_segment,
    A.ordered_on, 
    sum(B.weighted_forecast_by_day) as rolling_weighted_forecast,
    sum(B.weighted_budget_by_day) as rolling_weighted_budget

    from final_joined A

    left join final_joined B
        on A.cohort = B.cohort
        and A.revenue_group = B.revenue_group
        and A.revenue_segment = B.revenue_segment

    where A.ordered_on > '2021-12-28'
    and B.ordered_on <= A.ordered_on

    group by 1,2,3,4
    order by 1,2,3,4
),

final_adds as (

    select 
    final_joined.*,
    isnull(rolling_forecast.rolling_weighted_forecast,.00001) as rolling_weighted_forecast,
    isnull(rolling_forecast.rolling_weighted_budget,.00001) as rolling_weighted_budget

    from final_joined

    left join rolling_forecast
        on rolling_forecast.cohort = final_joined.cohort
        and rolling_forecast.revenue_group = final_joined.revenue_group
        and rolling_forecast.revenue_segment = final_joined.revenue_segment
        and rolling_forecast.ordered_on = final_joined.ordered_on
)

select * from final_adds order by 8 
