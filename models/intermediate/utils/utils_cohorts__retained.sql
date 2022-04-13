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

retentions as (

    select
        cohort_group,
        revenue_group,
        revenue_segment,
        forecasted_retentions,
        count(*)
    from
    (select
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
        case
            when
                cohort_group = 'Legacy' and revenue_group = 'Web'
            then '.9'
            when
                cohort_group = '2020' and revenue_group = 'Web'
            then '.95'
            when
                cohort_group = '2021' and revenue_group = 'Web'
            then '.19'
            when
                cohort_group = '2022' and revenue_group = 'Web'
            then '.95'
            when
                cohort_group = 'Legacy' and revenue_segment = 'Corporate'
            then '.88'
            when
                cohort_group = '2020' and revenue_segment = 'Corporate'
            then '.88'
            when
                cohort_group = '2021' and revenue_segment = 'Corporate'
            then '.21'
            when
                cohort_group = '2022' and revenue_segment = 'Corporate'
            then '.95'
            when
                cohort_group = 'Legacy' and revenue_segment = 'CallCenter'
            then '.86'
            when
                cohort_group = '2020' and revenue_segment = 'CallCenter'
            then '.86'
            when
                cohort_group = '2021' and revenue_segment = 'CallCenter'
            then '.134'
            when
                cohort_group = '2022' and revenue_segment = 'CallCenter'
            then '.95'
            when
                cohort_group = 'All' and revenue_segment = 'Goldbelly'
            then '1.22'
            when
                cohort_group = 'All' and revenue_segment = 'Amazon'
            then '1.1'
            when
                cohort_group = 'All' and revenue_segment = 'Wholesale/Misc'
            then '1.04'
        end as forecasted_retentions
        
        from orders_flattened

    where forecasted_retentions is NOT NULL)
    group by 1, 2, 3, 4

)

select * from retentions