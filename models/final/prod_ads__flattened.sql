{{ config(materialized='ephemeral') }}

with facebook as (

    select 
        *

    from {{ ref('prod_facebook_ads__flattened')}}
),

google_ads as (

    select
        *

    from {{ ref('prod_google_ads__flattened')}}
),

microsoft as (

    select
        *

    from {{ ref('prod_microsoft_ads__flattened')}}
),

all_ads as (

    select
        date_day,
        spend,
        'facebook' as ad_source

    from facebook

    UNION

        select
        date_day,
        spend,
        'google_ads' as ad_source
        
    from google_ads

    UNION

        select
        date_day,
        spend,
        'microsoft' as ad_source
        
    from microsoft
),

new_customers as (

    select
        ordered_on,
        is_new_customer,
        count(is_new_customer) as num_new_customer

    from {{ ref('prod_orders__flattened')}}

    where is_new_customer = 'Y'

    group by 1,2

),

ads_agg as (

    select
        all_ads.date_day,
        sum(spend) as spend_per_day

    from all_ads

    group by 1
),

ads_customers as (

    select
        date_day,
        datepart('month',ads_agg.date_day) as "month",
        datepart('year',ads_agg.date_day) as "year",
        ads_agg.spend_per_day,
        new_customers.num_new_customer

    from ads_agg

    left join new_customers
        on ads_agg.date_day = new_customers.ordered_on

    where date_day > '2021-02-28' and date_day < '2022-04-01'

    order by 1
),

final as (

    select
        "month",
        "year",
        sum(spend_per_day) as total_spend,
        sum(num_new_customer) as total_new_customers
        
    from ads_customers

    group by 2,1

    order by 2,1
)

select * from final