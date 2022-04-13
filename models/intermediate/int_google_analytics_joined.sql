with analytics as (

    select
        *
    
    from {{ ref('stg_google_analytics__overview')}}
),

orders as (

    select
        *
    
    from {{ ref('int_orders_joined')}}
),

joins as (

    select
        analytics.transaction_id,
        analytics.date,
        orders.billing_city as city,
        orders.billing_state as region,
        analytics.source,
        analytics.medium,
        analytics.campaign,
        analytics.channel_grouping,
        analytics.ad_group,
        analytics.transactions,
        analytics.quantity,
        analytics.revenue

    from analytics

    left join orders
        on orders.media_id = analytics.transaction_id
)

select * from joins