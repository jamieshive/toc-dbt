with base as ( 

    select
        *
    
    from {{ ref('base_google_analytics__overview')}}
),

staged as (

    select
        transaction_id,
        "date",
        "source",
        "medium",
        campaign,
        channel_grouping,
        ad_group,
        transactions,
        item_quantity as quantity,
        transaction_revenue as revenue

    from base
)
select * from staged