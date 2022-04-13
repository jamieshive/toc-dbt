with base as (

    select
        *

    from {{ ref('base_imm__creative_raw_data')}}
),

staged as (

    select
        id,
        "brand column aq" as brand_column_aq,
        "product column an" as product_column_an,
        funnel2,
        case
            when
                "items sold" = 0
            then
                .00001
            else 
                "items sold"
        end as items_sold,
        x3,
        audience1,
        size2,
        case
            when
                impressions = 0
            then
                .00001
            else 
                impressions
        end as impressions,
        x2,
        "placement name" as placement_name,
        brand,
        "banner size" as banner_size,
        geo,
        "channel2 column al" as channel2_column_al,
        campaign,
        banner_name,
        case
            when
                "sales revenue" = 0
            then 
                .00001
            else
                "sales revenue"
        end as sales_revenue,
        case
            when
                clicks = 0
            then
                .00001
            else
                clicks
        end as clicks,
        site2,
        campaign2,
        theme2,
        customergrp,
        quarterlive,
        imagefocus,
        "funnel column av" as funnel_column_av,
        "ad group id" as ad_group_id,
        platform,
        channel,
        product,
        site,
        company2,
        "crtv+offer" as crtv_plus_offer,
        targetingtype,
        code1,
        banner_id,
        funnel,
        case
            when
                ctr = 0
            then
                .00001
            else
                ctr 
        end as ctr,
        case
            when    
                transactions = 0
            then
                .00001
            else
                transactions
        end as transactions,
        offer,
        color,
        "banner type" as banner_type,
        theme,
        x1,
        audience2,
        channel2,
        "placement id" as placement_id,
        "date"::date as "date"
    
    from base

)

select * from staged