{# Takes base CMS orders view, renames columns, and removes unnecessary columns #}

with source as (
        
    select * from {{ ref('base_cms__orders') }}
        
),
    
staged as (
        
    select
        order_id,
        media_id,
        customer_id,
        cast(item_total as bigint) as item_total,
        shipping_total,
        discount_total,
        cast(state_tax_total as bigint) as state_tax_total,
        order_total,
        billing_address1,
        billing_address2,
        billing_city,
        billing_state,
        billing_zip,
        CAST(address_concat as varchar) as address_concat,
        was_cancelled,
        CAST(ordered_on as date) as ordered_on
        
    from source
    
),

order_src_adjust as (

    select
        order_id,
        order_src as order_src_mixed,
        case
            when 
                order_src_mixed = '57' and adcode = 'GOLDBELY'
            then '105'
            when
                order_src_mixed = '95'
            then '41'
            when
                order_src_mixed = '106' or order_src_mixed = '108'
            then '57'
            else order_src_mixed
        end as order_source_number,
        adcode

    from source
),

final as (

    select
        staged.*,
        cast(order_src_adjust.order_source_number as varchar),
        order_src_adjust.adcode

    from staged

    left join order_src_adjust
        on staged.order_id = order_src_adjust.order_id

)
    
select * from final