{# Takes base order source code view, renames columns, and removes unnecessary columns #}

with source as (
        
    select * from {{ ref('base_cms__order_source_codes') }}
        
),

staged as (

    select
        ordersrc_id as order_source_number,
        name as order_source_name,
        case
            when
                order_source_number = '57' 
                or order_source_number = '5' 
                or order_source_number = '70'
                or order_source_number = '54'
                or order_source_number = '13'
                or order_source_number = '52'
                or order_source_number = '12'
            then
                'Tastes of Chicago'
            when 
                order_source_number = '41' 
                or order_source_number = '103'
            then 
                'Amazon'
            when 
                order_source_number = '105'
            then 
                'Goldbelly'   
            when
                order_source_number = '80'
                or order_source_number = '86'
                or order_source_number = '85'
                or order_source_number = '88'
            then 
                'Wholesale Order'
            when
                order_source_number = '42'
            then 
                'Corporate Import'  
            else
                'Phoned In'
        end as rename,
        case
        when rename = 'Tastes of Chicago'
            then 'Web'
        when rename = 'Phoned In'
            then 'CallCenter'
        when rename = 'Corporate Import'
            then 'Corporate'
        when rename = 'Goldbelly'
            then 'Goldbelly'
        when rename = 'Amazon'
            then 'Amazon'
        when rename = 'Wholesale Order'
            then 'Wholesale/Misc'
        else 'Unknown'
    end as revenue_segment,
    case 
        when revenue_segment = 'Web'
            then 'Web'
        when revenue_segment = 'CallCenter' or revenue_segment = 'Corporate'
            then 'Concierge'
        when revenue_segment = 'Goldbelly' or revenue_segment = 'Amazon' or revenue_segment = 'Wholesale/Misc'
            then 'Marketplaces'
        else 'Unknown'
    end as revenue_group

    from source
)

select * from staged