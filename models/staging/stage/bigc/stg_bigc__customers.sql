{# Takes base customer view, renames columns, and removes unnecessary columns #}

with customers as (
        
    select * from {{ ref('base_bigc__customers') }}
        
),

shipments as (

    select * from {{ ref('stg_bigc__shipments')}}

),

cust_id_info as (

    select 
        distinct shipments.customer_id as id,
        customers.first_name,
        customers.last_name,
        customers.email,
        customers.phone,
        customers.company
    
    from shipments

    left join customers
        on shipments.email = customers.email

),
    
staged as (
        
    select
        id as customer_id,
        first_name,
        last_name,
        email,
        phone,
        company
        
    from cust_id_info
    
)
    
select * from staged