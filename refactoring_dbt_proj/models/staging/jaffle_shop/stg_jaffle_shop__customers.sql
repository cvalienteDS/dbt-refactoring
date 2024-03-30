with 

-- Import CTEs

base_customers as (

  select * from {{ source('jaffle_shop', 'customers') }}

),

transformed as (

    select
        id as customer_id,
        FIRST_NAME as customer_first_name,
        LAST_NAME as customer_last_name,
        first_name || ' ' || last_name as full_name
    from base_customers

)

select * from transformed