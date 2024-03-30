with

-- Import CTEs

customers as (

  select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

  select * from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

  select * from {{ ref('stg_stripe__payments') }}

),


-- Logical CTEs

completed_payments as( -- pagos agregado a nivel de order. Distinto de fail

    select order_id,
        max(payment_created_at) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by order_id

),

paid_orders as ( -- completed_payments + customers. Granularidad ORDER
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.customer_first_name,
        c.customer_last_name
    FROM orders
        left join 
            completed_payments p
                ON orders.order_id = p.order_id
        left join customers C 
            on orders.customer_id = C.customer_id
),
-- customer_orders as ( -- customer con info agregada de ordenes. granularidad: CUSTOMER
--     select 
--         customer_id,
--         min(ORDER_DATE) as first_order_date,
--         max(ORDER_DATE) as most_recent_order_date,
--         count(ORDERS.order_id) AS number_of_orders
--     from customers C
--         left join orders as Orders on orders.USER_ID = C.customer_id
--     group by customer_id
-- ),
final_cte as (
    select p.*,
        -- seq. I cant see no diff w/ order_id
        ROW_NUMBER() OVER (
            ORDER BY p.order_id
        ) as transaction_seq,
        -- seq w/ orders by customer
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY p.order_id
        ) as customer_sales_seq,
        -- new vs returnining customer. return no es la palabra mas adecuada
        CASE
            WHEN ROW_NUMBER() over( 
                PARTITION by customer_id
                order by order_placed_at asc
                ) = 1 
            THEN 'new'
            ELSE 'return'
        END as nvsr,
        -- accumulated paids for a customer until this order (including actual one)
        sum(total_amount_paid) over(
            PARTITION by customer_id
            order by order_placed_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        as customer_lifetime_value,
        -- first order date per customer
        first_value(order_placed_at) over(
            PARTITION by customer_id 
            order by order_placed_at) 
        as fdos
    FROM paid_orders p
        -- left join customer_orders as c USING (customer_id)
        )
select * from final_cte
ORDER BY order_id