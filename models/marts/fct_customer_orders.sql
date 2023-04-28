with

-- Import CTE's
customers as (

select * from {{ source('jaffle_shop', 'customers') }}

),

orders as (

select * from {{ source('jaffle_shop', 'orders') }}

),

base_payments as (

select * from {{ source('stripe', 'payment') }}

),

-- Logical CTE's

payments as (
    select 
        orderid as order_id,
        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid
    from base_payments
    where status <> 'fail'
    group by 1
),

paid_orders as (    
    select
        orders.id as order_id,
        orders.user_id as customer_id,
        orders.order_date as order_placed_at,
        orders.status as order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
        customers.first_name as customer_first_name,
        customers.last_name as customer_last_name
    from orders
    left join payments on orders.id = payments.order_id
left join customers on orders.user_id = customers.id ),

customer_lifetime_value as (
    select
        payments.order_id,
        sum(paid_orders.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders
        on payments.customer_id = paid_orders.customer_id 
        and payments.order_id >= paid_orders.order_id
    group by 1
    order by payments.order_id
),

-- Final CTE

final as (
    select
        payments.*,
        row_number() over (order by payments.order_id) as transaction_seq,
        row_number() over (partition by customer_id order by payments.order_id)
            as customer_sales_seq,

        case 
            when (
            rank() over (
                partition by customer_id
                order by order_placed_at. order_id
            ) = 1
            ) then 'new'
        else 'return' end as nvsr,

        customer_lifetime_value.clv_bad as customer_lifetime_value,
        first_value(paid_orders.order_placed_at) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
        ) as fdos
    
    from paid_orders p
    left outer join customer_lifetime_value
        on customer_lifetime_value.order_id = payments.order_id
    order by order_id
)

-- Simple Select Statement

select * from final