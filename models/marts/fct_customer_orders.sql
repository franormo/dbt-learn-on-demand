with 
--imports CTEs
customers as (
    select * from {{ source('jaffle_shop', 'customers') }}
),

orders as (
    select * from {{ source('jaffle_shop', 'orders') }}
),

payments as (
    select * from {{ source('stripe', 'payments') }}
    where STATUS <> 'fail'
),

--logical CTEs
order_total_payments as (
    select 
        ORDERID as order_id, 
        max(CREATED) as payment_finalized_date, 
        sum(AMOUNT) / 100.0 as total_amount_paid
    from payments
    group by 1
),

paid_orders as (
    select 
        Orders.ID as order_id,
        Orders.USER_ID	as customer_id,
        Orders.ORDER_DATE as order_placed_at,
        Orders.STATUS as order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        C.FIRST_NAME    as customer_first_name,
        C.LAST_NAME as customer_last_name
    from orders
    left join order_total_payments p 
    on orders.ID = p.order_id
    left join customers C 
    on orders.USER_ID = C.ID 
),


final as (
    select
        paid_orders.*,

        ROW_NUMBER() over (
            order by paid_orders.order_id
        ) as transaction_seq,

        ROW_NUMBER() over (
            partition by paid_orders.customer_id 
            order by paid_orders.order_id
        ) as customer_sales_seq,

        case 
            when 
                rank() over(
                    partition by paid_orders.order_id 
                    order by paid_orders.order_placed_at
                ) = 1 then 'new'
            else 'return' 
            end as nvsr,

        sum(total_amount_paid) over (
            partition by paid_orders.customer_id
            order by paid_orders.order_placed_at
        ) as customer_lifetime_value,

        first_value(paid_orders.order_placed_at) over (
            partition by paid_orders.order_id 
            order by paid_orders.order_placed_at
        ) as fdos
    from paid_orders
    order by order_id
)

select * from final