with

----import CTEs

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

completed_payments as (
    select 
        order_id,
        max(payment_created_at) as payment_finalized_date,
        sum(payment_amount) as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1

),

 paid_orders as (

  select 
    orders.order_id,
    orders.customer_id,
    orders.order_placed_at,
    orders.order_status,

    completed_payments.total_amount_paid,
    completed_payments.payment_finalized_date,

    customers.customer_first_name,
    customers.customer_last_name
  from orders
  left join completed_payments on orders.order_id = completed_payments.order_id
  left join customers on orders.customer_id = customers.customer_id

),



customer_orders 
    as (select customers.id as customer_id,
        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders
    from customers  
    left join orders on orders.user_id = customers.id 
    group by 1)

select
    paid_orders.*,
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
    ---new vs returning customer
    case 
    when (
        rank() over (
            partition by customer_id
            order by order_placed_at, order_id
        ) = 1
    ) then 'new'
    else 'return' end as nvsr,
    x.clv_bad as customer_lifetime_value,
    -- first day of sale
    first_value(paid_orders.order_placed_at) over (
        partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
    ) as fdos
    from paid_orders 
    left join customer_orders as c using (customer_id)
    left outer join 
    (
            select
            paid_orders.order_id,
            sum(t2.total_amount_paid) as clv_bad
        from paid_orders 
        left join paid_orders t2 on paid_orders.customer_id = t2.customer_id and paid_orders.order_id >= t2.order_id
        group by 1
        order by paid_orders.order_id
    ) x on x.order_id = paid_orders.order_id
    order by order_id