with

----import CTEs

customers as (
    select * from {{ ref('customers') }}
),

orders as (
    select * from {{ ref('orders') }}
),

payments as (
    select * from {{ ref('payments') }}
),

-- Logical CTEs
-- Final CTE
-- Simple Select Statment

completed_payments as (
    select orderid as order_id,
         max(created) as payment_finalized_date,
          sum(amount) / 100.0 as total_amount_paid
        from payments
        where status <> 'fail'
        group by 1

),

 paid_orders as (
    select orders.id as order_id,
        orders.user_id    as customer_id,
        orders.order_date as order_placed_at,
            orders.status as order_status,
        completed_payments.total_amount_paid,
        completed_payments.payment_finalized_date,
        customers.first_name    as customer_first_name,
            customers.last_name as customer_last_name
    from orders
    left join completed_payments on orders.id = completed_payments.order_id
    left join customers on orders.user_id = customers.id ),

customer_orders 
    as (select customers.id as customer_id,
        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.id) as number_of_orders
    from customers  
    left join orders on orders.user_id = customers.id 
    group by 1)

select
    completed_payments.*,
    row_number() over (order by completed_payments.order_id) as transaction_seq,
    row_number() over (partition by customer_id order by completed_payments.order_id) as customer_sales_seq,
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
    from paid_orders completed_payments
    left join customer_orders as c using (customer_id)
    left outer join 
    (
            select
            completed_payments.order_id,
            sum(t2.total_amount_paid) as clv_bad
        from paid_orders p
        left join paid_orders t2 on completed_payments.customer_id = t2.customer_id and completed_payments.order_id >= t2.order_id
        group by 1
        order by p.order_id
    ) x on x.order_id = completed_payments.order_id
    order by order_id