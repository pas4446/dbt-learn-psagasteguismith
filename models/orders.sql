/*- STEP 1: LOOK AT THE DATA -> what's funny about it? what are the gotchas? what are my asusmptions about this data?

- STEP 2 (right-to-left approach / Diego's approach):
  - create an orders.sql file
  - use CTEs to break up any transofrmations
  - then refactor into separate files

- STEP 2 (left to right approach / Benjamin):
  - create the staging model first (following existing patterns)
  - create an orders model next
*/
with order_customer AS (
    SELECT DISTINCT ID AS order_id,
                    user_id
    -- FROM raw.jaffle_shop.orders
    from {{ source('jaffle_shop', 'orders') }}
),

payments AS (
    SELECT orderid AS order_id,
            SUM(amount/100) AS amount --amounts are stored raw as cents, convert to dollars
    --FROM raw.stripe.payment
    from {{ source('stripe', 'payment') }}
    WHERE status = 'success'        --only want successful payments
    GROUP BY order_id
),

final AS (
    select payments.*,
            order_customer.user_id
    from payments
    LEFT JOIN order_customer 
    ON payments.order_id = order_customer.order_id
)
/*select sum(amount) from final*/
select order_id,
        user_id AS customer_id,
        amount 
from final