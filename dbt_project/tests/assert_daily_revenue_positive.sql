-- Custom test: every day in the mart must have positive total revenue
-- Catches days where all orders were cancelled/refunded unexpectedly

SELECT
    order_date,
    SUM(total_revenue) AS day_revenue
FROM {{ ref('mart_orders_summary') }}
GROUP BY order_date
HAVING SUM(total_revenue) <= 0
