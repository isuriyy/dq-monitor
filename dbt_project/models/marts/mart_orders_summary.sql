-- Business mart: daily order summary per status
-- Downstream dashboards and reports read from this model

SELECT
    order_date,
    order_status,
    COUNT(*)                                AS total_orders,
    SUM(order_total)                        AS total_revenue,
    AVG(order_total)                        AS avg_order_value,
    COUNT(CASE WHEN total_flag != 'ok' THEN 1 END) AS flagged_orders
FROM {{ ref('stg_orders') }}
GROUP BY order_date, order_status
ORDER BY order_date DESC
