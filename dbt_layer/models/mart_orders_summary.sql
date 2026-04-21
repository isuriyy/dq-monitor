SELECT
    order_date,
    order_status,
    COUNT(*)                                        AS total_orders,
    ROUND(SUM(order_total), 2)                      AS total_revenue,
    ROUND(AVG(order_total), 2)                      AS avg_order_value,
    COUNT(CASE WHEN total_flag != 'ok' THEN 1 END)  AS flagged_orders
FROM stg_orders
GROUP BY order_date, order_status
ORDER BY order_date DESC
