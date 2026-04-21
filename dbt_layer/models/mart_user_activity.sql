SELECT
    u.user_id,
    u.email,
    u.country_code,
    u.age,
    COUNT(o.order_id)               AS total_orders,
    ROUND(SUM(o.order_total), 2)    AS lifetime_value,
    MAX(o.order_date)               AS last_order_date,
    MIN(o.order_date)               AS first_order_date
FROM stg_users u
LEFT JOIN stg_orders o ON u.user_id = o.user_id
GROUP BY u.user_id, u.email, u.country_code, u.age
