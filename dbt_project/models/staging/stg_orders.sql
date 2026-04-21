-- Staging model: cleans and renames raw orders table
-- Casts types, filters truly invalid rows, adds derived columns

SELECT
    id                                      AS order_id,
    user_id,
    product_id,
    CAST(total AS DOUBLE)                   AS order_total,
    LOWER(TRIM(status))                     AS order_status,
    CAST(created_at AS DATE)                AS order_date,
    CASE
        WHEN total IS NULL     THEN 'missing_total'
        WHEN total <= 0        THEN 'invalid_total'
        WHEN total > 50000     THEN 'suspicious_total'
        ELSE 'ok'
    END                                     AS total_flag
FROM orders
WHERE id IS NOT NULL
