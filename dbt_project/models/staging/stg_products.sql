-- Staging model: cleans raw products table

SELECT
    id                                      AS product_id,
    TRIM(name)                              AS product_name,
    TRIM(category)                          AS category,
    CAST(price AS DOUBLE)                   AS price,
    CAST(stock AS INTEGER)                  AS stock_qty,
    CASE
        WHEN price IS NULL OR price <= 0   THEN 'invalid_price'
        WHEN stock IS NULL                 THEN 'no_stock_info'
        ELSE 'ok'
    END                                     AS product_flag
FROM products
WHERE id IS NOT NULL
