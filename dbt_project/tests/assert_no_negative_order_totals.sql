-- Custom test: fails if ANY order has a negative total
-- A passing test returns 0 rows. Returning any rows = FAIL.

SELECT
    order_id,
    order_total
FROM {{ ref('stg_orders') }}
WHERE order_total < 0
