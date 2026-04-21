-- Custom test: flags users with abnormally high order counts
-- Could indicate bot activity or data duplication

SELECT
    user_id,
    total_orders
FROM {{ ref('mart_user_activity') }}
WHERE total_orders > 500
