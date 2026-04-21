SELECT
    id                                          AS user_id,
    LOWER(TRIM(email))                          AS email,
    UPPER(TRIM(country))                        AS country_code,
    CAST(age AS INTEGER)                        AS age,
    CAST(joined AS DATE)                        AS joined_date,
    CASE
        WHEN email NOT LIKE '%@%'   THEN 'invalid_email'
        WHEN country IS NULL        THEN 'missing_country'
        ELSE                             'ok'
    END                                         AS user_flag
FROM users
WHERE id IS NOT NULL
