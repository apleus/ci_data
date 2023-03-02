WITH src AS (
    SELECT *
    FROM {{ source('lake', 'products') }}
), temp as (
    SELECT
        product_id,
        brand,
        title
    FROM src
)
SELECT *
FROM temp