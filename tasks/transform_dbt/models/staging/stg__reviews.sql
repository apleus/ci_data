WITH src AS (
        SELECT *
        FROM {{ source('lake', 'reviews') }}
        ),
    temp AS (
        SELECT
            product_id,
            review_id,
            name,
            rating::INT,
            title,
            location,
            date::TIMESTAMP,
            other,
            verified::BOOLEAN,
            body
        FROM src
        )
SELECT *
FROM temp