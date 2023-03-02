SELECT
    date,
    product_id,
    rating,
    location,
    COUNT(rating) OVER(
        PARTITION BY product_id
        ORDER BY date
        ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING
    ) num_rating
FROM {{ ref('stg__reviews') }}