SELECT
    date,
    product_id,
    rating,
    location
    AVG(rating) OVER(
        PARTITION BY product_id
        ORDER BY date
        RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW
    ) avg_rating
FROM {{ ref('stg__reviews') }}