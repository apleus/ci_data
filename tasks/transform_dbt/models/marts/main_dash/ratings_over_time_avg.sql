-- TODO (extension): partition by date range (more complex in postgres...)
-- e.g. RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW

SELECT
    date,
    product_id,
    rating,
    location,
    AVG(rating) OVER(
        PARTITION BY product_id
        ORDER BY date
        ROWS BETWEEN 5 PRECEDING AND 0 FOLLOWING
    ) avg_rating
FROM {{ ref('stg__reviews') }}