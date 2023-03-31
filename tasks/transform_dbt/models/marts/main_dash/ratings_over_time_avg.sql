-- TODO (extension): partition by date range (more complex in postgres...)
-- e.g. RANGE BETWEEN INTERVAL '30' DAY PRECEDING AND CURRENT ROW

SELECT
    r.date,
    r.product_id,
    r.rating,
    r.location,
    r.avg_rating,
    r.short_name,
    p.brand,
    p.title
FROM (
    SELECT
        date,
        product_id,
        rating,
        location,
        AVG(rating) OVER (
            PARTITION BY product_id
            ORDER BY date
            ROWS BETWEEN 30 PRECEDING AND 0 FOLLOWING
            ) avg_rating,
        CASE
            WHEN product_id = 'B0B5W1YJN1' THEN 'K-Supreme'
            WHEN product_id = 'B09B1TDJTW' THEN 'VertuoNext'
            WHEN product_id = 'B01NBJ2UT5' THEN 'VertuoPlus'
            ELSE 'Error: Update product shorthand name!'
        END AS short_name
    FROM {{ ref('stg__reviews') }}
    ) r
LEFT JOIN {{ ref('stg__products') }} p
ON r.product_id = p.product_id