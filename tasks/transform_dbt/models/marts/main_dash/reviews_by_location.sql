SELECT
    r.product_id,
    r.location,
    r.num_reviews,
    p.brand,
    p.title
FROM (
    SELECT
        product_id,
        location,
        COUNT(*) AS num_reviews
    FROM {{ ref('stg__reviews') }}
    GROUP BY product_id, location
    ) r
LEFT JOIN {{ ref('stg__products') }} p
ON r.product_id = p.product_id