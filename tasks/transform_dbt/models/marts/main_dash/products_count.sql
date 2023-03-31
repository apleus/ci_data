SELECT
    p.product_id,
    p.brand,
    p.title,
    r.count
FROM {{ ref('stg__products') }} p
LEFT JOIN (
    SELECT
        product_id,
        COUNT(*) count
    FROM {{ ref('stg__reviews')}}
    GROUP BY product_id
    ) r
ON p.product_id = r.product_id