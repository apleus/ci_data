SELECT
    product_id,
    location,
    COUNT(*) AS num_reviews
FROM {{ ref('stg__reviews') }}
GROUP BY
    product_id,
    location