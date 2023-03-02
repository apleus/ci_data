SELECT
    date,
    product_id,
    rating,
    location
FROM {{ ref('stg__reviews') }}