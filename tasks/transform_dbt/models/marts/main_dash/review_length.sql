SELECT
    r.product_id,
    r.date,
    r.rating,
    r.rev_len,
    p.brand,
    p.title
FROM (
    SELECT
        product_id,
        date,
        rating,
        LENGTH(body) AS rev_len
    from {{ ref('stg__reviews') }}
) r
    LEFT JOIN {{ ref('stg__products') }} p ON r.product_id = p.product_id