/* For every product ID from reviews, there must be a corresponding product */
/* in the products table. */

SELECT *
FROM (
    SELECT rev.product_id
    FROM {{ ref('stg__reviews')}} rev
    LEFT JOIN {{ ref('stg__products')}} prod
    ON rev.product_id = prod.product_id
    WHERE prod.product_id IS NULL
    ) temp