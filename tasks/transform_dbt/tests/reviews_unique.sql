/* Every product_id + review_id combo must be unique. */

SELECT product_id, review_id, COUNT(*)
FROM {{ ref('stg__reviews')}}
GROUP BY product_id, review_id
HAVING COUNT(*) >= 2