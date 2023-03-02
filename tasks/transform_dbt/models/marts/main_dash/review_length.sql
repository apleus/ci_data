SELECT
    product_id,
    date,
    rating,
    LENGTH(body) AS rev_len
from {{ ref('stg__reviews') }}