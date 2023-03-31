SELECT date, review_count
FROM lake.pipeline_metadata
WHERE product_id = '{product_id}'
AND status = '{status}'
ORDER BY date DESC LIMIT 1;