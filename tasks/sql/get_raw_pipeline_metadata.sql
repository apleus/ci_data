SELECT MAX(date)
FROM pipeline_metadata
WHERE product_id = '{product_id}' AND status = 2;