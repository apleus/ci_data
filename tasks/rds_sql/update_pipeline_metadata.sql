INSERT INTO lake.pipeline_metadata (
    product_id,
    date,
    review_count,
    status
) VALUES (
    '{product_id}',
    '{date}',
    '{review_count}',
    '{status}'
) ON CONFLICT (product_id, date, status) DO NOTHING;