CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;

CREATE TEMP TABLE reviews_temp (LIKE lake.reviews);

SELECT
    aws_s3.table_import_from_s3(
        'reviews_temp',
        '',
        '(FORMAT csv, HEADER True)',
        '{bucket}',
        '{filename}',
        '{region}',
        '{access_key}',
        '{secret_key}'
    );
INSERT INTO lake.reviews

SELECT *
FROM reviews_temp
ON CONFLICT (product_id, review_id) DO NOTHING;

DROP TABLE reviews_temp;