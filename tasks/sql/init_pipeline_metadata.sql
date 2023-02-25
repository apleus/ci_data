CREATE TABLE IF NOT EXISTS pipeline_metadata (
    product_id varchar(12) NOT NULL,
    date char(8) NOT NULL,
    review_count int NOT NULL,
    status int NOT NULL,
    PRIMARY KEY (product_id, date, status)
);

SELECT (review_count) FROM (
    SELECT * FROM pipeline_metadata
    WHERE product_id='{product_id}' AND status=4
) filtered_metadata
    ORDER BY date
    DESC LIMIT 1;