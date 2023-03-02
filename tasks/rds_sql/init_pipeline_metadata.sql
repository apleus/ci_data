CREATE TABLE IF NOT EXISTS lake.pipeline_metadata (
    product_id varchar(12) NOT NULL,
    date char(8) NOT NULL,
    review_count int NOT NULL,
    status int NOT NULL,
    PRIMARY KEY (product_id, date, status)
);