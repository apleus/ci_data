CREATE TABLE IF NOT EXISTS lake.reviews(
    product_id varchar(12) NOT NULL,
    review_id varchar(15) NOT NULL,
    name varchar(200) NOT NULL,
    rating int NOT NULL,
    title varchar(200) NOT NULL,
    location varchar(50) NOT NULL,
    date char(8) NOT NULL,
    other varchar(100),
    verified boolean NOT NULL,
    body varchar(30000),
    PRIMARY KEY (product_id, review_id)
);