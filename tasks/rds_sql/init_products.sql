CREATE TABLE IF NOT EXISTS lake.products (
    product_id varchar(12) NOT NULL PRIMARY KEY,
    brand varchar(200) NOT NULL,
    title varchar(200) NOT NULL
);