CREATE TABLE IF NOT EXISTS products (
    product_id varchar(12) NOT NULL PRIMARY KEY,
    brand varchar(200) NOT NULL,
    title varchar(200) NOT NULL
);

INSERT INTO products (product_id, brand, title)
    VALUES ('{product_id}', '{brand}', '{title}')
ON CONFLICT (product_id) DO NOTHING;