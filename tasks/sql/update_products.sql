INSERT INTO products (product_id, brand, title)
    VALUES ('{product_id}', '{brand}', '{title}')
ON CONFLICT (product_id) DO NOTHING;