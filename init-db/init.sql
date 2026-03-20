-- Create tables for the product catalog
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price NUMERIC(10, 2) NOT NULL,
    category_id INT REFERENCES categories(category_id)
);
-- REPLICA IDENTITY FULL is crucial! It ensures the WAL contains
-- the full previous state of a row for UPDATE and DELETE operations.
ALTER TABLE products REPLICA IDENTITY FULL;

CREATE TABLE inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INT UNIQUE REFERENCES products(product_id),
    quantity INT NOT NULL CHECK (quantity >= 0)
);
ALTER TABLE inventory REPLICA IDENTITY FULL;

-- Create a publication for all tables
-- This tells Postgres which tables' changes to send to subscribers.
CREATE PUBLICATION my_publication FOR ALL TABLES;
