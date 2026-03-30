-- 1. Drop tables one by one with CASCADE
DROP TABLE IF EXISTS customers, products, orders, order_items, payments, inventory, categories CASCADE;

-- 2. Create tables one by one
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    category_id INTEGER REFERENCES categories(category_id),
    price DECIMAL(10, 2)
);

CREATE TABLE inventory (
    product_id INTEGER PRIMARY KEY REFERENCES products(product_id),
    stock_quantity INTEGER DEFAULT 100,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    shop_id INTEGER DEFAULT 1, -- 사용자가 요청한 shop_id 추가 완료!
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER,
    price DECIMAL(10, 2)
);

CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    payment_method VARCHAR(20),
    amount DECIMAL(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Set Replica Identity to FULL (for CDC)
ALTER TABLE categories REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE inventory REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE payments REPLICA IDENTITY FULL;

-- 4. Insert Seed Data
INSERT INTO categories (name) VALUES ('Food'), ('Beverage'), ('Dessert');

INSERT INTO customers (first_name, last_name, email) VALUES
('Michael', 'Phelps', 'michael@example.com'),
('Serena', 'Williams', 'serena@example.com'),
('Usain', 'Bolt', 'usain@example.com'),
('Roger', 'Federer', 'roger@example.com'),
('Tiger', 'Woods', 'tiger@example.com'),
('Lionel', 'Messi', 'lionel@example.com'),
('Cristiano', 'Ronaldo', 'christiano@example.com'),
('LeBron', 'James', 'lebron@example.com'),
('Lewis', 'Hamilton', 'lewis@example.com'),
('Tom', 'Brady', 'tom@example.com');

INSERT INTO products (name, category_id, price) VALUES
('Jaffle: Cheese & Ham', 1, 12.50),
('Jaffle: Chicken & Avocado', 1, 14.00),
('Espresso', 2, 3.50),
('Latte', 2, 4.50),
('Cookie: Choco Chip', 3, 2.50);

INSERT INTO inventory (product_id, stock_quantity)
SELECT product_id, 100 FROM products;

-- 5. Create Indexes for Time-based Queries (Spark Watermark Event Time)
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_payments_created_at ON payments(created_at);