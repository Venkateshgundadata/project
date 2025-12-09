-- E-Commerce Source Database Schema
-- Database: ecommerce_source

-- Drop existing tables if they exist
DROP TABLE IF EXISTS inventory_transactions CASCADE;
DROP TABLE IF EXISTS marketing_campaigns CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS customers CASCADE;

-- 1. CUSTOMERS TABLE (100k rows)
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    date_of_birth DATE,
    registration_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    customer_segment VARCHAR(50), -- 'Premium', 'Regular', 'New'
    loyalty_tier VARCHAR(20), -- 'Gold', 'Silver', 'Bronze', 'None'
    country VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    marketing_consent BOOLEAN DEFAULT FALSE,
    last_login_date TIMESTAMP,
    account_status VARCHAR(20) DEFAULT 'Active', -- 'Active', 'Inactive', 'Suspended'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_segment ON customers(customer_segment);
CREATE INDEX idx_customers_registration_date ON customers(registration_date);

-- 2. PRODUCTS TABLE (50k rows)
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    sku VARCHAR(100) UNIQUE NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    cost DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    reorder_level INTEGER DEFAULT 10,
    supplier_id INTEGER,
    weight_kg DECIMAL(8, 2),
    dimensions_cm VARCHAR(50), -- 'LxWxH'
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_price_positive CHECK (price >= 0),
    CONSTRAINT chk_cost_positive CHECK (cost >= 0),
    CONSTRAINT chk_stock_non_negative CHECK (stock_quantity >= 0)
);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_brand ON products(brand);
CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_products_is_active ON products(is_active);

-- 3. ORDERS TABLE (500k rows)
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(50) NOT NULL, -- 'Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled'
    payment_method VARCHAR(50), -- 'Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer'
    shipping_address VARCHAR(500),
    billing_address VARCHAR(500),
    total_amount DECIMAL(12, 2) NOT NULL,
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    tax_amount DECIMAL(10, 2) DEFAULT 0,
    shipping_cost DECIMAL(8, 2) DEFAULT 0,
    currency VARCHAR(3) DEFAULT 'USD',
    payment_status VARCHAR(50) DEFAULT 'Pending', -- 'Pending', 'Paid', 'Failed', 'Refunded'
    fulfillment_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_total_amount_positive CHECK (total_amount >= 0)
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(order_status);
CREATE INDEX idx_orders_payment_status ON orders(payment_status);

-- 4. ORDER_ITEMS TABLE (1.5M rows)
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_percentage DECIMAL(5, 2) DEFAULT 0,
    line_total DECIMAL(12, 2) NOT NULL,
    tax_amount DECIMAL(10, 2) DEFAULT 0,
    return_status VARCHAR(50) DEFAULT 'None', -- 'None', 'Requested', 'Approved', 'Completed'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_unit_price_positive CHECK (unit_price >= 0),
    CONSTRAINT chk_discount_valid CHECK (discount_percentage >= 0 AND discount_percentage <= 100)
);

CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);

-- 5. INVENTORY_TRANSACTIONS TABLE (200k rows)
CREATE TABLE inventory_transactions (
    transaction_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    warehouse_id INTEGER,
    transaction_type VARCHAR(20) NOT NULL, -- 'IN', 'OUT', 'ADJUSTMENT'
    quantity INTEGER NOT NULL,
    transaction_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reference_order_id INTEGER REFERENCES orders(order_id),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_inventory_product_id ON inventory_transactions(product_id);
CREATE INDEX idx_inventory_transaction_date ON inventory_transactions(transaction_date);
CREATE INDEX idx_inventory_type ON inventory_transactions(transaction_type);

-- 6. MARKETING_CAMPAIGNS TABLE (10k rows)
CREATE TABLE marketing_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    channel VARCHAR(50) NOT NULL, -- 'EMAIL', 'SMS', 'SOCIAL', 'DISPLAY', 'SEARCH'
    start_date DATE NOT NULL,
    end_date DATE,
    budget DECIMAL(12, 2),
    target_segment VARCHAR(100), -- 'All', 'Premium', 'Regular', 'New'
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    conversions INTEGER DEFAULT 0,
    revenue_generated DECIMAL(12, 2) DEFAULT 0,
    roi DECIMAL(8, 2), -- Return on Investment percentage
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_budget_positive CHECK (budget >= 0),
    CONSTRAINT chk_metrics_non_negative CHECK (impressions >= 0 AND clicks >= 0 AND conversions >= 0)
);

CREATE INDEX idx_campaigns_channel ON marketing_campaigns(channel);
CREATE INDEX idx_campaigns_start_date ON marketing_campaigns(start_date);
CREATE INDEX idx_campaigns_target_segment ON marketing_campaigns(target_segment);

-- Add comments for documentation
COMMENT ON TABLE customers IS 'Customer master data with demographic and behavioral information';
COMMENT ON TABLE products IS 'Product catalog with pricing and inventory information';
COMMENT ON TABLE orders IS 'Order header information including payment and shipping details';
COMMENT ON TABLE order_items IS 'Line items for each order with product details and pricing';
COMMENT ON TABLE inventory_transactions IS 'Inventory movement tracking for stock management';
COMMENT ON TABLE marketing_campaigns IS 'Marketing campaign performance metrics';
