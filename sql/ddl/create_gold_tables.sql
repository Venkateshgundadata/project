-- E-Commerce Gold Layer Schema
-- Database: ecommerce_gold
-- Purpose: Business-ready aggregated data for analytics

-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_daily_sales CASCADE;
DROP TABLE IF EXISTS fact_customer_orders CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS agg_campaign_performance CASCADE;
DROP TABLE IF EXISTS agg_inventory_status CASCADE;

-- DIMENSION: Products (SCD Type 1 - Current state only)
CREATE TABLE dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(255),
    sku VARCHAR(100),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    current_price DECIMAL(10, 2),
    current_cost DECIMAL(10, 2),
    is_active BOOLEAN,
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id)
);

CREATE INDEX idx_dim_products_category ON dim_products(category);
CREATE INDEX idx_dim_products_brand ON dim_products(brand);

-- DIMENSION: Customers (SCD Type 1 - Current state only)
CREATE TABLE dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    email VARCHAR(255),
    full_name VARCHAR(255),
    customer_segment VARCHAR(50),
    loyalty_tier VARCHAR(20),
    country VARCHAR(100),
    city VARCHAR(100),
    registration_date TIMESTAMP,
    account_status VARCHAR(20),
    effective_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id)
);

CREATE INDEX idx_dim_customers_segment ON dim_customers(customer_segment);
CREATE INDEX idx_dim_customers_tier ON dim_customers(loyalty_tier);

-- FACT: Daily Sales Aggregation
CREATE TABLE fact_daily_sales (
    sales_date DATE NOT NULL,
    category VARCHAR(100),
    total_orders INTEGER,
    total_items_sold INTEGER,
    total_revenue DECIMAL(15, 2),
    total_discount DECIMAL(12, 2),
    total_tax DECIMAL(12, 2),
    avg_order_value DECIMAL(10, 2),
    unique_customers INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sales_date, category)
);

CREATE INDEX idx_fact_daily_sales_date ON fact_daily_sales(sales_date);

-- FACT: Customer Orders (Detailed)
CREATE TABLE fact_customer_orders (
    order_id INTEGER PRIMARY KEY,
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    order_date TIMESTAMP,
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    total_amount DECIMAL(12, 2),
    discount_amount DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    shipping_cost DECIMAL(8, 2),
    total_items INTEGER,
    payment_status VARCHAR(50),
    fulfillment_date TIMESTAMP,
    days_to_fulfill INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_orders_customer ON fact_customer_orders(customer_key);
CREATE INDEX idx_fact_orders_date ON fact_customer_orders(order_date);
CREATE INDEX idx_fact_orders_status ON fact_customer_orders(order_status);

-- AGGREGATE: Campaign Performance
CREATE TABLE agg_campaign_performance (
    campaign_id INTEGER PRIMARY KEY,
    campaign_name VARCHAR(255),
    channel VARCHAR(50),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12, 2),
    target_segment VARCHAR(100),
    total_impressions INTEGER,
    total_clicks INTEGER,
    total_conversions INTEGER,
    total_revenue DECIMAL(12, 2),
    roi_percentage DECIMAL(8, 2),
    ctr DECIMAL(5, 2), -- Click-through rate
    conversion_rate DECIMAL(5, 2),
    cost_per_click DECIMAL(8, 2),
    cost_per_conversion DECIMAL(8, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_agg_campaigns_channel ON agg_campaign_performance(channel);
CREATE INDEX idx_agg_campaigns_dates ON agg_campaign_performance(start_date, end_date);

-- AGGREGATE: Inventory Status
CREATE TABLE agg_inventory_status (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    current_stock INTEGER,
    reorder_level INTEGER,
    stock_status VARCHAR(50), -- 'In Stock', 'Low Stock', 'Out of Stock', 'Overstock'
    total_in INTEGER,
    total_out INTEGER,
    total_adjustments INTEGER,
    last_transaction_date TIMESTAMP,
    days_since_last_movement INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_agg_inventory_category ON agg_inventory_status(category);
CREATE INDEX idx_agg_inventory_status ON agg_inventory_status(stock_status);

-- Add comments
COMMENT ON TABLE dim_products IS 'Product dimension table with current state';
COMMENT ON TABLE dim_customers IS 'Customer dimension table with current state';
COMMENT ON TABLE fact_daily_sales IS 'Daily sales aggregation by category';
COMMENT ON TABLE fact_customer_orders IS 'Detailed order facts for customer analysis';
COMMENT ON TABLE agg_campaign_performance IS 'Marketing campaign performance metrics';
COMMENT ON TABLE agg_inventory_status IS 'Current inventory status and movement summary';
