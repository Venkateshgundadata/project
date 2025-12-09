# E-Commerce Data Pipeline - Data Dictionary

## Overview

This document provides detailed information about all datasets, their schemas, relationships, and business rules.

## Entity Relationship Diagram

```
┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│  CUSTOMERS   │       │    ORDERS    │       │   PRODUCTS   │
├──────────────┤       ├──────────────┤       ├──────────────┤
│ customer_id  │◄──────│ customer_id  │       │ product_id   │
│ email        │       │ order_id     │       │ product_name │
│ first_name   │       │ order_date   │       │ sku          │
│ last_name    │       │ order_status │       │ category     │
│ segment      │       │ total_amount │       │ price        │
└──────────────┘       └──────┬───────┘       └──────▲───────┘
                              │                      │
                              │                      │
                              ▼                      │
                       ┌──────────────┐              │
                       │ ORDER_ITEMS  │              │
                       ├──────────────┤              │
                       │ order_id     │──────────────┘
                       │ product_id   │
                       │ quantity     │
                       │ unit_price   │
                       └──────────────┘

┌──────────────────────┐       ┌──────────────────────┐
│ INVENTORY_TRANS      │       │ MARKETING_CAMPAIGNS  │
├──────────────────────┤       ├──────────────────────┤
│ transaction_id       │       │ campaign_id          │
│ product_id           │───┐   │ campaign_name        │
│ transaction_type     │   │   │ channel              │
│ quantity             │   │   │ budget               │
│ transaction_date     │   │   │ roi                  │
└──────────────────────┘   │   └──────────────────────┘
                           │
                           └──▶ PRODUCTS
```

## Source Tables

### 1. CUSTOMERS

**Purpose:** Master data for customer information and demographics

**Row Count:** 100,000

| Column Name | Data Type | Nullable | Description | Example Values |
|------------|-----------|----------|-------------|----------------|
| customer_id | SERIAL | NO | Primary key, auto-increment | 1, 2, 3... |
| email | VARCHAR(255) | NO | Unique email address | john.doe@email.com |
| first_name | VARCHAR(100) | NO | Customer first name | John |
| last_name | VARCHAR(100) | NO | Customer last name | Doe |
| phone | VARCHAR(20) | YES | Contact phone number | +1-555-0123 |
| date_of_birth | DATE | YES | Date of birth | 1985-06-15 |
| registration_date | TIMESTAMP | NO | Account creation timestamp | 2023-01-15 10:30:00 |
| customer_segment | VARCHAR(50) | YES | Business segment | Premium, Regular, New |
| loyalty_tier | VARCHAR(20) | YES | Loyalty program tier | Gold, Silver, Bronze, None |
| country | VARCHAR(100) | YES | Country of residence | United States |
| city | VARCHAR(100) | YES | City of residence | New York |
| postal_code | VARCHAR(20) | YES | Postal/ZIP code | 10001 |
| marketing_consent | BOOLEAN | YES | Marketing email consent | TRUE, FALSE |
| last_login_date | TIMESTAMP | YES | Last login timestamp | 2024-11-20 14:22:00 |
| account_status | VARCHAR(20) | YES | Account status | Active, Inactive, Suspended |

**Business Rules:**
- Email must be unique
- Registration date cannot be in the future
- Customer segment is assigned based on purchase history
- Loyalty tier is calculated based on total spend

**Indexes:**
- PRIMARY KEY on customer_id
- UNIQUE INDEX on email
- INDEX on customer_segment
- INDEX on registration_date

---

### 2. PRODUCTS

**Purpose:** Product catalog with pricing and inventory information

**Row Count:** 50,000

| Column Name | Data Type | Nullable | Description | Example Values |
|------------|-----------|----------|-------------|----------------|
| product_id | SERIAL | NO | Primary key, auto-increment | 1, 2, 3... |
| product_name | VARCHAR(255) | NO | Product name | Wireless Bluetooth Headphones |
| sku | VARCHAR(100) | NO | Stock keeping unit (unique) | SKU-1234-ABCD |
| category | VARCHAR(100) | NO | Product category | Electronics, Clothing, Home & Garden |
| subcategory | VARCHAR(100) | YES | Product subcategory | Smartphones, Men, Furniture |
| brand | VARCHAR(100) | YES | Brand name | BrandA, BrandB |
| price | DECIMAL(10,2) | NO | Current selling price | 99.99 |
| cost | DECIMAL(10,2) | NO | Cost of goods sold | 45.00 |
| stock_quantity | INTEGER | YES | Current stock level | 150 |
| reorder_level | INTEGER | YES | Minimum stock threshold | 20 |
| supplier_id | INTEGER | YES | Supplier identifier | 42 |
| weight_kg | DECIMAL(8,2) | YES | Product weight in kg | 0.5 |
| dimensions_cm | VARCHAR(50) | YES | L x W x H in cm | 20x15x5 |
| is_active | BOOLEAN | YES | Product active status | TRUE, FALSE |
| created_date | TIMESTAMP | YES | Product creation date | 2023-01-01 00:00:00 |
| last_updated | TIMESTAMP | YES | Last update timestamp | 2024-11-20 10:00:00 |

**Business Rules:**
- SKU must be unique
- Price must be greater than or equal to cost
- Stock quantity cannot be negative
- Products with stock below reorder_level trigger alerts

**Indexes:**
- PRIMARY KEY on product_id
- UNIQUE INDEX on sku
- INDEX on category
- INDEX on brand
- INDEX on is_active

---

### 3. ORDERS

**Purpose:** Order header information including payment and shipping details

**Row Count:** 500,000

| Column Name | Data Type | Nullable | Description | Example Values |
|------------|-----------|----------|-------------|----------------|
| order_id | SERIAL | NO | Primary key, auto-increment | 1, 2, 3... |
| customer_id | INTEGER | NO | Foreign key to customers | 12345 |
| order_date | TIMESTAMP | NO | Order placement timestamp | 2024-11-20 15:30:00 |
| order_status | VARCHAR(50) | NO | Current order status | Pending, Processing, Shipped, Delivered, Cancelled |
| payment_method | VARCHAR(50) | YES | Payment method used | Credit Card, PayPal, Bank Transfer |
| shipping_address | VARCHAR(500) | YES | Shipping address | 123 Main St, New York, NY 10001 |
| billing_address | VARCHAR(500) | YES | Billing address | 123 Main St, New York, NY 10001 |
| total_amount | DECIMAL(12,2) | NO | Total order value | 299.99 |
| discount_amount | DECIMAL(10,2) | YES | Discount applied | 29.99 |
| tax_amount | DECIMAL(10,2) | YES | Tax amount | 24.00 |
| shipping_cost | DECIMAL(8,2) | YES | Shipping cost | 9.99 |
| currency | VARCHAR(3) | YES | Currency code | USD, EUR, GBP |
| payment_status | VARCHAR(50) | YES | Payment status | Pending, Paid, Failed, Refunded |
| fulfillment_date | TIMESTAMP | YES | Order fulfillment date | 2024-11-22 10:00:00 |

**Business Rules:**
- Customer must exist in customers table
- Total amount must be positive
- Fulfillment date must be after order date
- Cancelled orders cannot be fulfilled

**Indexes:**
- PRIMARY KEY on order_id
- FOREIGN KEY on customer_id
- INDEX on order_date
- INDEX on order_status
- INDEX on payment_status

---

### 4. ORDER_ITEMS

**Purpose:** Line items for each order with product details and pricing

**Row Count:** 1,500,000 (avg 3 items per order)

| Column Name | Data Type | Nullable | Description | Example Values |
|------------|-----------|----------|-------------|----------------|
| order_item_id | SERIAL | NO | Primary key, auto-increment | 1, 2, 3... |
| order_id | INTEGER | NO | Foreign key to orders | 12345 |
| product_id | INTEGER | NO | Foreign key to products | 6789 |
| quantity | INTEGER | NO | Quantity ordered | 2 |
| unit_price | DECIMAL(10,2) | NO | Price per unit at time of order | 49.99 |
| discount_percentage | DECIMAL(5,2) | YES | Discount percentage | 10.00 |
| line_total | DECIMAL(12,2) | NO | Line item total | 89.98 |
| tax_amount | DECIMAL(10,2) | YES | Tax for this line | 7.20 |
| return_status | VARCHAR(50) | YES | Return status | None, Requested, Approved, Completed |

**Business Rules:**
- Order and product must exist
- Quantity must be positive
- Line total = (unit_price × quantity) × (1 - discount_percentage/100)
- Discount percentage must be between 0 and 100

**Indexes:**
- PRIMARY KEY on order_item_id
- FOREIGN KEY on order_id (CASCADE DELETE)
- FOREIGN KEY on product_id
- INDEX on order_id
- INDEX on product_id

---

### 5. INVENTORY_TRANSACTIONS

**Purpose:** Inventory movement tracking for stock management

**Row Count:** 200,000

| Column Name | Data Type | Nullable | Description | Example Values |
|------------|-----------|----------|-------------|----------------|
| transaction_id | SERIAL | NO | Primary key, auto-increment | 1, 2, 3... |
| product_id | INTEGER | NO | Foreign key to products | 6789 |
| warehouse_id | INTEGER | YES | Warehouse identifier | 1, 2, 3 |
| transaction_type | VARCHAR(20) | NO | Type of transaction | IN, OUT, ADJUSTMENT |
| quantity | INTEGER | NO | Quantity moved (+ or -) | 100, -50 |
| transaction_date | TIMESTAMP | NO | Transaction timestamp | 2024-11-20 09:00:00 |
| reference_order_id | INTEGER | YES | Related order ID if OUT | 12345 |
| notes | TEXT | YES | Additional notes | Restocking from supplier |

**Business Rules:**
- Product must exist
- IN transactions have positive quantity
- OUT transactions have negative quantity
- ADJUSTMENT can be positive or negative

**Indexes:**
- PRIMARY KEY on transaction_id
- FOREIGN KEY on product_id
- FOREIGN KEY on reference_order_id
- INDEX on product_id
- INDEX on transaction_date
- INDEX on transaction_type

---

### 6. MARKETING_CAMPAIGNS

**Purpose:** Marketing campaign performance metrics

**Row Count:** 10,000

| Column Name | Data Type | Nullable | Description | Example Values |
|------------|-----------|----------|-------------|----------------|
| campaign_id | SERIAL | NO | Primary key, auto-increment | 1, 2, 3... |
| campaign_name | VARCHAR(255) | NO | Campaign name | Black Friday 2024 |
| channel | VARCHAR(50) | NO | Marketing channel | EMAIL, SMS, SOCIAL, DISPLAY, SEARCH |
| start_date | DATE | NO | Campaign start date | 2024-11-01 |
| end_date | DATE | YES | Campaign end date | 2024-11-30 |
| budget | DECIMAL(12,2) | YES | Campaign budget | 50000.00 |
| target_segment | VARCHAR(100) | YES | Target customer segment | Premium, All |
| impressions | INTEGER | YES | Total impressions | 1000000 |
| clicks | INTEGER | YES | Total clicks | 50000 |
| conversions | INTEGER | YES | Total conversions | 2500 |
| revenue_generated | DECIMAL(12,2) | YES | Revenue attributed | 125000.00 |
| roi | DECIMAL(8,2) | YES | Return on investment % | 150.00 |

**Business Rules:**
- End date must be after start date
- Budget must be non-negative
- ROI = ((revenue - budget) / budget) × 100
- Clicks cannot exceed impressions
- Conversions cannot exceed clicks

**Indexes:**
- PRIMARY KEY on campaign_id
- INDEX on channel
- INDEX on start_date
- INDEX on target_segment

---

## Gold Layer Tables

### 1. dim_products
**Type:** Dimension Table (SCD Type 1)
**Purpose:** Current state of products for analytics
**Source:** Silver layer products

### 2. dim_customers
**Type:** Dimension Table (SCD Type 1)
**Purpose:** Current state of customers for analytics
**Source:** Silver layer customers

### 3. fact_daily_sales
**Type:** Fact Table (Aggregated)
**Purpose:** Daily sales metrics by category
**Grain:** One row per date per category
**Source:** Silver layer orders + order_items + products

### 4. fact_customer_orders
**Type:** Fact Table (Transactional)
**Purpose:** Detailed order facts for customer analysis
**Grain:** One row per order
**Source:** Silver layer orders + order_items

### 5. agg_campaign_performance
**Type:** Aggregate Table
**Purpose:** Marketing campaign metrics and KPIs
**Source:** Silver layer marketing_campaigns

### 6. agg_inventory_status
**Type:** Aggregate Table
**Purpose:** Current inventory status and movement summary
**Source:** Silver layer products + inventory_transactions

---

## Data Quality Rules

### Bronze Layer
- Schema validation
- Record count verification
- Primary key null checks
- Duplicate detection

### Silver Layer
- Data type validation
- Range checks (dates, amounts)
- Referential integrity
- Business rule validation
- Standardization (email lowercase, category title case)

### Gold Layer
- Aggregation accuracy
- KPI threshold alerts
- Trend anomaly detection
- Completeness checks

---

## Change Log

| Date | Version | Changes |
|------|---------|---------|
| 2024-11-24 | 1.0 | Initial data dictionary |

---

**Document Owner:** Data Engineering Team  
**Last Updated:** 2024-11-24
