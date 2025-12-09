# E-Commerce Data Engineering Pipeline

![Architecture](https://img.shields.io/badge/Architecture-Bronze--Silver--Gold-blue)
![Python](https://img.shields.io/badge/Python-3.9+-green)
![Airflow](https://img.shields.io/badge/Airflow-2.7.3-red)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue)

A complete end-to-end data engineering project implementing a modern **Bronze-Silver-Gold** architecture for an e-commerce analytics platform.

## 📋 Table of Contents

- [Problem Statement](#problem-statement)
- [Architecture](#architecture)
- [Technologies](#technologies)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Running the Pipeline](#running-the-pipeline)
- [Data Dictionary](#data-dictionary)
- [Analytics & KPIs](#analytics--kpis)
- [Troubleshooting](#troubleshooting)

## 🎯 Problem Statement

An e-commerce platform needs to analyze:
- Customer behavior and segmentation
- Product performance and inventory management
- Order patterns and fulfillment efficiency
- Marketing campaign effectiveness
- Sales trends and revenue forecasting

This project builds a scalable data pipeline to transform raw transactional data into actionable business insights.

## 🏗️ Architecture

### System Architecture

```
┌─────────────────┐
│  PostgreSQL     │
│  (Source DB)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────────┐
│  Bronze Layer   │─────▶│  Silver Layer    │
│  (Raw Data)     │      │  (Cleaned Data)  │
│  MinIO/Parquet  │      │  MinIO/Parquet   │
└─────────────────┘      └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │  Gold Layer      │
                         │  (Aggregated)    │
                         │  PostgreSQL      │
                         └──────────────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │  BI Tools /      │
                         │  Analytics       │
                         └──────────────────┘
```

### Data Flow

1. **Bronze Layer** (Raw): Extract data from PostgreSQL source → Store as Parquet in MinIO
2. **Silver Layer** (Cleaned): Read from Bronze → Apply transformations → Store in MinIO
3. **Gold Layer** (Business-Ready): Read from Silver → Create aggregations → Load to PostgreSQL

### Orchestration

Apache Airflow manages the entire pipeline with 3 DAGs:
- `daily_bronze_ingestion` - Runs at 1:00 AM
- `daily_silver_transformation` - Runs at 3:00 AM
- `daily_gold_aggregation` - Runs at 5:00 AM

## 🛠️ Technologies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 2.7.3 | Workflow management |
| **Data Lake** | MinIO | S3-compatible object storage |
| **Database** | PostgreSQL 13+ | Source and Gold layer storage |
| **Processing** | Python, Pandas | Data transformation |
| **Data Generation** | Faker | Synthetic data creation |
| **Containerization** | Docker | Service deployment |

## 📁 Project Structure

```
ecommerce-data-pipeline/
├── dags/                          # Airflow DAGs
│   ├── bronze_ingestion_dag.py
│   ├── silver_transformation_dag.py
│   └── gold_aggregation_dag.py
├── src/                           # Source code
│   ├── bronze/
│   │   └── ingest_to_bronze.py
│   ├── silver/
│   │   └── transform_to_silver.py
│   ├── gold/
│   │   └── aggregate_to_gold.py
│   └── utils/
│       ├── db_connector.py
│       ├── minio_client.py
│       └── data_quality.py
├── sql/                           # SQL scripts
│   ├── ddl/
│   │   ├── create_source_tables.sql
│   │   └── create_gold_tables.sql
│   └── analytics/
│       └── kpi_queries.sql
├── scripts/                       # Utility scripts
│   ├── generate_data.py
│   └── setup_minio_buckets.py
├── config/                        # Configuration files
│   └── database.ini
├── docker-compose.yml             # Docker services
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## 🚀 Setup Instructions

### Prerequisites

- Docker & Docker Compose installed
- PostgreSQL 13+ installed locally
- Python 3.9+
- 8GB RAM minimum
- 10GB free disk space

### Step 1: Configure PostgreSQL

Ensure your local PostgreSQL accepts connections from Docker containers.

**On Windows**, edit `C:\Program Files\PostgreSQL\13\data\pg_hba.conf`:
```
# Add this line
host    all             all             172.16.0.0/12           md5
```

Restart PostgreSQL service.

### Step 2: Create Databases

```sql
-- Connect to PostgreSQL as postgres user
psql -U postgres

-- Create databases
CREATE DATABASE ecommerce_source;
CREATE DATABASE ecommerce_gold;

-- Exit
\q
```

### Step 3: Create Database Schemas

```bash
# Create source tables
psql -U postgres -d ecommerce_source -f sql/ddl/create_source_tables.sql

# Create gold tables
psql -U postgres -d ecommerce_gold -f sql/ddl/create_gold_tables.sql
```

### Step 4: Install Python Dependencies

```bash
pip install -r requirements.txt
```

### Step 5: Start Docker Services

```bash
# Start Airflow and MinIO
docker-compose up -d

# Wait for services to be healthy (2-3 minutes)
docker-compose ps
```

**Access Points:**
- Airflow UI: http://localhost:8080 (admin/admin)
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

### Step 6: Setup MinIO Buckets

```bash
python scripts/setup_minio_buckets.py
```

### Step 7: Generate Synthetic Data

```bash
# This will generate:
# - 100,000 customers
# - 50,000 products
# - 500,000 orders
# - 1,500,000 order items
# - 200,000 inventory transactions
# - 10,000 marketing campaigns

python scripts/generate_data.py
```

**Expected runtime:** 10-15 minutes

## ▶️ Running the Pipeline

### Option 1: Manual Execution (Recommended for Testing)

```bash
# Run Bronze ingestion
python src/bronze/ingest_to_bronze.py

# Run Silver transformation
python src/silver/transform_to_silver.py

# Run Gold aggregation
python src/gold/aggregate_to_gold.py
```

### Option 2: Airflow DAGs

1. Open Airflow UI: http://localhost:8080
2. Enable the DAGs:
   - `daily_bronze_ingestion`
   - `daily_silver_transformation`
   - `daily_gold_aggregation`
3. Trigger manually or wait for scheduled runs

### Verify Results

```sql
-- Connect to gold database
psql -U postgres -d ecommerce_gold

-- Check row counts
SELECT 'dim_products' as table_name, COUNT(*) as row_count FROM dim_products
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM dim_customers
UNION ALL
SELECT 'fact_daily_sales', COUNT(*) FROM fact_daily_sales
UNION ALL
SELECT 'fact_customer_orders', COUNT(*) FROM fact_customer_orders
UNION ALL
SELECT 'agg_campaign_performance', COUNT(*) FROM agg_campaign_performance
UNION ALL
SELECT 'agg_inventory_status', COUNT(*) FROM agg_inventory_status;
```

## 📊 Data Dictionary

### Source Tables (6 tables)

#### 1. customers (100k rows)
| Column | Type | Description |
|--------|------|-------------|
| customer_id | SERIAL | Primary key |
| email | VARCHAR(255) | Customer email (unique) |
| first_name | VARCHAR(100) | First name |
| last_name | VARCHAR(100) | Last name |
| customer_segment | VARCHAR(50) | Premium/Regular/New |
| loyalty_tier | VARCHAR(20) | Gold/Silver/Bronze/None |
| registration_date | TIMESTAMP | Account creation date |

#### 2. products (50k rows)
| Column | Type | Description |
|--------|------|-------------|
| product_id | SERIAL | Primary key |
| product_name | VARCHAR(255) | Product name |
| sku | VARCHAR(100) | Stock keeping unit (unique) |
| category | VARCHAR(100) | Product category |
| price | DECIMAL(10,2) | Current price |
| stock_quantity | INTEGER | Available stock |

#### 3. orders (500k rows)
| Column | Type | Description |
|--------|------|-------------|
| order_id | SERIAL | Primary key |
| customer_id | INTEGER | Foreign key to customers |
| order_date | TIMESTAMP | Order placement date |
| order_status | VARCHAR(50) | Pending/Processing/Shipped/Delivered/Cancelled |
| total_amount | DECIMAL(12,2) | Total order value |

#### 4. order_items (1.5M rows)
| Column | Type | Description |
|--------|------|-------------|
| order_item_id | SERIAL | Primary key |
| order_id | INTEGER | Foreign key to orders |
| product_id | INTEGER | Foreign key to products |
| quantity | INTEGER | Quantity ordered |
| unit_price | DECIMAL(10,2) | Price per unit |

#### 5. inventory_transactions (200k rows)
| Column | Type | Description |
|--------|------|-------------|
| transaction_id | SERIAL | Primary key |
| product_id | INTEGER | Foreign key to products |
| transaction_type | VARCHAR(20) | IN/OUT/ADJUSTMENT |
| quantity | INTEGER | Quantity moved |
| transaction_date | TIMESTAMP | Transaction timestamp |

#### 6. marketing_campaigns (10k rows)
| Column | Type | Description |
|--------|------|-------------|
| campaign_id | SERIAL | Primary key |
| campaign_name | VARCHAR(255) | Campaign name |
| channel | VARCHAR(50) | EMAIL/SMS/SOCIAL/DISPLAY/SEARCH |
| budget | DECIMAL(12,2) | Campaign budget |
| roi | DECIMAL(8,2) | Return on investment % |

### Gold Tables (6 tables)

- `dim_products` - Product dimension
- `dim_customers` - Customer dimension
- `fact_daily_sales` - Daily sales aggregation
- `fact_customer_orders` - Order facts
- `agg_campaign_performance` - Campaign metrics
- `agg_inventory_status` - Inventory status

## 📈 Analytics & KPIs

### Key Performance Indicators

1. **Sales Metrics**
   - Daily/Monthly revenue
   - Average order value
   - Conversion rate

2. **Customer Metrics**
   - Customer lifetime value
   - Retention rate
   - Segment performance

3. **Product Metrics**
   - Top selling products
   - Inventory turnover
   - Stock-out frequency

4. **Marketing Metrics**
   - Campaign ROI
   - Channel effectiveness
   - Cost per acquisition

### Sample Queries

See `sql/analytics/kpi_queries.sql` for 20+ pre-built analytics queries.

## 🔧 Troubleshooting

### Issue: Docker containers can't connect to PostgreSQL

**Solution:**
1. Check `pg_hba.conf` has Docker network range
2. Verify PostgreSQL is listening on all interfaces (`listen_addresses = '*'` in `postgresql.conf`)
3. Restart PostgreSQL service

### Issue: Airflow DAGs not appearing

**Solution:**
1. Check DAG files have no syntax errors
2. Verify `/opt/airflow/dags` volume mount in docker-compose.yml
3. Restart Airflow: `docker-compose restart airflow-webserver airflow-scheduler`

### Issue: MinIO connection failed

**Solution:**
1. Verify MinIO is running: `docker-compose ps`
2. Check MinIO console: http://localhost:9001
3. Verify credentials in `config/database.ini`

### Issue: Out of memory during data generation

**Solution:**
For questions or support, please open an issue on GitHub.

---

**Built with ❤️ for Data Engineering**
