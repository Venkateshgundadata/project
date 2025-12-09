# E-Commerce Data Pipeline - Architecture Documentation

## System Overview

The E-Commerce Data Pipeline is a modern data engineering solution implementing the **Medallion Architecture** (Bronze-Silver-Gold) pattern to transform raw transactional data into actionable business insights.

## Architecture Principles

### 1. Separation of Concerns
- **Bronze Layer**: Raw data ingestion and storage
- **Silver Layer**: Data cleaning and transformation
- **Gold Layer**: Business aggregations and analytics

### 2. Scalability
- Object storage (MinIO) for horizontal scalability
- Partitioned data by date for efficient querying
- Modular ETL scripts for independent scaling

### 3. Data Quality
- Quality checks at each layer
- Automated validation and monitoring
- Clear data lineage tracking

### 4. Maintainability
- Clear folder structure
- Reusable utility modules
- Comprehensive documentation

## Technology Stack

### Infrastructure Layer
- **Docker**: Containerization of Airflow and MinIO
- **PostgreSQL**: Source and gold layer database
- **MinIO**: S3-compatible object storage for data lake

### Orchestration Layer
- **Apache Airflow**: Workflow orchestration
- **DAGs**: Bronze, Silver, Gold pipelines
- **Sensors**: Cross-DAG dependencies

### Processing Layer
- **Python 3.9+**: Core processing language
- **Pandas**: Data manipulation
- **SQLAlchemy**: Database ORM
- **Boto3**: S3/MinIO client

### Data Layer
- **Parquet**: Columnar storage format
- **PostgreSQL**: Relational database
- **Partitioning**: Date-based partitions

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     SOURCE SYSTEMS                          │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Customers   │  │   Products   │  │    Orders    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Order Items  │  │  Inventory   │  │  Campaigns   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
│                                                             │
│              PostgreSQL (ecommerce_source)                  │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          │ Extract (Daily @ 1 AM)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER                            │
│                                                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │  MinIO Bucket: bronze/                             │    │
│  │  Format: Parquet                                   │    │
│  │  Partitioning: table/year=YYYY/month=MM/day=DD/    │    │
│  │  Metadata: _ingested_at, _source_system            │    │
│  └────────────────────────────────────────────────────┘    │
│                                                             │
│  Data Quality: Schema validation, row counts, PK checks     │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          │ Transform (Daily @ 3 AM)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                     SILVER LAYER                            │
│                                                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │  MinIO Bucket: silver/                             │    │
│  │  Format: Parquet                                   │    │
│  │  Partitioning: table/year=YYYY/month=MM/day=DD/    │    │
│  │  Metadata: _transformed_at                         │    │
│  └────────────────────────────────────────────────────┘    │
│                                                             │
│  Transformations:                                           │
│  - Deduplication                                            │
│  - Data type conversions                                    │
│  - Standardization (email, category, etc.)                  │
│  - Derived columns (full_name, profit_margin, etc.)         │
│  - Data quality checks                                      │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          │ Aggregate (Daily @ 5 AM)
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      GOLD LAYER                             │
│                                                             │
│  ┌────────────────────────────────────────────────────┐    │
│  │  PostgreSQL (ecommerce_gold)                       │    │
│  │                                                    │    │
│  │  Dimensions:                                       │    │
│  │  - dim_products                                    │    │
│  │  - dim_customers                                   │    │
│  │                                                    │    │
│  │  Facts:                                            │    │
│  │  - fact_daily_sales                                │    │
│  │  - fact_customer_orders                            │    │
│  │                                                    │    │
│  │  Aggregates:                                       │    │
│  │  - agg_campaign_performance                        │    │
│  │  - agg_inventory_status                            │    │
│  └────────────────────────────────────────────────────┘    │
│                                                             │
│  Business Logic: KPIs, metrics, aggregations                │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          │ Consume
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   ANALYTICS & BI                            │
│                                                             │
│  - SQL Queries                                              │
│  - BI Tools (Tableau, Power BI, etc.)                       │
│  - Reports and Dashboards                                   │
│  - Machine Learning Models                                  │
└─────────────────────────────────────────────────────────────┘
```

## Airflow DAG Architecture

```
┌──────────────────────────────────────────────────────────┐
│  daily_bronze_ingestion (Schedule: 0 1 * * *)           │
│                                                          │
│  ┌────────────────────────────────────────────────┐     │
│  │  Task: ingest_to_bronze                        │     │
│  │  - Extract from PostgreSQL source              │     │
│  │  - Add metadata columns                        │     │
│  │  - Data quality checks                         │     │
│  │  - Write to MinIO bronze/                      │     │
│  └────────────────────────────────────────────────┘     │
└──────────────────────────┬───────────────────────────────┘
                           │
                           │ ExternalTaskSensor
                           ▼
┌──────────────────────────────────────────────────────────┐
│  daily_silver_transformation (Schedule: 0 3 * * *)      │
│                                                          │
│  ┌────────────────────────────────────────────────┐     │
│  │  Task: wait_for_bronze_ingestion               │     │
│  └────────────────────────────────────────────────┘     │
│                           │                              │
│                           ▼                              │
│  ┌────────────────────────────────────────────────┐     │
│  │  Task: transform_to_silver                     │     │
│  │  - Read from MinIO bronze/                     │     │
│  │  - Apply transformations                       │     │
│  │  - Data quality checks                         │     │
│  │  - Write to MinIO silver/                      │     │
│  └────────────────────────────────────────────────┘     │
└──────────────────────────┬───────────────────────────────┘
                           │
                           │ ExternalTaskSensor
                           ▼
┌──────────────────────────────────────────────────────────┐
│  daily_gold_aggregation (Schedule: 0 5 * * *)           │
│                                                          │
│  ┌────────────────────────────────────────────────┐     │
│  │  Task: wait_for_silver_transformation          │     │
│  └────────────────────────────────────────────────┘     │
│                           │                              │
│                           ▼                              │
│  ┌────────────────────────────────────────────────┐     │
│  │  Task: aggregate_to_gold                       │     │
│  │  - Read from MinIO silver/                     │     │
│  │  - Create dimensions                           │     │
│  │  - Create facts                                │     │
│  │  - Create aggregates                           │     │
│  │  - Write to PostgreSQL gold                    │     │
│  └────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────┘
```

## Network Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      HOST MACHINE                           │
│                                                             │
│  ┌──────────────────────────────────────────────────┐      │
│  │  PostgreSQL (localhost:5432)                     │      │
│  │  - ecommerce_source                              │      │
│  │  - ecommerce_gold                                │      │
│  └──────────────────────────────────────────────────┘      │
│                                                             │
│  ┌──────────────────────────────────────────────────┐      │
│  │  Docker Network: ecommerce-network               │      │
│  │                                                  │      │
│  │  ┌────────────────────────────────────────┐     │      │
│  │  │  Airflow Webserver (8080)              │     │      │
│  │  └────────────────────────────────────────┘     │      │
│  │  ┌────────────────────────────────────────┐     │      │
│  │  │  Airflow Scheduler                     │     │      │
│  │  └────────────────────────────────────────┘     │      │
│  │  ┌────────────────────────────────────────┐     │      │
│  │  │  MinIO (9000, 9001)                    │     │      │
│  │  └────────────────────────────────────────┘     │      │
│  │  ┌────────────────────────────────────────┐     │      │
│  │  │  PostgreSQL Airflow Metadata           │     │      │
│  │  └────────────────────────────────────────┘     │      │
│  │                                                  │      │
│  │  Connection to host: host.docker.internal        │      │
│  └──────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

## Security Considerations

### 1. Credentials Management
- Database credentials in `config/database.ini`
- Environment variables for sensitive data
- Docker secrets for production deployment

### 2. Network Security
- PostgreSQL configured to accept Docker network connections
- MinIO with access key authentication
- Airflow with basic authentication

### 3. Data Security
- No PII in logs
- Encrypted connections (production)
- Access control on MinIO buckets

## Performance Optimization

### 1. Data Partitioning
- Date-based partitions in MinIO
- Efficient query pruning
- Parallel processing capability

### 2. File Format
- Parquet for columnar storage
- Compression enabled
- Predicate pushdown support

### 3. Database Optimization
- Indexes on frequently queried columns
- Materialized views for complex aggregations
- Connection pooling

## Monitoring and Observability

### 1. Airflow Monitoring
- DAG run history
- Task duration metrics
- Failure alerts

### 2. Data Quality Monitoring
- Row count validation
- Schema drift detection
- Business rule violations

### 3. System Monitoring
- MinIO storage usage
- PostgreSQL performance
- Docker container health

## Disaster Recovery

### 1. Backup Strategy
- PostgreSQL automated backups
- MinIO versioning enabled
- Configuration files in version control

### 2. Recovery Procedures
- Point-in-time recovery for PostgreSQL
- MinIO object restoration
- DAG replay capability

## Future Enhancements

1. **Incremental Processing**: Implement CDC for efficient updates
2. **Data Catalog**: Add metadata management (Apache Atlas)
3. **Data Lineage**: Track data flow end-to-end
4. **Real-time Processing**: Add streaming layer (Kafka)
5. **ML Integration**: Feature store for ML models
6. **Advanced Analytics**: dbt for transformation logic

---

**Document Version:** 1.0  
**Last Updated:** 2024-11-24  
**Maintained By:** Data Engineering Team
