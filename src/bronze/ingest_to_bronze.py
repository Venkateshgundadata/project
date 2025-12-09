"""
Bronze Layer Ingestion
Extracts data from PostgreSQL source and loads to MinIO bronze layer
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
from datetime import datetime
from src.utils.db_connector import DatabaseConnector
from src.utils.minio_client import MinIOClient
from src.utils.data_quality import DataQualityChecker

# Tables to ingest
TABLES = ['customers', 'products', 'orders', 'order_items', 'inventory_transactions', 'marketing_campaigns']
BRONZE_BUCKET = 'bronze'

def ingest_table_to_bronze(table_name, db_connector, minio_client, run_date=None):
    """Ingest a single table from PostgreSQL to Bronze layer"""
    if run_date is None:
        run_date = datetime.now()
    
    print(f"\n[Bronze] Ingesting table: {table_name}")
    print(f"  Run date: {run_date.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Extract from PostgreSQL
    engine = db_connector.get_source_engine()
    query = f"SELECT * FROM {table_name}"
    
    print(f"  Extracting data from PostgreSQL...")
    df = pd.read_sql(query, engine)
    print(f"  ✓ Extracted {len(df):,} rows")
    
    # Add metadata columns
    df['_ingested_at'] = run_date
    df['_source_system'] = 'ecommerce_source'
    df['_bronze_layer_date'] = run_date.date()
    
    # Data quality checks
    print(f"  Running data quality checks...")
    dq = DataQualityChecker()
    dq.check_row_count(df, expected_min=1, table_name=table_name)
    
    # Check for nulls in primary key (first column is typically PK)
    pk_column = df.columns[0]
    dq.check_null_values(df, [pk_column], table_name)
    
    if not dq.print_summary():
        raise Exception(f"Data quality checks failed for {table_name}")
    
    # Load to MinIO Bronze
    partition_path = minio_client.get_partition_path(table_name, run_date)
    print(f"  Loading to MinIO: s3://{BRONZE_BUCKET}/{partition_path}")
    minio_client.upload_dataframe(df, BRONZE_BUCKET, partition_path, file_format='parquet')
    
    print(f"  ✓ Successfully ingested {table_name} to bronze layer")
    return len(df)

def main(run_date=None):
    """Main ingestion function"""
    print("=" * 80)
    print("BRONZE LAYER INGESTION")
    print("=" * 80)
    
    # Initialize clients
    db_connector = DatabaseConnector()
    minio_client = MinIOClient()
    
    # Create bronze bucket if not exists
    minio_client.create_bucket(BRONZE_BUCKET)
    
    # Ingest all tables
    total_rows = 0
    for table in TABLES:
        try:
            rows = ingest_table_to_bronze(table, db_connector, minio_client, run_date)
            total_rows += rows
        except Exception as e:
            print(f"  ❌ Error ingesting {table}: {str(e)}")
            raise
    
    print("\n" + "=" * 80)
    print(f"BRONZE INGESTION COMPLETE - Total rows: {total_rows:,}")
    print("=" * 80)

if __name__ == "__main__":
    main()
