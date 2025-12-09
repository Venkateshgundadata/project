"""
Silver Layer Transformation
Reads from Bronze layer, applies transformations, and writes to Silver layer
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
from datetime import datetime
from src.utils.minio_client import MinIOClient
from src.utils.data_quality import DataQualityChecker

BRONZE_BUCKET = 'bronze'
SILVER_BUCKET = 'silver'
TABLES = ['customers', 'products', 'orders', 'order_items', 'inventory_transactions', 'marketing_campaigns']

def transform_customers(df):
    """Transform customers data"""
    print("  Applying transformations...")
    
    # Remove duplicates based on email
    df = df.drop_duplicates(subset=['email'], keep='last')
    
    # Standardize email to lowercase
    df['email'] = df['email'].str.lower().str.strip()
    
    # Handle null values
    df['phone'] = df['phone'].fillna('Unknown')
    df['marketing_consent'] = df['marketing_consent'].fillna(False)
    
    # Create full_name column
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    
    # Calculate customer age
    if 'date_of_birth' in df.columns:
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'])
        df['age'] = ((datetime.now() - df['date_of_birth']).dt.days / 365.25).astype(int)
    
    # Convert dates
    df['registration_date'] = pd.to_datetime(df['registration_date'])
    df['last_login_date'] = pd.to_datetime(df['last_login_date'])
    
    return df

def transform_products(df):
    """Transform products data"""
    print("  Applying transformations...")
    
    # Remove duplicates based on SKU
    df = df.drop_duplicates(subset=['sku'], keep='last')
    
    # Standardize category and brand names
    df['category'] = df['category'].str.title().str.strip()
    df['subcategory'] = df['subcategory'].str.title().str.strip()
    df['brand'] = df['brand'].str.upper().str.strip()
    
    # Calculate profit margin
    df['profit_margin'] = ((df['price'] - df['cost']) / df['price'] * 100).round(2)
    
    # Stock status
    df['stock_status'] = df.apply(lambda x: 
        'Out of Stock' if x['stock_quantity'] == 0 
        else 'Low Stock' if x['stock_quantity'] <= x['reorder_level']
        else 'In Stock', axis=1)
    
    # Convert dates
    df['created_date'] = pd.to_datetime(df['created_date'])
    df['last_updated'] = pd.to_datetime(df['last_updated'])
    
    return df

def transform_orders(df):
    """Transform orders data"""
    print("  Applying transformations...")
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['order_id'], keep='last')
    
    # Convert dates
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['fulfillment_date'] = pd.to_datetime(df['fulfillment_date'])
    
    # Calculate days to fulfill
    df['days_to_fulfill'] = (df['fulfillment_date'] - df['order_date']).dt.days
    
    # Calculate net amount
    df['net_amount'] = df['total_amount'] - df['discount_amount']
    
    # Standardize status values
    df['order_status'] = df['order_status'].str.title()
    df['payment_status'] = df['payment_status'].str.title()
    
    return df

def transform_order_items(df):
    """Transform order items data"""
    print("  Applying transformations...")
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['order_item_id'], keep='last')
    
    # Calculate discount amount
    df['discount_amount'] = (df['unit_price'] * df['quantity'] * df['discount_percentage'] / 100).round(2)
    
    # Calculate final price
    df['final_price'] = df['line_total'] + df['tax_amount']
    
    return df

def transform_inventory_transactions(df):
    """Transform inventory transactions data"""
    print("  Applying transformations...")
    
    # Convert dates
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    
    # Standardize transaction type
    df['transaction_type'] = df['transaction_type'].str.upper()
    
    # Add absolute quantity
    df['abs_quantity'] = df['quantity'].abs()
    
    return df

def transform_marketing_campaigns(df):
    """Transform marketing campaigns data"""
    print("  Applying transformations...")
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['campaign_id'], keep='last')
    
    # Convert dates
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    
    # Calculate campaign duration
    df['duration_days'] = (df['end_date'] - df['start_date']).dt.days
    
    # Calculate metrics
    df['ctr'] = ((df['clicks'] / df['impressions']) * 100).round(2).fillna(0)
    df['conversion_rate'] = ((df['conversions'] / df['clicks']) * 100).round(2).fillna(0)
    df['cost_per_click'] = (df['budget'] / df['clicks']).round(2).fillna(0)
    df['cost_per_conversion'] = (df['budget'] / df['conversions']).round(2).fillna(0)
    
    # Standardize channel
    df['channel'] = df['channel'].str.upper()
    
    return df

TRANSFORM_FUNCTIONS = {
    'customers': transform_customers,
    'products': transform_products,
    'orders': transform_orders,
    'order_items': transform_order_items,
    'inventory_transactions': transform_inventory_transactions,
    'marketing_campaigns': transform_marketing_campaigns
}

def transform_table_to_silver(table_name, minio_client, run_date=None):
    """Transform a single table from Bronze to Silver"""
    if run_date is None:
        run_date = datetime.now()
    
    print(f"\n[Silver] Transforming table: {table_name}")
    
    # Read from Bronze
    bronze_path = minio_client.get_partition_path(table_name, run_date)
    print(f"  Reading from Bronze: s3://{BRONZE_BUCKET}/{bronze_path}")
    df = minio_client.read_dataframe(BRONZE_BUCKET, bronze_path)
    
    # Apply transformations
    if table_name in TRANSFORM_FUNCTIONS:
        df = TRANSFORM_FUNCTIONS[table_name](df)
    
    # Data quality checks
    print(f"  Running data quality checks...")
    dq = DataQualityChecker()
    dq.check_row_count(df, expected_min=1, table_name=table_name)
    
    if not dq.print_summary():
        raise Exception(f"Data quality checks failed for {table_name}")
    
    # Add silver metadata
    df['_transformed_at'] = run_date
    df['_silver_layer_date'] = run_date.date()
    
    # Write to Silver
    silver_path = minio_client.get_partition_path(table_name, run_date)
    print(f"  Writing to Silver: s3://{SILVER_BUCKET}/{silver_path}")
    minio_client.upload_dataframe(df, SILVER_BUCKET, silver_path)
    
    print(f"  ✓ Successfully transformed {table_name} to silver layer")
    return len(df)

def main(run_date=None):
    """Main transformation function"""
    print("=" * 80)
    print("SILVER LAYER TRANSFORMATION")
    print("=" * 80)
    
    # Initialize client
    minio_client = MinIOClient()
    
    # Create silver bucket if not exists
    minio_client.create_bucket(SILVER_BUCKET)
    
    # Transform all tables
    total_rows = 0
    for table in TABLES:
        try:
            rows = transform_table_to_silver(table, minio_client, run_date)
            total_rows += rows
        except Exception as e:
            print(f"  ❌ Error transforming {table}: {str(e)}")
            raise
    
    print("\n" + "=" * 80)
    print(f"SILVER TRANSFORMATION COMPLETE - Total rows: {total_rows:,}")
    print("=" * 80)

if __name__ == "__main__":
    main()
