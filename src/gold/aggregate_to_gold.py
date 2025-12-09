"""
Gold Layer Aggregation
Reads from Silver layer, creates business aggregations, and loads to PostgreSQL gold database
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

import pandas as pd
from datetime import datetime
from src.utils.db_connector import DatabaseConnector
from src.utils.minio_client import MinIOClient

SILVER_BUCKET = 'silver'

def create_dim_products(minio_client, db_connector, run_date=None):
    """Create product dimension table"""
    if run_date is None:
        run_date = datetime.now()
    
    print("\n[Gold] Creating dim_products...")
    
    # Read from Silver
    silver_path = minio_client.get_partition_path('products', run_date)
    df = minio_client.read_dataframe(SILVER_BUCKET, silver_path)
    
    # Select and rename columns
    dim_products = df[[
        'product_id', 'product_name', 'sku', 'category', 'subcategory',
        'brand', 'price', 'cost', 'is_active'
    ]].copy()
    
    dim_products.rename(columns={
        'price': 'current_price',
        'cost': 'current_cost'
    }, inplace=True)
    
    dim_products['effective_date'] = run_date
    
    # Load to PostgreSQL
    engine = db_connector.get_gold_engine()
    dim_products.to_sql('dim_products', engine, if_exists='replace', index=False, method='multi')
    
    print(f"  ✓ Loaded {len(dim_products):,} products to dim_products")
    return len(dim_products)

def create_dim_customers(minio_client, db_connector, run_date=None):
    """Create customer dimension table"""
    if run_date is None:
        run_date = datetime.now()
    
    print("\n[Gold] Creating dim_customers...")
    
    # Read from Silver
    silver_path = minio_client.get_partition_path('customers', run_date)
    df = minio_client.read_dataframe(SILVER_BUCKET, silver_path)
    
    # Select and rename columns
    dim_customers = df[[
        'customer_id', 'email', 'full_name', 'customer_segment',
        'loyalty_tier', 'country', 'city', 'registration_date', 'account_status'
    ]].copy()
    
    dim_customers['effective_date'] = run_date
    
    # Load to PostgreSQL
    engine = db_connector.get_gold_engine()
    dim_customers.to_sql('dim_customers', engine, if_exists='replace', index=False, method='multi')
    
    print(f"  ✓ Loaded {len(dim_customers):,} customers to dim_customers")
    return len(dim_customers)

def create_fact_daily_sales(minio_client, db_connector, run_date=None):
    """Create daily sales fact table"""
    if run_date is None:
        run_date = datetime.now()
    
    print("\n[Gold] Creating fact_daily_sales...")
    
    # Read from Silver
    orders_path = minio_client.get_partition_path('orders', run_date)
    order_items_path = minio_client.get_partition_path('order_items', run_date)
    products_path = minio_client.get_partition_path('products', run_date)
    
    df_orders = minio_client.read_dataframe(SILVER_BUCKET, orders_path)
    df_order_items = minio_client.read_dataframe(SILVER_BUCKET, order_items_path)
    df_products = minio_client.read_dataframe(SILVER_BUCKET, products_path)
    
    # Join orders with order items and products
    # Use only columns that exist in all datasets
    df = df_order_items.merge(df_orders[['order_id', 'customer_id', 'order_date']], 
                               on='order_id', how='left')
    df = df.merge(df_products[['product_id', 'category']], on='product_id', how='left')
    
    # Extract date
    df['sales_date'] = pd.to_datetime(df['order_date']).dt.date
    
    # Aggregate by date and category
    fact_daily_sales = df.groupby(['sales_date', 'category']).agg({
        'order_id': 'nunique',
        'quantity': 'sum',
        'line_total': 'sum',
        'customer_id': 'nunique'
    }).reset_index()
    
    fact_daily_sales.columns = [
        'sales_date', 'category', 'total_orders', 'total_items_sold',
        'total_revenue', 'unique_customers'
    ]
    
    # Add calculated columns
    fact_daily_sales['total_discount'] = 0.0
    fact_daily_sales['total_tax'] = 0.0
    
    # Calculate average order value
    fact_daily_sales['avg_order_value'] = (
        fact_daily_sales['total_revenue'] / fact_daily_sales['total_orders']
    ).round(2)
    
    # Load to PostgreSQL
    engine = db_connector.get_gold_engine()
    fact_daily_sales.to_sql('fact_daily_sales', engine, if_exists='replace', index=False, method='multi')
    
    print(f"  ✓ Loaded {len(fact_daily_sales):,} records to fact_daily_sales")
    return len(fact_daily_sales)

def create_fact_customer_orders(minio_client, db_connector, run_date=None):
    """Create customer orders fact table"""
    if run_date is None:
        run_date = datetime.now()
    
    print("\n[Gold] Creating fact_customer_orders...")
    
    # Read from Silver
    orders_path = minio_client.get_partition_path('orders', run_date)
    order_items_path = minio_client.get_partition_path('order_items', run_date)
    customers_path = minio_client.get_partition_path('customers', run_date)
    
    df_orders = minio_client.read_dataframe(SILVER_BUCKET, orders_path)
    df_order_items = minio_client.read_dataframe(SILVER_BUCKET, order_items_path)
    df_customers = minio_client.read_dataframe(SILVER_BUCKET, customers_path)
    
    # Count items per order
    items_per_order = df_order_items.groupby('order_id').size().reset_index(name='total_items')
    
    # Join with orders
    fact_orders = df_orders.merge(items_per_order, on='order_id', how='left')
    
    # Join with customers to get customer_key (using customer_id as key for now)
    fact_orders = fact_orders.merge(
        df_customers[['customer_id']].reset_index().rename(columns={'index': 'customer_key'}),
        on='customer_id',
        how='left'
    )
    
    # Select columns
    fact_orders = fact_orders[[
        'order_id', 'customer_key', 'order_date', 'order_status', 'payment_method',
        'total_amount', 'discount_amount', 'tax_amount', 'shipping_cost',
        'total_items', 'payment_status', 'fulfillment_date', 'days_to_fulfill'
    ]]
    
    # Load to PostgreSQL
    engine = db_connector.get_gold_engine()
    fact_orders.to_sql('fact_customer_orders', engine, if_exists='replace', index=False, method='multi')
    
    print(f"  ✓ Loaded {len(fact_orders):,} orders to fact_customer_orders")
    return len(fact_orders)

def create_agg_campaign_performance(minio_client, db_connector, run_date=None):
    """Create campaign performance aggregation"""
    if run_date is None:
        run_date = datetime.now()
    
    print("\n[Gold] Creating agg_campaign_performance...")
    
    # Read from Silver
    campaigns_path = minio_client.get_partition_path('marketing_campaigns', run_date)
    df = minio_client.read_dataframe(SILVER_BUCKET, campaigns_path)
    
    # Select and rename columns
    agg_campaigns = df[[
        'campaign_id', 'campaign_name', 'channel', 'start_date', 'end_date',
        'budget', 'target_segment', 'impressions', 'clicks', 'conversions',
        'revenue_generated', 'roi', 'ctr', 'conversion_rate',
        'cost_per_click', 'cost_per_conversion'
    ]].copy()
    
    agg_campaigns.rename(columns={
        'impressions': 'total_impressions',
        'clicks': 'total_clicks',
        'conversions': 'total_conversions',
        'revenue_generated': 'total_revenue',
        'roi': 'roi_percentage'
    }, inplace=True)
    
    # Load to PostgreSQL
    engine = db_connector.get_gold_engine()
    agg_campaigns.to_sql('agg_campaign_performance', engine, if_exists='replace', index=False, method='multi')
    
    print(f"  ✓ Loaded {len(agg_campaigns):,} campaigns to agg_campaign_performance")
    return len(agg_campaigns)

def create_agg_inventory_status(minio_client, db_connector, run_date=None):
    """Create inventory status aggregation"""
    if run_date is None:
        run_date = datetime.now()
    
    print("\n[Gold] Creating agg_inventory_status...")
    
    # Read from Silver
    products_path = minio_client.get_partition_path('products', run_date)
    inventory_path = minio_client.get_partition_path('inventory_transactions', run_date)
    
    df_products = minio_client.read_dataframe(SILVER_BUCKET, products_path)
    df_inventory = minio_client.read_dataframe(SILVER_BUCKET, inventory_path)
    
    # Aggregate inventory transactions
    inventory_agg = df_inventory.groupby('product_id').agg({
        'quantity': lambda x: x[df_inventory.loc[x.index, 'transaction_type'] == 'IN'].sum(),
        'transaction_date': 'max'
    }).reset_index()
    
    inventory_agg.columns = ['product_id', 'total_in', 'last_transaction_date']
    
    # Calculate total OUT and ADJUSTMENT
    out_agg = df_inventory[df_inventory['transaction_type'] == 'OUT'].groupby('product_id')['quantity'].sum().abs()
    adj_agg = df_inventory[df_inventory['transaction_type'] == 'ADJUSTMENT'].groupby('product_id')['quantity'].sum()
    
    inventory_agg = inventory_agg.merge(
        out_agg.reset_index().rename(columns={'quantity': 'total_out'}),
        on='product_id', how='left'
    )
    inventory_agg = inventory_agg.merge(
        adj_agg.reset_index().rename(columns={'quantity': 'total_adjustments'}),
        on='product_id', how='left'
    )
    
    inventory_agg.fillna(0, inplace=True)
    
    # Join with products
    agg_inventory = df_products[['product_id', 'product_name', 'category', 'stock_quantity', 'reorder_level', 'stock_status']].merge(
        inventory_agg, on='product_id', how='left'
    )
    
    agg_inventory.rename(columns={'stock_quantity': 'current_stock'}, inplace=True)
    
    # Calculate days since last movement
    agg_inventory['days_since_last_movement'] = (
        (datetime.now() - pd.to_datetime(agg_inventory['last_transaction_date'])).dt.days
    )
    
    # Load to PostgreSQL
    engine = db_connector.get_gold_engine()
    agg_inventory.to_sql('agg_inventory_status', engine, if_exists='replace', index=False, method='multi')
    
    print(f"  ✓ Loaded {len(agg_inventory):,} products to agg_inventory_status")
    return len(agg_inventory)

def main(run_date=None):
    """Main aggregation function"""
    print("=" * 80)
    print("GOLD LAYER AGGREGATION")
    print("=" * 80)
    
    # Initialize clients
    minio_client = MinIOClient()
    db_connector = DatabaseConnector()
    
    # Create all gold tables
    try:
        create_dim_products(minio_client, db_connector, run_date)
        create_dim_customers(minio_client, db_connector, run_date)
        create_fact_daily_sales(minio_client, db_connector, run_date)
        create_fact_customer_orders(minio_client, db_connector, run_date)
        create_agg_campaign_performance(minio_client, db_connector, run_date)
        create_agg_inventory_status(minio_client, db_connector, run_date)
    except Exception as e:
        print(f"\n❌ Error creating gold tables: {str(e)}")
        raise
    
    print("\n" + "=" * 80)
    print("GOLD AGGREGATION COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()
