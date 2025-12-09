"""
E-Commerce Data Generator
Generates synthetic data for all 6 tables using Faker library
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
from sqlalchemy import create_engine
import configparser
import os

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

# Read database configuration
config = configparser.ConfigParser()
config.read('config/database.ini')

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Database connection
# Try env vars first, fallback to config
DB_HOST = os.getenv('LOCAL_POSTGRES_HOST') or config['source_db']['host']
DB_PORT = os.getenv('LOCAL_POSTGRES_PORT') or config['source_db']['port']
DB_NAME = os.getenv('LOCAL_DB_SOURCE') or config['source_db']['database']
DB_USER = os.getenv('LOCAL_POSTGRES_USER') or config['source_db']['user']
DB_PASS = os.getenv('LOCAL_POSTGRES_PASSWORD') or config['source_db']['password']

CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Data generation parameters
NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 500
NUM_ORDERS = 5000
NUM_CAMPAIGNS = 100

print("=" * 80)
print("E-COMMERCE DATA GENERATOR")
print("=" * 80)

def generate_customers(n=NUM_CUSTOMERS):
    """Generate customer data"""
    print(f"\n[1/6] Generating {n:,} customers...")
    
    segments = ['Premium', 'Regular', 'New']
    tiers = ['Gold', 'Silver', 'Bronze', 'None']
    statuses = ['Active', 'Inactive', 'Suspended']
    
    customers = []
    for i in range(n):
        reg_date = fake.date_time_between(start_date='-3y', end_date='now')
        
        customer = {
            'email': fake.unique.email(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'phone': fake.phone_number()[:20],
            'date_of_birth': fake.date_of_birth(minimum_age=18, maximum_age=80),
            'registration_date': reg_date,
            'customer_segment': random.choices(segments, weights=[0.15, 0.70, 0.15])[0],
            'loyalty_tier': random.choices(tiers, weights=[0.05, 0.15, 0.30, 0.50])[0],
            'country': fake.country(),
            'city': fake.city(),
            'postal_code': fake.postcode(),
            'marketing_consent': random.choice([True, False]),
            'last_login_date': fake.date_time_between(start_date=reg_date, end_date='now'),
            'account_status': random.choices(statuses, weights=[0.85, 0.10, 0.05])[0]
        }
        customers.append(customer)
        
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1:,} customers...")
    
    df = pd.DataFrame(customers)
    print(f"  ✓ Completed: {len(df):,} customers generated")
    return df

def generate_products(n=NUM_PRODUCTS):
    """Generate product data"""
    print(f"\n[2/6] Generating {n:,} products...")
    
    categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Accessories'],
        'Clothing': ['Men', 'Women', 'Kids', 'Shoes'],
        'Home & Garden': ['Furniture', 'Kitchen', 'Decor', 'Tools'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Cycling'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics']
    }
    
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE', 'BrandF', 'BrandG', 'BrandH']
    
    products = []
    for i in range(n):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        cost = round(random.uniform(5, 500), 2)
        price = round(cost * random.uniform(1.3, 2.5), 2)
        
        product = {
            'product_name': fake.catch_phrase()[:255],
            'sku': fake.unique.bothify(text='SKU-####-????').upper(),
            'category': category,
            'subcategory': subcategory,
            'brand': random.choice(brands),
            'price': price,
            'cost': cost,
            'stock_quantity': random.randint(0, 1000),
            'reorder_level': random.randint(10, 50),
            'supplier_id': random.randint(1, 100),
            'weight_kg': round(random.uniform(0.1, 50), 2),
            'dimensions_cm': f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(5, 50)}",
            'is_active': random.choices([True, False], weights=[0.95, 0.05])[0]
        }
        products.append(product)
        
        if (i + 1) % 5000 == 0:
            print(f"  Generated {i + 1:,} products...")
    
    df = pd.DataFrame(products)
    print(f"  ✓ Completed: {len(df):,} products generated")
    return df

def generate_orders(n=NUM_ORDERS, customer_ids=None):
    """Generate order data"""
    print(f"\n[3/6] Generating {n:,} orders...")
    
    statuses = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']
    payment_statuses = ['Pending', 'Paid', 'Failed', 'Refunded']
    
    orders = []
    for i in range(n):
        order_date = fake.date_time_between(start_date='-2y', end_date='now')
        status = random.choices(statuses, weights=[0.05, 0.10, 0.15, 0.65, 0.05])[0]
        
        # Calculate fulfillment date based on status
        fulfillment_date = None
        if status in ['Shipped', 'Delivered']:
            fulfillment_date = order_date + timedelta(days=random.randint(1, 7))
        
        subtotal = round(random.uniform(20, 2000), 2)
        discount = round(subtotal * random.uniform(0, 0.2), 2)
        tax = round((subtotal - discount) * 0.08, 2)
        shipping = round(random.uniform(0, 25), 2)
        
        order = {
            'customer_id': random.choice(customer_ids) if customer_ids else random.randint(1, 100000),
            'order_date': order_date,
            'order_status': status,
            'payment_method': random.choice(payment_methods),
            'shipping_address': fake.address().replace('\n', ', ')[:500],
            'billing_address': fake.address().replace('\n', ', ')[:500],
            'total_amount': round(subtotal - discount + tax + shipping, 2),
            'discount_amount': discount,
            'tax_amount': tax,
            'shipping_cost': shipping,
            'currency': 'USD',
            'payment_status': random.choices(payment_statuses, weights=[0.05, 0.90, 0.03, 0.02])[0],
            'fulfillment_date': fulfillment_date
        }
        orders.append(order)
        
        if (i + 1) % 50000 == 0:
            print(f"  Generated {i + 1:,} orders...")
    
    df = pd.DataFrame(orders)
    print(f"  ✓ Completed: {len(df):,} orders generated")
    return df

def generate_order_items(order_ids, product_ids):
    """Generate order items data"""
    print(f"\n[4/6] Generating order items (avg 3 items per order)...")
    
    return_statuses = ['None', 'Requested', 'Approved', 'Completed']
    
    order_items = []
    for i, order_id in enumerate(order_ids):
        num_items = random.choices([1, 2, 3, 4, 5], weights=[0.3, 0.3, 0.2, 0.15, 0.05])[0]
        
        for _ in range(num_items):
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(10, 500), 2)
            discount_pct = random.choices([0, 5, 10, 15, 20], weights=[0.6, 0.2, 0.1, 0.05, 0.05])[0]
            line_total = round(quantity * unit_price * (1 - discount_pct / 100), 2)
            tax = round(line_total * 0.08, 2)
            
            item = {
                'order_id': order_id,
                'product_id': random.choice(product_ids),
                'quantity': quantity,
                'unit_price': unit_price,
                'discount_percentage': discount_pct,
                'line_total': line_total,
                'tax_amount': tax,
                'return_status': random.choices(return_statuses, weights=[0.95, 0.02, 0.02, 0.01])[0]
            }
            order_items.append(item)
        
        if (i + 1) % 50000 == 0:
            print(f"  Generated items for {i + 1:,} orders...")
    
    df = pd.DataFrame(order_items)
    print(f"  ✓ Completed: {len(df):,} order items generated")
    return df

def generate_inventory_transactions(product_ids, order_ids):
    """Generate inventory transaction data"""
    print(f"\n[5/6] Generating inventory transactions...")
    
    n = 200000
    transaction_types = ['IN', 'OUT', 'ADJUSTMENT']
    
    transactions = []
    for i in range(n):
        trans_type = random.choices(transaction_types, weights=[0.4, 0.5, 0.1])[0]
        
        transaction = {
            'product_id': random.choice(product_ids),
            'warehouse_id': random.randint(1, 10),
            'transaction_type': trans_type,
            'quantity': random.randint(1, 100) if trans_type == 'IN' else -random.randint(1, 50),
            'transaction_date': fake.date_time_between(start_date='-2y', end_date='now'),
            'reference_order_id': random.choice(order_ids) if trans_type == 'OUT' and random.random() > 0.5 else None,
            'notes': fake.sentence() if random.random() > 0.7 else None
        }
        transactions.append(transaction)
        
        if (i + 1) % 20000 == 0:
            print(f"  Generated {i + 1:,} transactions...")
    
    df = pd.DataFrame(transactions)
    print(f"  ✓ Completed: {len(df):,} inventory transactions generated")
    return df

def generate_marketing_campaigns(n=NUM_CAMPAIGNS):
    """Generate marketing campaign data"""
    print(f"\n[6/6] Generating {n:,} marketing campaigns...")
    
    channels = ['EMAIL', 'SMS', 'SOCIAL', 'DISPLAY', 'SEARCH']
    segments = ['All', 'Premium', 'Regular', 'New']
    
    campaigns = []
    for i in range(n):
        start_date = fake.date_between(start_date='-2y', end_date='now')
        end_date = start_date + timedelta(days=random.randint(7, 90))
        budget = round(random.uniform(1000, 100000), 2)
        impressions = random.randint(1000, 1000000)
        clicks = int(impressions * random.uniform(0.01, 0.05))
        conversions = int(clicks * random.uniform(0.01, 0.1))
        revenue = round(conversions * random.uniform(50, 500), 2)
        roi = round(((revenue - budget) / budget) * 100, 2) if budget > 0 else 0
        
        campaign = {
            'campaign_name': f"{fake.catch_phrase()} - {start_date.strftime('%Y%m')}",
            'channel': random.choice(channels),
            'start_date': start_date,
            'end_date': end_date,
            'budget': budget,
            'target_segment': random.choice(segments),
            'impressions': impressions,
            'clicks': clicks,
            'conversions': conversions,
            'revenue_generated': revenue,
            'roi': roi
        }
        campaigns.append(campaign)
        
        if (i + 1) % 1000 == 0:
            print(f"  Generated {i + 1:,} campaigns...")
    
    df = pd.DataFrame(campaigns)
    print(f"  ✓ Completed: {len(df):,} campaigns generated")
    return df

def load_to_database(df, table_name, engine):
    """Load DataFrame to PostgreSQL"""
    print(f"\n  Loading {len(df):,} rows to table '{table_name}'...")
    df.to_sql(table_name, engine, if_exists='append', index=False, method='multi', chunksize=1000)
    print(f"  ✓ Successfully loaded to '{table_name}'")

def main():
    """Main execution function"""
    try:
        # Create database engine
        print(f"\nConnecting to database: {DB_NAME}@{DB_HOST}:{DB_PORT}")
        engine = create_engine(CONNECTION_STRING)
        
        # Test connection
        with engine.connect() as conn:
            print("✓ Database connection successful\n")
        
        # Generate data
        print("Starting data generation...")
        print(f"Target rows: Customers={NUM_CUSTOMERS:,}, Products={NUM_PRODUCTS:,}, Orders={NUM_ORDERS:,}")
        
        # 1. Customers
        df_customers = generate_customers()
        load_to_database(df_customers, 'customers', engine)
        customer_ids = list(range(1, len(df_customers) + 1))
        
        # 2. Products
        df_products = generate_products()
        load_to_database(df_products, 'products', engine)
        product_ids = list(range(1, len(df_products) + 1))
        
        # 3. Orders
        df_orders = generate_orders(customer_ids=customer_ids)
        load_to_database(df_orders, 'orders', engine)
        order_ids = list(range(1, len(df_orders) + 1))
        
        # 4. Order Items
        df_order_items = generate_order_items(order_ids, product_ids)
        load_to_database(df_order_items, 'order_items', engine)
        
        # 5. Inventory Transactions
        df_inventory = generate_inventory_transactions(product_ids, order_ids)
        load_to_database(df_inventory, 'inventory_transactions', engine)
        
        # 6. Marketing Campaigns
        df_campaigns = generate_marketing_campaigns()
        load_to_database(df_campaigns, 'marketing_campaigns', engine)
        
        # Summary
        print("\n" + "=" * 80)
        print("DATA GENERATION COMPLETE!")
        print("=" * 80)
        print(f"Customers:              {len(df_customers):>10,}")
        print(f"Products:               {len(df_products):>10,}")
        print(f"Orders:                 {len(df_orders):>10,}")
        print(f"Order Items:            {len(df_order_items):>10,}")
        print(f"Inventory Transactions: {len(df_inventory):>10,}")
        print(f"Marketing Campaigns:    {len(df_campaigns):>10,}")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()
