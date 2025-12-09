"""
Drop and recreate gold tables
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def drop_gold_tables():
    """Drop all gold tables with CASCADE"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('LOCAL_POSTGRES_HOST', 'localhost'),
            port=os.getenv('LOCAL_POSTGRES_PORT', '5432'),
            user=os.getenv('LOCAL_POSTGRES_USER', 'postgres'),
            password=os.getenv('LOCAL_POSTGRES_PASSWORD', 'postgres'),
            database='ecommerce_gold'
        )
        cursor = conn.cursor()
        
        tables = [
            'fact_daily_sales',
            'fact_customer_orders',
            'dim_products',
            'dim_customers',
            'agg_campaign_performance',
            'agg_inventory_status'
        ]
        
        for table in tables:
            cursor.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
            print(f"✓ Dropped {table}")
        
        conn.commit()
        cursor.close()
        conn.close()
        print("\nAll gold tables dropped successfully!")
        return True
    except Exception as e:
        print(f"Error dropping tables: {e}")
        return False

if __name__ == "__main__":
    drop_gold_tables()
