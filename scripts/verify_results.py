"""
Verify pipeline results
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def verify_gold_tables():
    """Check row counts in gold tables"""
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
            'dim_products',
            'dim_customers',
            'fact_daily_sales',
            'fact_customer_orders',
            'agg_campaign_performance',
            'agg_inventory_status'
        ]
        
        print("=" * 80)
        print("GOLD LAYER VERIFICATION")
        print("=" * 80)
        
        for table in tables:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            print(f"{table:30} {count:>15,} rows")
        
        print("=" * 80)
        
        # Sample query
        print("\nSample Query - Top 5 Categories by Revenue:")
        print("-" * 80)
        cursor.execute("""
            SELECT 
                category,
                SUM(total_revenue) as total_revenue,
                SUM(total_orders) as total_orders
            FROM fact_daily_sales
            GROUP BY category
            ORDER BY total_revenue DESC
            LIMIT 5
        """)
        
        results = cursor.fetchall()
        for row in results:
            print(f"{row[0]:20} ${row[1]:>15,.2f}  ({row[2]:>10,} orders)")
        
        cursor.close()
        conn.close()
        print("\n✓ Verification complete!")
        return True
    except Exception as e:
        print(f"Error verifying tables: {e}")
        return False

if __name__ == "__main__":
    verify_gold_tables()
