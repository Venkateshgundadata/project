"""
Test PostgreSQL connection from Airflow container
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    # Test connection to source database
    print("Testing connection to ecommerce_source...")
    conn = psycopg2.connect(
        host=os.getenv('LOCAL_POSTGRES_HOST', 'host.docker.internal'),
        port=os.getenv('LOCAL_POSTGRES_PORT', '5432'),
        user=os.getenv('LOCAL_POSTGRES_USER', 'postgres'),
        password=os.getenv('LOCAL_POSTGRES_PASSWORD', 'postgres'),
        database=os.getenv('LOCAL_DB_SOURCE', 'ecommerce_source')
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM customers")
    count = cursor.fetchone()[0]
    print(f"✓ Connected to ecommerce_source - {count:,} customers found")
    cursor.close()
    conn.close()
    
    # Test connection to gold database
    print("\nTesting connection to ecommerce_gold...")
    conn = psycopg2.connect(
        host=os.getenv('LOCAL_POSTGRES_HOST', 'host.docker.internal'),
        port=os.getenv('LOCAL_POSTGRES_PORT', '5432'),
        user=os.getenv('LOCAL_POSTGRES_USER', 'postgres'),
        password=os.getenv('LOCAL_POSTGRES_PASSWORD', 'postgres'),
        database=os.getenv('LOCAL_DB_GOLD', 'ecommerce_gold')
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM dim_customers")
    count = cursor.fetchone()[0]
    print(f"✓ Connected to ecommerce_gold - {count:,} customers in dim_customers")
    cursor.close()
    conn.close()
    
    print("\n✅ All PostgreSQL connections working!")
    
except Exception as e:
    print(f"❌ Connection failed: {e}")
