"""
Create PostgreSQL databases using Python
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from dotenv import load_dotenv

load_dotenv()

def create_database(db_name):
    """Create a database if it doesn't exist"""
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(
            host=os.getenv('LOCAL_POSTGRES_HOST', 'localhost'),
            port=os.getenv('LOCAL_POSTGRES_PORT', '5432'),
            user=os.getenv('LOCAL_POSTGRES_USER', 'postgres'),
            password=os.getenv('LOCAL_POSTGRES_PASSWORD', 'postgres'),
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
        exists = cursor.fetchone()
        
        if exists:
            print(f"Database '{db_name}' already exists")
        else:
            cursor.execute(f'CREATE DATABASE {db_name}')
            print(f"✓ Created database '{db_name}'")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error creating database '{db_name}': {e}")
        return False

if __name__ == "__main__":
    print("Creating PostgreSQL databases...")
    create_database('ecommerce_source')
    create_database('ecommerce_gold')
    print("\nDatabases ready!")
