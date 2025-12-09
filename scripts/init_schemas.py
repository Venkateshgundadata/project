"""
Initialize database schemas using Python
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def execute_sql_file(db_name, sql_file):
    """Execute SQL file on specified database"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('LOCAL_POSTGRES_HOST', 'localhost'),
            port=os.getenv('LOCAL_POSTGRES_PORT', '5432'),
            user=os.getenv('LOCAL_POSTGRES_USER', 'postgres'),
            password=os.getenv('LOCAL_POSTGRES_PASSWORD', 'postgres'),
            database=db_name
        )
        cursor = conn.cursor()
        
        # Read and execute SQL file
        with open(sql_file, 'r') as f:
            sql = f.read()
        
        cursor.execute(sql)
        conn.commit()
        
        cursor.close()
        conn.close()
        print(f"✓ Executed {sql_file} on {db_name}")
        return True
    except Exception as e:
        print(f"Error executing {sql_file}: {e}")
        return False

if __name__ == "__main__":
    print("Initializing database schemas...\n")
    
    # Create source tables
    execute_sql_file('ecommerce_source', 'sql/ddl/create_source_tables.sql')
    
    # Create gold tables
    execute_sql_file('ecommerce_gold', 'sql/ddl/create_gold_tables.sql')
    
    print("\nSchemas initialized successfully!")
