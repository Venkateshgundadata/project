"""
Check columns in silver layer data
"""
import sys
sys.path.append('.')

from src.utils.minio_client import MinIOClient
from datetime import datetime

minio_client = MinIOClient()
run_date = datetime.now()

tables = ['customers', 'products', 'orders', 'order_items', 'inventory_transactions', 'marketing_campaigns']

for table in tables:
    try:
        path = minio_client.get_partition_path(table, run_date)
        df = minio_client.read_dataframe('silver', path)
        print(f"\n{table.upper()}:")
        print(f"Columns: {list(df.columns)}")
        print(f"Rows: {len(df)}")
    except Exception as e:
        print(f"\n{table.upper()}: Error - {e}")
