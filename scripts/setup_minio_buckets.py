"""
Setup MinIO Buckets
Creates the required buckets for Bronze, Silver, and Gold layers
"""

import sys
sys.path.append('.')

from src.utils.minio_client import MinIOClient

def main():
    """Create MinIO buckets"""
    print("=" * 80)
    print("MINIO BUCKET SETUP")
    print("=" * 80)
    
    minio_client = MinIOClient()
    
    buckets = ['bronze', 'silver', 'gold']
    
    for bucket in buckets:
        try:
            minio_client.create_bucket(bucket)
        except Exception as e:
            print(f"Error creating bucket '{bucket}': {e}")
    
    print("\n" + "=" * 80)
    print("BUCKET SETUP COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()
