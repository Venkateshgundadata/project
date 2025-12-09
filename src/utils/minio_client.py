"""
MinIO Client Utilities
Provides S3-compatible object storage operations for MinIO using environment variables
"""

import boto3
from botocore.client import Config
import pandas as pd
import io
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class MinIOClient:
    """Manages MinIO S3 operations"""
    
    def __init__(self):
        """Initialize MinIO client with environment variables"""
        self.endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
        self.access_key = os.getenv('MINIO_ROOT_USER')
        self.secret_key = os.getenv('MINIO_ROOT_PASSWORD')

        if not self.access_key or not self.secret_key:
            raise ValueError("MinIO credentials not found. Please set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD in .env file.")
        
        self.client = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
    
    def create_bucket(self, bucket_name):
        """Create a bucket if it doesn't exist"""
        try:
            self.client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' already exists")
        except:
            self.client.create_bucket(Bucket=bucket_name)
            print(f"Created bucket '{bucket_name}'")
    
    def upload_dataframe(self, df, bucket, key, file_format='parquet'):
        """Upload pandas DataFrame to MinIO"""
        buffer = io.BytesIO()
        
        if file_format == 'parquet':
            df.to_parquet(buffer, index=False, engine='pyarrow')
        elif file_format == 'csv':
            df.to_csv(buffer, index=False)
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        buffer.seek(0)
        self.client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        print(f"Uploaded to s3://{bucket}/{key}")
    
    def read_dataframe(self, bucket, key, file_format='parquet'):
        """Read pandas DataFrame from MinIO"""
        obj = self.client.get_object(Bucket=bucket, Key=key)
        
        if file_format == 'parquet':
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        elif file_format == 'csv':
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        else:
            raise ValueError(f"Unsupported format: {file_format}")
        
        print(f"Read {len(df)} rows from s3://{bucket}/{key}")
        return df
    
    def list_objects(self, bucket, prefix=''):
        """List objects in bucket with optional prefix"""
        try:
            response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except Exception as e:
            print(f"Error listing objects: {e}")
            return []
    
    def delete_object(self, bucket, key):
        """Delete an object from bucket"""
        self.client.delete_object(Bucket=bucket, Key=key)
        print(f"Deleted s3://{bucket}/{key}")
    
    def get_partition_path(self, table_name, date=None):
        """Generate partition path for table by date"""
        if date is None:
            date = datetime.now()
        
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')
        
        return f"{table_name}/year={year}/month={month}/day={day}/data.parquet"
