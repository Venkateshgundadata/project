"""
Database Connection Utilities
Provides connection management for PostgreSQL databases using environment variables
"""

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseConnector:
    """Manages database connections using environment variables"""
    
    def __init__(self):
        """Initialize database connector"""
        pass
    
    def get_source_engine(self):
        """Get SQLAlchemy engine for source database"""
        host = os.getenv('LOCAL_POSTGRES_HOST', 'localhost')
        port = os.getenv('LOCAL_POSTGRES_PORT', '5432')
        database = os.getenv('LOCAL_DB_SOURCE', 'ecommerce_source')
        user = os.getenv('LOCAL_POSTGRES_USER')
        password = os.getenv('LOCAL_POSTGRES_PASSWORD')

        if not user or not password:
            raise ValueError("Database credentials not found. Please set LOCAL_POSTGRES_USER and LOCAL_POSTGRES_PASSWORD in .env file.")
        
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        return create_engine(connection_string, poolclass=NullPool)
    
    def get_gold_engine(self):
        """Get SQLAlchemy engine for gold database"""
        host = os.getenv('LOCAL_POSTGRES_HOST', 'localhost')
        port = os.getenv('LOCAL_POSTGRES_PORT', '5432')
        database = os.getenv('LOCAL_DB_GOLD', 'ecommerce_gold')
        user = os.getenv('LOCAL_POSTGRES_USER')
        password = os.getenv('LOCAL_POSTGRES_PASSWORD')

        if not user or not password:
            raise ValueError("Database credentials not found. Please set LOCAL_POSTGRES_USER and LOCAL_POSTGRES_PASSWORD in .env file.")
        
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        return create_engine(connection_string, poolclass=NullPool)
    
    def get_connection_string(self, db_type='source'):
        """Get connection string for specified database type"""
        host = os.getenv('LOCAL_POSTGRES_HOST', 'localhost')
        port = os.getenv('LOCAL_POSTGRES_PORT', '5432')
        user = os.getenv('LOCAL_POSTGRES_USER')
        password = os.getenv('LOCAL_POSTGRES_PASSWORD')

        if not user or not password:
            raise ValueError("Database credentials not found. Please set LOCAL_POSTGRES_USER and LOCAL_POSTGRES_PASSWORD in .env file.")
        
        if db_type == 'source':
            database = os.getenv('LOCAL_DB_SOURCE', 'ecommerce_source')
        elif db_type == 'gold':
            database = os.getenv('LOCAL_DB_GOLD', 'ecommerce_gold')
        else:
            raise ValueError(f"Unknown database type: {db_type}")
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
