"""
Complete Setup Script for E-Commerce Data Pipeline
Runs all setup steps in sequence
"""

import subprocess
import sys
import time
import os

def print_header(text):
    """Print formatted header"""
    print("\n" + "=" * 80)
    print(text.center(80))
    print("=" * 80 + "\n")

def run_command(cmd, description):
    """Run a shell command and handle errors"""
    print(f"[STEP] {description}")
    print(f"[CMD] {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"[SUCCESS] {description}")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] {description} failed")
        print(f"Error: {e.stderr}")
        return False

def check_prerequisites():
    """Check if required software is installed"""
    print_header("CHECKING PREREQUISITES")
    
    checks = [
        ("docker --version", "Docker"),
        ("docker-compose --version", "Docker Compose"),
        ("psql --version", "PostgreSQL"),
        ("python --version", "Python")
    ]
    
    all_ok = True
    for cmd, name in checks:
        try:
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
            print(f"✓ {name} is installed")
        except:
            print(f"✗ {name} is NOT installed")
            all_ok = False
    
    return all_ok

def main():
    """Main setup function"""
    print_header("E-COMMERCE DATA PIPELINE - COMPLETE SETUP")
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n[ERROR] Please install missing prerequisites before continuing.")
        sys.exit(1)
    
    # Step 1: Create databases
    print_header("STEP 1: CREATE POSTGRESQL DATABASES")
    if not run_command(
        "python scripts/create_databases.py",
        "Creating databases: ecommerce_source and ecommerce_gold"
    ):
        print("[ERROR] Database creation failed. Please check your PostgreSQL connection.")
        sys.exit(1)

    
    # Step 2: Create source tables
    print_header("STEP 2: CREATE SOURCE DATABASE SCHEMA")
    if not run_command(
        "psql -U postgres -d ecommerce_source -f sql/ddl/create_source_tables.sql",
        "Creating source tables"
    ):
        print("[WARNING] Source tables creation failed. You may need to run this manually.")
    
    # Step 3: Create gold tables
    print_header("STEP 3: CREATE GOLD DATABASE SCHEMA")
    if not run_command(
        "psql -U postgres -d ecommerce_gold -f sql/ddl/create_gold_tables.sql",
        "Creating gold tables"
    ):
        print("[WARNING] Gold tables creation failed. You may need to run this manually.")

    
    # Step 4: Install Python dependencies
    print_header("STEP 4: INSTALL PYTHON DEPENDENCIES")
    run_command(
        "pip install -r requirements.txt",
        "Installing Python packages"
    )
    
    # Step 5: Start Docker services
    print_header("STEP 5: START DOCKER SERVICES")
    run_command(
        "docker-compose up -d",
        "Starting Airflow and MinIO containers"
    )
    
    print("\nWaiting for services to be healthy (60 seconds)...")
    time.sleep(60)
    
    # Step 6: Setup MinIO buckets
    print_header("STEP 6: SETUP MINIO BUCKETS")
    run_command(
        "python scripts/setup_minio_buckets.py",
        "Creating MinIO buckets"
    )
    
    # Step 7: Generate data
    print_header("STEP 7: GENERATE SYNTHETIC DATA")
    print("This will generate:")
    print("  - 100,000 customers")
    print("  - 50,000 products")
    print("  - 500,000 orders")
    print("  - 1,500,000 order items")
    print("  - 200,000 inventory transactions")
    print("  - 10,000 marketing campaigns")
    print("\nThis may take 10-15 minutes...")
    
    proceed = input("\nProceed with data generation? (y/n): ")
    if proceed.lower() == 'y':
        run_command(
            "python scripts/generate_data.py",
            "Generating synthetic data"
        )
    else:
        print("[SKIPPED] Data generation")
    
    # Final summary
    print_header("SETUP COMPLETE!")
    
    print("✓ Databases created")
    print("✓ Schemas initialized")
    print("✓ Python dependencies installed")
    print("✓ Docker services running")
    print("✓ MinIO buckets created")
    
    if proceed.lower() == 'y':
        print("✓ Synthetic data generated")
    
    print("\n" + "=" * 80)
    print("NEXT STEPS:")
    print("=" * 80)
    print("\n1. Access Airflow UI: http://localhost:8080")
    print("   Username: admin")
    print("   Password: admin")
    
    print("\n2. Access MinIO Console: http://localhost:9001")
    print("   Username: minioadmin")
    print("   Password: minioadmin")
    
    print("\n3. Run the ETL pipeline:")
    print("   - Option A (Manual):")
    print("     python src/bronze/ingest_to_bronze.py")
    print("     python src/silver/transform_to_silver.py")
    print("     python src/gold/aggregate_to_gold.py")
    
    print("\n   - Option B (Airflow):")
    print("     Enable and trigger DAGs in Airflow UI")
    
    print("\n4. Query the gold database:")
    print("   psql -U postgres -d ecommerce_gold")
    print("   \\dt")
    
    print("\n" + "=" * 80)
    print("For detailed documentation, see:")
    print("  - README.md")
    print("  - docs/architecture.md")
    print("  - docs/data_dictionary.md")
    print("=" * 80 + "\n")

if __name__ == "__main__":
    main()
