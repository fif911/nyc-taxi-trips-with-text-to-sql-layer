#!/usr/bin/env python3
"""
Verify Glue tables created by processed and insights crawlers.

Usage:
    python3 2_verify_processed_tables.py [OPTIONS]
    python3 scripts/step8/2_verify_processed_tables.py [OPTIONS]

Options:
    --processed-only     Verify only processed tables
    --insights-only      Verify only insights tables
    --list-all           List all tables in database
    -h, --help           Show this help message

Examples:
    # From scripts/step8/ directory:
    python3 2_verify_processed_tables.py              # Verify all processed and insights tables
    python3 2_verify_processed_tables.py --list-all    # List all tables in database
    python3 2_verify_processed_tables.py --processed-only  # Verify only processed tables
    python3 2_verify_processed_tables.py --insights-only   # Verify only insights tables
    
    # From project root:
    python3 scripts/step8/2_verify_processed_tables.py
    python3 scripts/step8/2_verify_processed_tables.py --list-all
"""
import sys
import argparse
from datetime import datetime
from pathlib import Path

# Add project root to path to import config_loader
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import boto3
from pyspark.utils.config_loader import load_and_set_config, get_glue_database_name


def format_datetime(timestamp):
    """Format timestamp to readable string."""
    if isinstance(timestamp, datetime):
        return timestamp.strftime("%Y-%m-%d %H:%M:%S")
    return str(timestamp)


def list_all_tables(glue_client, database_name: str):
    """List all tables in the database."""
    print(f"All tables in database '{database_name}':")
    print()
    
    try:
        paginator = glue_client.get_paginator("get_tables")
        table_count = 0
        
        for page in paginator.paginate(DatabaseName=database_name):
            if "TableList" in page:
                for table in page["TableList"]:
                    table_count += 1
                    print(f"  {table_count}. {table['Name']}")
                    if "StorageDescriptor" in table and "Location" in table["StorageDescriptor"]:
                        location = table["StorageDescriptor"]["Location"]
                        print(f"     Location: {location}")
                    if "CreateTime" in table:
                        print(f"     Created: {format_datetime(table['CreateTime'])}")
                    print()
        
        if table_count == 0:
            print("  (No tables found)")
        else:
            print(f"Total: {table_count} table(s)")
        
    except Exception as e:
        print(f"✗ Error listing tables: {e}", file=sys.stderr)
        sys.exit(1)


def verify_table(glue_client, database_name: str, table_name: str) -> bool:
    """
    Verify a specific table exists and show details.
    
    Returns:
        True if table exists, False otherwise
    """
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        table = response["Table"]
        
        print(f"✓ Table '{table_name}' exists in database '{database_name}'")
        print()
        print("Table details:")
        print(f"  Name: {table['Name']}")
        print(f"  CreateTime: {format_datetime(table['CreateTime'])}")
        if "UpdateTime" in table:
            print(f"  UpdateTime: {format_datetime(table['UpdateTime'])}")
        if "StorageDescriptor" in table and "Location" in table["StorageDescriptor"]:
            print(f"  Location: {table['StorageDescriptor']['Location']}")
        if "StorageDescriptor" in table and "Columns" in table["StorageDescriptor"]:
            columns = [col["Name"] for col in table["StorageDescriptor"]["Columns"]]
            print(f"  Columns ({len(columns)}): {', '.join(columns[:10])}")
            if len(columns) > 10:
                print(f"    ... and {len(columns) - 10} more")
        print()
        
        return True
        
    except glue_client.exceptions.EntityNotFoundException:
        print(f"✗ Table '{table_name}' not found in database '{database_name}'")
        print()
        return False
    except Exception as e:
        print(f"✗ Error verifying table '{table_name}': {e}", file=sys.stderr)
        return False


def find_processed_tables(glue_client, database_name: str) -> list:
    """Find tables that appear to be from processed data."""
    processed_tables = []
    try:
        paginator = glue_client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database_name):
            if "TableList" in page:
                for table in page["TableList"]:
                    table_name = table["Name"].lower()
                    location = table.get("StorageDescriptor", {}).get("Location", "").lower()
                    
                    # Look for tables in processed/ folder or with processed-related names
                    if "processed" in location or "processed" in table_name:
                        if "trips_cleaned" in table_name or "trips_cleaned" in location:
                            processed_tables.append(table["Name"])
    except Exception as e:
        print(f"Warning: Could not search for processed tables: {e}")
    
    return processed_tables


def find_insights_tables(glue_client, database_name: str) -> list:
    """Find tables that appear to be from insights data."""
    insights_tables = []
    try:
        paginator = glue_client.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database_name):
            if "TableList" in page:
                for table in page["TableList"]:
                    table_name = table["Name"].lower()
                    location = table.get("StorageDescriptor", {}).get("Location", "").lower()
                    
                    # Look for tables in insights/ folder or with insights-related names
                    if "insights" in location or "insights" in table_name:
                        insights_tables.append(table["Name"])
    except Exception as e:
        print(f"Warning: Could not search for insights tables: {e}")
    
    return insights_tables


def main():
    parser = argparse.ArgumentParser(
        description="Verify Glue tables created by processed and insights crawlers.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--processed-only",
        action="store_true",
        help="Verify only processed tables"
    )
    parser.add_argument(
        "--insights-only",
        action="store_true",
        help="Verify only insights tables"
    )
    parser.add_argument(
        "--list-all",
        action="store_true",
        help="List all tables in database"
    )
    parser.add_argument(
        "-d", "--database",
        help="Database name (overrides SSM Parameter Store)"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.processed_only and args.insights_only:
        print("Error: Cannot specify both --processed-only and --insights-only", file=sys.stderr)
        sys.exit(1)
    
    # Get database name from SSM Parameter Store or argument
    database_name = args.database
    if not database_name:
        try:
            load_and_set_config()  # Load config and set environment variables
            database_name = get_glue_database_name()
        except Exception as e:
            print(f"Error: Could not load config from SSM Parameter Store: {e}", file=sys.stderr)
            print("Please provide database name with -d option or ensure SSM parameter is configured.", file=sys.stderr)
            sys.exit(1)
    
    if not database_name:
        print("Error: Could not get database name. Please provide it with -d option.", file=sys.stderr)
        sys.exit(1)
    
    print("=" * 60)
    print("Step 8.2: Verify Processed Data Tables")
    print("=" * 60)
    print()
    print(f"Database: {database_name}")
    print()
    
    # Initialize Glue client
    glue_client = boto3.client("glue")
    
    # List all tables if requested
    if args.list_all:
        list_all_tables(glue_client, database_name)
        return
    
    all_success = True
    
    # Verify processed tables
    if not args.insights_only:
        print("=" * 60)
        print("Verifying Processed Tables")
        print("=" * 60)
        print()
        
        processed_tables = find_processed_tables(glue_client, database_name)
        
        if processed_tables:
            for table_name in processed_tables:
                print(f"Verifying table: {database_name}.{table_name}")
                print()
                if not verify_table(glue_client, database_name, table_name):
                    all_success = False
        else:
            print("No processed tables found. Common table names to check:")
            print("  - trips_cleaned")
            print("  - processed")
            print()
            print("Trying to verify 'trips_cleaned' table...")
            print()
            if not verify_table(glue_client, database_name, "trips_cleaned"):
                print("Trying to verify 'processed' table...")
                print()
                if not verify_table(glue_client, database_name, "processed"):
                    all_success = False
                    print()
                    print("Tip: Use --list-all to see all available tables")
    
    # Verify insights tables
    if not args.processed_only:
        if not args.insights_only:
            print()
            print("=" * 60)
        
        print("=" * 60)
        print("Verifying Insights Tables")
        print("=" * 60)
        print()
        
        insights_tables = find_insights_tables(glue_client, database_name)
        
        if insights_tables:
            for table_name in insights_tables:
                print(f"Verifying table: {database_name}.{table_name}")
                print()
                if not verify_table(glue_client, database_name, table_name):
                    all_success = False
        else:
            print("No insights tables found. Searching for common insights table names...")
            print()
            
            # Try common insights table names
            common_names = [
                "trip_volume_by_hour",
                "trip_volume_by_day",
                "trip_volume_by_zone",
                "popular_pickup_zones",
                "popular_dropoff_zones",
                "revenue_by_payment_type",
                "revenue_by_time",
                "insights"
            ]
            
            found_any = False
            for table_name in common_names:
                try:
                    response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
                    found_any = True
                    print(f"Verifying table: {database_name}.{table_name}")
                    print()
                    verify_table(glue_client, database_name, table_name)
                    print()
                except glue_client.exceptions.EntityNotFoundException:
                    continue
            
            if not found_any:
                print("No insights tables found with common names.")
                print()
                print("Tip: Use --list-all to see all available tables")
                all_success = False
    
    print("=" * 60)
    if all_success:
        print("✓ Table verification complete")
        print("=" * 60)
        print()
        print("Next steps:")
        print("  1. Run: python3 scripts/step9/1_run_sample_queries.py")
        print("  2. Or test queries manually using Athena")
    else:
        print("✗ Some tables were not found")
        print("=" * 60)
        print()
        print("Troubleshooting:")
        print("  1. Make sure crawlers have completed (run step 8.1)")
        print("  2. Check crawler status: aws glue get-crawler --name <crawler-name>")
        print("  3. Use --list-all to see all available tables")
        sys.exit(1)


if __name__ == "__main__":
    main()

