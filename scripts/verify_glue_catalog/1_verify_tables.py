#!/usr/bin/env python3
"""
Verify that a Glue table exists in the Glue Data Catalog.

Usage:
    python3 1_verify_tables.py [OPTIONS] [TABLE_NAME]
    python3 scripts/verify_glue_catalog/1_verify_tables.py [OPTIONS] [TABLE_NAME]

TABLE_NAME:
    raw        Verify raw table (default)
    processed  Verify processed tables
    insights   Verify insights tables
    <name>     Verify specific table name

Options:
    -d, --database DATABASE  Database name (overrides SSM Parameter Store)
    -h, --help               Show this help message

Examples:
    # From scripts/verify_glue_catalog/ directory:
    python3 1_verify_table.py raw              # Verify raw table (default)
    python3 1_verify_table.py processed        # Verify processed tables
    python3 1_verify_table.py insights         # Verify insights tables
    python3 1_verify_table.py -d my_db my_table  # Verify specific table in specific database
    
    # From project root:
    python3 scripts/step5/1_verify_table.py raw
    python3 scripts/step5/1_verify_table.py processed
    python3 scripts/step5/1_verify_table.py -d my_database my_table_name
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


def main():
    parser = argparse.ArgumentParser(
        description="Verify that a Glue table exists in the Glue Data Catalog."
    )
    parser.add_argument("-d", "--database", help="Database name (overrides SSM Parameter Store)")
    parser.add_argument("table_name", nargs="?", default="raw", help="Table name to verify (default: raw)")
    
    args = parser.parse_args()
    
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
    
    table_name = args.table_name
    
    print(f"Verifying table: {database_name}.{table_name}")
    print()
    
    # Initialize Glue client
    glue_client = boto3.client("glue")
    
    try:
        response = glue_client.get_table(DatabaseName=database_name, Name=table_name)
        table = response["Table"]
        
        print(f"✓ Table '{table_name}' exists in database '{database_name}'")
        print()
        print("Table details:")
        print(f"  Name: {table['Name']}")
        print(f"  CreateTime: {format_datetime(table['CreateTime'])}")
        if 'UpdateTime' in table:
            print(f"  UpdateTime: {format_datetime(table['UpdateTime'])}")
        print(f"  Location: {table['StorageDescriptor']['Location']}")
        print(f"  Columns: {', '.join([col['Name'] for col in table['StorageDescriptor']['Columns']])}")
        print()
        print("To view full table schema:")
        print(f"  aws glue get-table --database-name {database_name} --name {table_name}")
        
    except glue_client.exceptions.EntityNotFoundException:
        print(f"✗ Table '{table_name}' not found in database '{database_name}'")
        print()
        print("Available tables in database:")
        try:
            tables_response = glue_client.get_tables(DatabaseName=database_name)
            if tables_response.get("TableList"):
                for tbl in tables_response["TableList"]:
                    print(f"  - {tbl['Name']}")
            else:
                print("  (No tables found)")
        except Exception as e:
            print(f"  (Could not list tables: {e})")
        print()
        print("Make sure the table exists. If using crawlers, ensure they have completed successfully.")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error verifying table: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
