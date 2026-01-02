#!/usr/bin/env python3
"""
Test an Athena query on a Glue table.

Usage:
    python3 2_test_athena_query.py [OPTIONS] [TABLE_NAME]
    python3 scripts/step5/2_test_athena_query.py [OPTIONS] [TABLE_NAME]

TABLE_NAME:
    raw        Query raw table (default)
    processed  Query processed tables
    insights   Query insights tables
    <name>     Query specific table name

Options:
    -d, --database DATABASE     Database name (overrides SSM Parameter Store)
    -w, --workgroup WORKGROUP   Athena workgroup (overrides SSM Parameter Store)
    -q, --query QUERY           Custom SQL query (overrides default test query)
    -h, --help                  Show this help message

Examples:
    # From scripts/step5/ directory:
    python3 2_test_athena_query.py raw              # Test query on raw table (default)
    python3 2_test_athena_query.py processed        # Test query on processed table
    python3 2_test_athena_query.py insights         # Test query on insights table
    python3 2_test_athena_query.py -q "SELECT COUNT(*) FROM my_db.my_table"  # Run custom query
    python3 2_test_athena_query.py -d my_db -w my_workgroup raw  # With custom database and workgroup
    
    # From project root:
    python3 scripts/step5/2_test_athena_query.py raw
    python3 scripts/step5/2_test_athena_query.py processed
    python3 scripts/step5/2_test_athena_query.py -q "SELECT * FROM my_db.my_table LIMIT 10"
"""
import sys
import argparse
import time
from pathlib import Path

# Add project root to path to import config_loader
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import boto3
from pyspark.utils.config_loader import load_and_set_config, get_glue_database_name


def main():
    parser = argparse.ArgumentParser(
        description="Test an Athena query on a Glue table."
    )
    parser.add_argument("-d", "--database", help="Database name (overrides SSM Parameter Store)")
    parser.add_argument("-w", "--workgroup", help="Athena workgroup (overrides SSM Parameter Store)")
    parser.add_argument("-q", "--query", help="Custom SQL query (overrides default test query)")
    parser.add_argument("table_name", nargs="?", default="raw", help="Table name to query (default: raw)")
    
    args = parser.parse_args()
    
    # Get config from SSM Parameter Store
    try:
        config = load_and_set_config()  # Load config and set environment variables
    except Exception as e:
        print(f"Error: Could not load config from SSM Parameter Store: {e}", file=sys.stderr)
        sys.exit(1)
    
    database_name = args.database or get_glue_database_name()
    workgroup_name = args.workgroup or config.get("athena_workgroup_name", "primary")
    result_location = config.get("athena_query_result_location")
    
    if not database_name:
        print("Error: Could not get database name. Please provide it with -d option.", file=sys.stderr)
        sys.exit(1)
    
    if not result_location:
        print("Error: Could not get result location from SSM Parameter Store.", file=sys.stderr)
        sys.exit(1)
    
    table_name = args.table_name
    
    print("Testing Athena query")
    print(f"Database: {database_name}")
    print(f"Table: {table_name}")
    print(f"Workgroup: {workgroup_name}")
    print(f"Result location: {result_location}")
    print()
    
    # Build query
    if args.query:
        query = args.query
    else:
        # Default test query
        query = f'SELECT COUNT(*) as row_count FROM "{database_name}"."{table_name}"'
    
    print(f"Query: {query}")
    print()
    
    # Initialize Athena client
    athena_client = boto3.client("athena")
    
    # Execute query
    # Note: ResultConfiguration is not needed when using a workgroup with enforce_workgroup_configuration=true
    # The workgroup's result location is automatically used
    print("Executing query...")
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            WorkGroup=workgroup_name
        )
        query_execution_id = response["QueryExecutionId"]
    except Exception as e:
        print(f"✗ Failed to start query execution: {e}", file=sys.stderr)
        sys.exit(1)
    
    print("✓ Query submitted successfully")
    print(f"Query Execution ID: {query_execution_id}")
    print()
    
    # Wait for query to complete
    print("Waiting for query to complete...")
    max_wait = 60
    waited = 0
    
    while waited < max_wait:
        try:
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            state = response["QueryExecution"]["Status"]["State"]
            
            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            
            print(".", end="", flush=True)
            time.sleep(2)
            waited += 2
        except Exception as e:
            print(f"\n✗ Failed to get query state: {e}", file=sys.stderr)
            sys.exit(1)
    
    print()
    
    # Get final status
    try:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        final_state = response["QueryExecution"]["Status"]["State"]
    except Exception as e:
        print(f"✗ Failed to get query execution: {e}", file=sys.stderr)
        sys.exit(1)
    
    if final_state == "SUCCEEDED":
        print("✓ Query completed successfully")
        print()
        print("Results:")
        try:
            # Get query results
            response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            # Print column names
            if "ResultSet" in response and "ResultSetMetadata" in response["ResultSet"]:
                columns = [col["Name"] for col in response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
                print(" | ".join(columns))
                print("-" * (sum(len(c) for c in columns) + len(columns) * 3))
                
                # Print data rows (skip first row which is column names)
                if "Rows" in response["ResultSet"]:
                    for row in response["ResultSet"]["Rows"][1:]:
                        values = []
                        for i, data in enumerate(row["Data"]):
                            values.append(data.get("VarCharValue", ""))
                        print(" | ".join(values))
        except Exception as e:
            print(f"Error getting results: {e}")
    else:
        print(f"✗ Query failed or timed out (Status: {final_state})")
        print()
        print("Query execution details:")
        status = response["QueryExecution"]["Status"]
        print(f"  State: {status['State']}")
        if "StateChangeReason" in status:
            print(f"  Reason: {status['StateChangeReason']}")
        if "AthenaError" in status:
            print(f"  Error: {status['AthenaError']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
