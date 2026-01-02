#!/usr/bin/env python3
"""
Run sample Athena queries on Glue tables.

This script implements Step 9.1: Test Queries from the architecture plan.
It runs three sample queries:
1. Trip Volume by Day
2. Average Fare by Payment Type
3. Popular Pickup Zones

Usage:
    python3 1_run_sample_queries.py [OPTIONS]
    python3 scripts/test_queries/1_run_sample_queries.py [OPTIONS]

Options:
    -d, --database DATABASE     Database name (overrides SSM Parameter Store)
    -w, --workgroup WORKGROUP   Athena workgroup (overrides SSM Parameter Store)
    -t, --table TABLE           Processed table name (default: trips_cleaned)
    -q, --query QUERY_NUMBER    Run specific query (1, 2, or 3)
    -h, --help                  Show this help message

Examples:
    # From scripts/test_queries/ directory:
    python3 1_run_sample_queries.py                    # Run all queries
    python3 1_run_sample_queries.py -q 1               # Run only query 1
    python3 1_run_sample_queries.py -t my_table        # Use different table name
    
    # From project root:
    python3 scripts/test_queries/1_run_sample_queries.py
    python3 scripts/test_queries/1_run_sample_queries.py -q 2
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


def execute_athena_query(athena_client, query: str, workgroup: str, result_location: str, query_name: str):
    """
    Execute an Athena query and return results.
    
    Args:
        athena_client: Boto3 Athena client
        query: SQL query string
        workgroup: Athena workgroup name
        result_location: S3 location for query results
        query_name: Name of the query for logging
        
    Returns:
        Tuple of (query_execution_id, results_dict) or None if failed
    """
    print(f"Executing query: {query_name}")
    print(f"SQL: {query}")
    print()
    
    # Note: ResultConfiguration is not needed when using a workgroup with enforce_workgroup_configuration=true
    # The workgroup's result location is automatically used
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            WorkGroup=workgroup
        )
        query_execution_id = response["QueryExecutionId"]
        print(f"✓ Query submitted successfully")
        print(f"Query Execution ID: {query_execution_id}")
        print()
    except Exception as e:
        print(f"✗ Failed to start query execution: {e}", file=sys.stderr)
        return None
    
    # Wait for query to complete
    print("Waiting for query to complete...")
    max_wait = 120  # 2 minutes
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
            return None
    
    print()
    
    # Get final status
    try:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        final_state = response["QueryExecution"]["Status"]["State"]
        status = response["QueryExecution"]["Status"]
    except Exception as e:
        print(f"✗ Failed to get query execution: {e}", file=sys.stderr)
        return None
    
    if final_state == "SUCCEEDED":
        print("✓ Query completed successfully")
        print()
        
        # Get and display results
        try:
            response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
            
            if "ResultSet" in response and "ResultSetMetadata" in response["ResultSet"]:
                columns = [col["Name"] for col in response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
                print("Results:")
                print(" | ".join(columns))
                print("-" * (sum(len(c) for c in columns) + len(columns) * 3))
                
                # Print data rows (skip first row which is column names)
                if "Rows" in response["ResultSet"]:
                    row_count = 0
                    for row in response["ResultSet"]["Rows"][1:]:  # Skip header row
                        values = []
                        for i, data in enumerate(row["Data"]):
                            values.append(data.get("VarCharValue", ""))
                        print(" | ".join(values))
                        row_count += 1
                    
                    if row_count == 0:
                        print("(No rows returned)")
                    else:
                        print()
                        print(f"Total rows: {row_count}")
        except Exception as e:
            print(f"Error getting results: {e}")
        
        print()
        print("=" * 80)
        print()
        
        return query_execution_id
    else:
        print(f"✗ Query failed or timed out (Status: {final_state})")
        print()
        print("Query execution details:")
        print(f"  State: {status['State']}")
        if "StateChangeReason" in status:
            print(f"  Reason: {status['StateChangeReason']}")
        if "AthenaError" in status:
            print(f"  Error: {status['AthenaError']}")
        print()
        print("=" * 80)
        print()
        return None


def main():
    parser = argparse.ArgumentParser(
        description="Run sample Athena queries on Glue tables.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-d", "--database", help="Database name (overrides SSM Parameter Store)")
    parser.add_argument("-w", "--workgroup", help="Athena workgroup (overrides SSM Parameter Store)")
    parser.add_argument("-t", "--table", default="trips_cleaned", help="Processed table name (default: trips_cleaned)")
    parser.add_argument("-q", "--query", type=int, choices=[1, 2, 3], help="Run specific query (1, 2, or 3)")
    
    args = parser.parse_args()
    
    # Get config from SSM Parameter Store
    try:
        config = load_and_set_config()
    except Exception as e:
        print(f"Error: Could not load config from SSM Parameter Store: {e}", file=sys.stderr)
        print("Please ensure SSM parameter is configured or provide values via command-line options.", file=sys.stderr)
        sys.exit(1)
    
    database_name = args.database or get_glue_database_name()
    workgroup_name = args.workgroup or config.get("athena_workgroup_name", "primary")
    result_location = config.get("athena_query_result_location")
    table_name = args.table
    
    if not database_name:
        print("Error: Could not get database name. Please provide it with -d option.", file=sys.stderr)
        sys.exit(1)
    
    if not result_location:
        print("Error: Could not get result location from SSM Parameter Store.", file=sys.stderr)
        sys.exit(1)
    
    print("=" * 80)
    print("Step 9.1: Athena Query Layer - Sample Queries")
    print("=" * 80)
    print()
    print(f"Database: {database_name}")
    print(f"Table: {table_name}")
    print(f"Workgroup: {workgroup_name}")
    print(f"Result location: {result_location}")
    print()
    print("=" * 80)
    print()
    
    # Initialize Athena client
    athena_client = boto3.client("athena")
    
    # Query 1: Trip Volume by Day
    query1 = f"""
SELECT 
    date_trunc('day', tpep_pickup_datetime) as trip_date,
    COUNT(*) as trip_count
FROM "{database_name}"."{table_name}"
GROUP BY date_trunc('day', tpep_pickup_datetime)
ORDER BY trip_date
LIMIT 20
"""
    
    # Query 2: Average Fare by Payment Type
    query2 = f"""
SELECT 
    payment_type,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    COUNT(*) as trip_count
FROM "{database_name}"."{table_name}"
WHERE payment_type IS NOT NULL
GROUP BY payment_type
ORDER BY payment_type
"""
    
    # Query 3: Popular Pickup Zones
    query3 = f"""
SELECT 
    pulocationid,
    COUNT(*) as pickup_count,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance
FROM "{database_name}"."{table_name}"
WHERE pulocationid IS NOT NULL
GROUP BY pulocationid
ORDER BY pickup_count DESC
LIMIT 10
"""
    
    queries = [
        ("Trip Volume by Day", query1),
        ("Average Fare by Payment Type", query2),
        ("Popular Pickup Zones", query3)
    ]
    
    # Run queries
    if args.query:
        # Run specific query
        query_num = args.query - 1
        if 0 <= query_num < len(queries):
            query_name, query_sql = queries[query_num]
            execute_athena_query(athena_client, query_sql, workgroup_name, result_location, query_name)
        else:
            print(f"Error: Invalid query number. Must be 1, 2, or 3.", file=sys.stderr)
            sys.exit(1)
    else:
        # Run all queries
        for query_name, query_sql in queries:
            execute_athena_query(athena_client, query_sql, workgroup_name, result_location, query_name)
    
    print("=" * 80)
    print("✓ Sample queries completed")
    print("=" * 80)
    print()
    print("Next steps:")
    print("  1. Review query results above")
    print("  2. Optionally create views: python3 scripts/test_queries/2_create_views.py")
    print("  3. Proceed to Step 10: Text-to-SQL Interface")


if __name__ == "__main__":
    main()

