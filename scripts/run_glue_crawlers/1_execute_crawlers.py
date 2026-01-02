#!/usr/bin/env python3
"""
Execute Glue crawlers for raw, processed, and insights data.

Usage:
    python3 1_execute_crawlers.py [CRAWLER_TYPE] [OPTIONS]
    python3 scripts/run_glue_crawlers/1_execute_crawlers.py [CRAWLER_TYPE] [OPTIONS]

CRAWLER_TYPE:
    raw        Execute raw data crawler
    processed  Execute processed data crawler
    insights   Execute insights data crawler
    all        Execute all crawlers sequentially (default)

Options:
    --wait     Wait for each crawler to complete before starting next
    -h, --help Show this help message

Examples:
    # From scripts/run_glue_crawlers/ directory:
    python3 1_execute_crawlers.py              # Execute all crawlers (default)
    python3 1_execute_crawlers.py all          # Execute all crawlers
    python3 1_execute_crawlers.py all --wait   # Execute all crawlers and wait for completion
    python3 1_execute_crawlers.py processed    # Execute only processed crawler
    python3 1_execute_crawlers.py insights --wait  # Execute insights crawler and wait
    python3 1_execute_processed_crawlers.py raw          # Execute only raw crawler
    
    # From project root:
    python3 scripts/run_glue_crawlers/1_execute_processed_crawlers.py
    python3 scripts/run_glue_crawlers/1_execute_processed_crawlers.py all --wait
    python3 scripts/run_glue_crawlers/1_execute_processed_crawlers.py processed
"""
import sys
import argparse
import time
from datetime import datetime
from pathlib import Path

# Add project root to path to import config_loader
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import boto3
from pyspark.utils.config_loader import load_and_set_config


def format_elapsed_time(seconds: float) -> str:
    """Format elapsed time in a human-readable way."""
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def get_timestamp() -> str:
    """Get current timestamp for logging."""
    return datetime.now().strftime("%H:%M:%S")


def wait_for_crawler(glue_client, crawler_name: str, max_wait: int = 300) -> bool:
    """
    Wait for crawler to complete with detailed logging.
    
    Args:
        glue_client: Boto3 Glue client
        crawler_name: Name of the crawler
        max_wait: Maximum time to wait in seconds (default: 5 minutes)
    
    Returns:
        True if crawler completed successfully, False otherwise
    """
    print(f"\n{'─' * 60}")
    print(f"⏳ Waiting for crawler '{crawler_name}' to complete...")
    print(f"{'─' * 60}")
    
    start_time = time.time()
    check_interval = 10  # Check every 10 seconds
    last_state = None
    check_count = 0
    
    while time.time() - start_time < max_wait:
        try:
            response = glue_client.get_crawler(Name=crawler_name)
            crawler = response["Crawler"]
            state = crawler["State"]
            last_crawl = crawler.get("LastCrawl", {})
            last_crawl_status = last_crawl.get("Status", "UNKNOWN")
            last_crawl_time = last_crawl.get("StartTime")
            
            elapsed = time.time() - start_time
            check_count += 1
            
            # Only print if state changed or every 3rd check
            if state != last_state or check_count % 3 == 0:
                status_icon = "🔄" if state == "RUNNING" else "✅" if state == "READY" else "⚠️" if state == "STOPPING" else "❌"
                print(f"[{get_timestamp()}] {status_icon} State: {state:10s} | Status: {last_crawl_status:10s} | Elapsed: {format_elapsed_time(elapsed)}")
                
                # Show additional details
                if last_crawl_time:
                    try:
                        crawl_start = last_crawl_time.strftime("%H:%M:%S")
                        print(f"         └─ Crawl started at: {crawl_start}")
                    except:
                        pass
                
                last_state = state
            
            if state == "READY":
                if last_crawl_status == "SUCCEEDED":
                    elapsed_total = time.time() - start_time
                    print(f"\n{'─' * 60}")
                    print(f"✅ Crawler completed successfully!")
                    print(f"   Total time: {format_elapsed_time(elapsed_total)}")
                    
                    # Try to get metrics
                    try:
                        metrics_response = glue_client.get_crawler_metrics(CrawlerNameList=[crawler_name])
                        if metrics_response.get("CrawlerMetricsList"):
                            metrics = metrics_response["CrawlerMetricsList"][0]
                            if "LastRuntimeSeconds" in metrics:
                                print(f"   Runtime: {metrics['LastRuntimeSeconds']:.1f}s")
                            if "TablesCreated" in metrics:
                                print(f"   Tables created: {metrics['TablesCreated']}")
                            if "TablesUpdated" in metrics:
                                print(f"   Tables updated: {metrics['TablesUpdated']}")
                            if "TablesDeleted" in metrics:
                                print(f"   Tables deleted: {metrics['TablesDeleted']}")
                    except Exception as e:
                        # Metrics not critical, just skip
                        pass
                    
                    print(f"{'─' * 60}\n")
                    return True
                elif last_crawl_status == "FAILED":
                    print(f"\n{'─' * 60}")
                    print(f"❌ Crawler failed!")
                    
                    # Try to get error message
                    try:
                        if "ErrorMessage" in last_crawl:
                            print(f"   Error: {last_crawl['ErrorMessage']}")
                        if "LogStream" in last_crawl:
                            print(f"   Log stream: {last_crawl['LogStream']}")
                    except:
                        pass
                    
                    print(f"{'─' * 60}\n")
                    return False
                else:
                    # Crawler is ready but hasn't run yet
                    break
            
            if state in ["STOPPING", "STOPPED"]:
                print(f"\n{'─' * 60}")
                print(f"⚠️  Crawler stopped (state: {state})")
                print(f"{'─' * 60}\n")
                return False
            
            time.sleep(check_interval)
        except Exception as e:
            print(f"[{get_timestamp()}] ⚠️  Error checking crawler status: {e}")
            time.sleep(check_interval)
    
    elapsed_total = time.time() - start_time
    print(f"\n{'─' * 60}")
    print(f"⏱️  Timeout waiting for crawler (waited {format_elapsed_time(elapsed_total)})")
    print(f"{'─' * 60}\n")
    return False


def execute_crawler(glue_client, crawler_name: str, crawler_type: str, wait: bool = False) -> bool:
    """
    Execute a Glue crawler with detailed logging.
    
    Args:
        glue_client: Boto3 Glue client
        crawler_name: Name of the crawler
        crawler_type: Type of crawler (for display)
        wait: Whether to wait for completion
    
    Returns:
        True if crawler started successfully, False otherwise
    """
    print(f"📋 Crawler Information:")
    print(f"   Name: {crawler_name}")
    print(f"   Type: {crawler_type}")
    
    # Get crawler details
    try:
        crawler_response = glue_client.get_crawler(Name=crawler_name)
        crawler = crawler_response["Crawler"]
        
        # Show database
        if "DatabaseName" in crawler:
            print(f"   Database: {crawler['DatabaseName']}")
        
        # Show S3 targets
        if "Targets" in crawler and "S3Targets" in crawler["Targets"]:
            s3_targets = crawler["Targets"]["S3Targets"]
            if s3_targets:
                print(f"   S3 Targets:")
                for target in s3_targets:
                    path = target.get("Path", "N/A")
                    print(f"     • {path}")
        
        # Show current state
        current_state = crawler.get("State", "UNKNOWN")
        print(f"   Current state: {current_state}")
        
        # Show last crawl info if available
        last_crawl = crawler.get("LastCrawl", {})
        if last_crawl:
            last_status = last_crawl.get("Status", "UNKNOWN")
            last_time = last_crawl.get("StartTime")
            if last_time:
                try:
                    last_time_str = last_time.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"   Last crawl: {last_status} at {last_time_str}")
                except:
                    print(f"   Last crawl: {last_status}")
        
        print()
        
    except Exception as e:
        print(f"   ⚠️  Could not fetch crawler details: {e}")
        print()
    
    # Start the crawler
    print(f"🚀 Starting crawler...")
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"✅ Crawler started successfully at {get_timestamp()}")
        print()
        
        if wait:
            success = wait_for_crawler(glue_client, crawler_name)
            if not success:
                return False
        else:
            print(f"ℹ️  Crawler is running in the background.")
            print(f"   Monitor progress with:")
            print(f"     aws glue get-crawler --name {crawler_name}")
            print(f"     aws glue get-crawler-metrics --crawler-name-list {crawler_name}")
            print()
        
        return True
        
    except glue_client.exceptions.CrawlerRunningException:
        print(f"⚠️  Crawler is already running")
        if wait:
            print(f"   Waiting for current run to complete...")
            return wait_for_crawler(glue_client, crawler_name)
        return True
    except Exception as e:
        print(f"❌ Failed to start crawler: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Execute Glue crawlers for raw, processed, and insights data.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "crawler_type",
        nargs="?",
        default="all",
        choices=["raw", "processed", "insights", "all"],
        help="Crawler type to execute: 'raw', 'processed', 'insights', or 'all' (default: all)"
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for each crawler to complete before starting next"
    )
    
    args = parser.parse_args()
    
    # Get crawler names from SSM Parameter Store
    try:
        config = load_and_set_config()  # Load config and set environment variables
    except Exception as e:
        print(f"Error: Could not load config from SSM Parameter Store: {e}", file=sys.stderr)
        sys.exit(1)
    
    raw_crawler_name = config.get("glue_raw_crawler_name")
    processed_crawler_name = config.get("glue_processed_crawler_name")
    insights_crawler_name = config.get("glue_insights_crawler_name")
    
    # Validate crawler names exist
    crawlers_to_run = []
    if args.crawler_type == "all":
        if raw_crawler_name:
            crawlers_to_run.append(("raw", raw_crawler_name))
        if processed_crawler_name:
            crawlers_to_run.append(("processed", processed_crawler_name))
        if insights_crawler_name:
            crawlers_to_run.append(("insights", insights_crawler_name))
    else:
        crawler_name = config.get(f"glue_{args.crawler_type}_crawler_name")
        if not crawler_name:
            print(f"Error: Could not get {args.crawler_type} crawler name from SSM Parameter Store.", file=sys.stderr)
            print(f"  Expected config key: glue_{args.crawler_type}_crawler_name", file=sys.stderr)
            sys.exit(1)
        crawlers_to_run.append((args.crawler_type, crawler_name))
    
    if not crawlers_to_run:
        print("Error: No crawlers found to execute.", file=sys.stderr)
        sys.exit(1)
    
    # Initialize Glue client
    glue_client = boto3.client("glue")
    
    print("=" * 60)
    print("Step 8.1: Execute Data Crawlers")
    print("=" * 60)
    print()
    
    success = True
    
    # Execute crawlers
    for idx, (crawler_type, crawler_name) in enumerate(crawlers_to_run, 1):
        if len(crawlers_to_run) > 1:
            print("=" * 60)
            print(f"Executing {crawler_type.upper()} Data Crawler ({idx}/{len(crawlers_to_run)})")
            print("=" * 60)
            print()
        else:
            print("=" * 60)
            print(f"Executing {crawler_type.upper()} Data Crawler")
            print("=" * 60)
            print()
        
        if not execute_crawler(glue_client, crawler_name, crawler_type, args.wait):
            success = False
        
        # Add separator between crawlers if running multiple
        if idx < len(crawlers_to_run) and args.wait:
            print()
            print("=" * 60)
            print()
        
        if not args.wait and idx < len(crawlers_to_run):
            print()
            print("Note: Crawler is running in the background.")
            print()
    
    if success:
        print("=" * 60)
        if len(crawlers_to_run) > 1:
            print(f"✓ All {len(crawlers_to_run)} crawler(s) started successfully")
        else:
            print(f"✓ Crawler started successfully")
        print("=" * 60)
        if not args.wait:
            print()
            print("Next steps:")
            print("  1. Wait 2-5 minutes for crawler(s) to complete")
            print("  2. Run: python3 scripts/run_glue_crawlers/2_verify_tables.py")
    else:
        print("=" * 60)
        print("✗ Some crawlers failed to start")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()

