#!/usr/bin/env python3
"""
Upload NYC Yellow Taxi Parquet files to S3.

Usage:
    python3 2_upload_to_s3.py [OPTIONS] [MONTHS...]
    python3 scripts/step4/2_upload_to_s3.py [OPTIONS] [MONTHS...]

Options:
    -y, --year YEAR      Year to upload (default: 2025)
    -b, --bucket BUCKET  S3 bucket name (overrides SSM Parameter Store)
    -h, --help           Show this help message

Examples:
    # From scripts/step4/ directory:
    python3 2_upload_to_s3.py                     # Upload months 01-10 (default)
    python3 2_upload_to_s3.py 01 02              # Upload January and February
    python3 2_upload_to_s3.py 01-03               # Upload January through March
    python3 2_upload_to_s3.py --year 2024 01-12   # Upload all months of 2024
    python3 2_upload_to_s3.py --bucket my-bucket 01 02  # Upload with custom bucket
    
    # From project root:
    python3 scripts/step4/2_upload_to_s3.py 01 02
    python3 scripts/step4/2_upload_to_s3.py --year 2024 --bucket my-bucket 01-12
"""
import sys
import os
import argparse
from pathlib import Path

# Add project root to path to import config_loader
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import boto3
from pyspark.utils.config_loader import load_and_set_config, get_s3_bucket


def expand_month_range(month_arg: str) -> list:
    """Expand month range (e.g., "01-03" -> ["01", "02", "03"])."""
    if "-" in month_arg:
        parts = month_arg.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid month range format: {month_arg}")
        start = int(parts[0])
        end = int(parts[1])
        if start > end:
            raise ValueError(f"Invalid range {month_arg} (start > end)")
        return [f"{i:02d}" for i in range(start, end + 1)]
    elif month_arg.isdigit() and len(month_arg) == 2:
        return [month_arg]
    else:
        raise ValueError(f"Invalid month format: {month_arg} (expected MM or MM-MM)")


def find_data_file(filename: str, project_root: Path) -> Path:
    """
    Find data file with fallback logic.
    Checks project root first, then current directory.
    """
    # Try project root first (preferred location)
    project_file = project_root / "data" / "raw" / filename
    if project_file.exists():
        return project_file
    
    # Fallback: try current directory
    current_file = Path("data/raw") / filename
    if current_file.exists():
        return current_file
    
    # Fallback: try scripts/step4/data/raw (legacy location)
    script_file = Path(__file__).parent / "data" / "raw" / filename
    if script_file.exists():
        return script_file
    
    # Return project root path (will be used for error message)
    return project_file


def main():
    parser = argparse.ArgumentParser(
        description="Upload NYC Yellow Taxi Parquet files to S3 for specified months.",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("-y", "--year", default="2025", help="Year to upload (default: 2025)")
    parser.add_argument("-b", "--bucket", help="S3 bucket name (overrides SSM Parameter Store)")
    parser.add_argument("months", nargs="*", help="Months to upload (MM or MM-MM format)")
    
    args = parser.parse_args()
    
    # Get bucket name from SSM Parameter Store or argument
    bucket_name = args.bucket
    if not bucket_name:
        try:
            load_and_set_config()  # Load config and set environment variables
            bucket_name = get_s3_bucket()
        except Exception as e:
            print(f"Error: Could not load config from SSM Parameter Store: {e}", file=sys.stderr)
            print("Please provide bucket name with -b option or ensure SSM parameter is configured.", file=sys.stderr)
            sys.exit(1)
    
    if not bucket_name:
        print("Error: Could not get bucket name. Please provide it with -b option.", file=sys.stderr)
        sys.exit(1)
    
    # Default months if none specified
    if not args.months:
        args.months = ["01-10"]
    
    year = args.year
    
    # Expand all month ranges
    all_months = []
    for month_arg in args.months:
        try:
            expanded = expand_month_range(month_arg)
            all_months.extend(expanded)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    
    # Remove duplicates and sort
    all_months = sorted(list(set(all_months)))
    
    print(f"Uploading NYC Yellow Taxi data for year {year}, months: {' '.join(all_months)}")
    print(f"Target bucket: {bucket_name}")
    print()
    
    # Initialize S3 client
    s3_client = boto3.client("s3")
    
    failed = False
    for month in all_months:
        filename = f"yellow_tripdata_{year}-{month}.parquet"
        local_file = find_data_file(filename, project_root)
        s3_key = f"raw/year={year}/month={month}/{filename}"
        
        # Check if local file exists
        if not local_file.exists():
            # Get relative paths for cleaner error messages
            project_file_path = project_root / "data" / "raw" / filename
            try:
                project_file_rel = project_file_path.relative_to(project_root)
            except ValueError:
                project_file_rel = project_file_path
            
            current_file_path = Path("data/raw") / filename
            current_file_rel = current_file_path  # Already relative
            
            script_file_path = Path(__file__).parent / "data" / "raw" / filename
            try:
                script_file_rel = script_file_path.relative_to(project_root)
            except ValueError:
                script_file_rel = script_file_path
            
            print(f"✗ Local file not found: {filename}")
            print(f"  Checked locations (relative to project root):")
            print(f"    - {project_file_rel}")
            print(f"    - {current_file_rel}")
            print(f"    - {script_file_rel}")
            print("  Run 1_download_data.py first to download the file.")
            failed = True
            continue
        
        # Check if file is empty or corrupted
        file_size = local_file.stat().st_size
        if file_size == 0:
            # Show relative path for cleaner error message
            try:
                local_file_rel = local_file.relative_to(project_root)
            except ValueError:
                local_file_rel = local_file
            print(f"✗ File is empty: {local_file_rel}")
            print("  Re-download the file using 1_download_data.py")
            failed = True
            continue
        
        print(f"Uploading {filename} to s3://{bucket_name}/{s3_key}...")
        
        try:
            s3_client.upload_file(str(local_file), bucket_name, s3_key)
            print(f"✓ Uploaded {filename}")
        except Exception as e:
            print(f"✗ Failed to upload {filename}: {e}")
            failed = True
    
    print()
    if failed:
        print("Some uploads failed. Please check the errors above.")
        sys.exit(1)
    else:
        print("All files uploaded successfully!")
        print(f"Files available at: s3://{bucket_name}/raw/year={year}/")


if __name__ == "__main__":
    main()
