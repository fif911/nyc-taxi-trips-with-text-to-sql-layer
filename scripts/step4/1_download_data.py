#!/usr/bin/env python3
"""
Download NYC Yellow Taxi Parquet files from public source.

Usage:
    python3 1_download_data.py [OPTIONS] [MONTHS...]
    python3 scripts/step4/1_download_data.py [OPTIONS] [MONTHS...]

Options:
    -y, --year YEAR      Year to download (default: 2025)
    -h, --help           Show this help message

Examples:
    # From scripts/step4/ directory:
    python3 1_download_data.py                     # Download months 01-10 (default)
    python3 1_download_data.py 01 02              # Download January and February
    python3 1_download_data.py 01-03               # Download January through March
    python3 1_download_data.py --year 2024 01-12   # Download all months of 2024
    
    # From project root:
    python3 scripts/step4/1_download_data.py 01 02
    python3 scripts/step4/1_download_data.py --year 2024 01-12
"""
import os
import sys
import argparse
import urllib.request
from pathlib import Path

# Get project root (3 levels up from scripts/step4/)
project_root = Path(__file__).parent.parent.parent


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


def download_file(url: str, local_path: Path) -> bool:
    """Download a file from URL to local path."""
    try:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        with urllib.request.urlopen(url, timeout=60) as response:
            with open(local_path, "wb") as f:
                f.write(response.read())
        
        # Verify file is not empty
        if local_path.stat().st_size == 0:
            print(f"✗ Downloaded file is empty: {local_path.name}")
            return False
        
        return True
    except Exception as e:
        print(f"✗ Failed to download {local_path.name}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Download NYC Yellow Taxi Parquet files for specified months.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From scripts/step4/ directory:
  python3 %(prog)s                     # Download months 01-10 (default)
  python3 %(prog)s 01 02              # Download January and February
  python3 %(prog)s 01-03               # Download January through March
  python3 %(prog)s --year 2024 01-12   # Download all months of 2024
  
  # From project root:
  python3 scripts/step4/%(prog)s 01 02
  python3 scripts/step4/%(prog)s --year 2024 01-12
        """
    )
    parser.add_argument("-y", "--year", default="2025", help="Year to download (default: 2025)")
    parser.add_argument("months", nargs="*", help="Months to download (MM or MM-MM format)")
    
    args = parser.parse_args()
    
    # Default months if none specified
    if not args.months:
        args.months = ["01-10"]
    
    year = args.year
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    # Use project root for data directory
    local_dir = project_root / "data" / "raw"
    
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
    
    print(f"Downloading NYC Yellow Taxi data for year {year}, months: {' '.join(all_months)}")
    print()
    
    failed = False
    for month in all_months:
        filename = f"yellow_tripdata_{year}-{month}.parquet"
        url = f"{base_url}/{filename}"
        local_file = local_dir / filename
        
        print(f"Downloading {filename}...")
        
        if download_file(url, local_file):
            file_size = local_file.stat().st_size / (1024 * 1024)  # Size in MB
            print(f"✓ Downloaded {filename} ({file_size:.1f} MB)")
        else:
            failed = True
    
    print()
    if failed:
        print("Some downloads failed. Please check the errors above.")
        sys.exit(1)
    else:
        print("All files downloaded successfully!")
        print(f"Files saved to: {local_dir}")


if __name__ == "__main__":
    main()
