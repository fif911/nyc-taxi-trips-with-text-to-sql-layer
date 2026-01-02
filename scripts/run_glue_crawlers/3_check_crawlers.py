import boto3
import json

glue = boto3.client('glue')

try:
    response = glue.get_crawler(Name='nyc-taxi-analytics-dev-processed-crawler')
    crawler = response['Crawler']
    
    print("=" * 60)
    print("PROCESSED CRAWLER STATUS")
    print("=" * 60)
    print(f"State: {crawler['State']}")
    
    if 'LastCrawl' in crawler:
        last_crawl = crawler['LastCrawl']
        print(f"\nLast Crawl Details:")
        print(f"  Status: {last_crawl.get('Status', 'N/A')}")
        print(f"  Error Message: {last_crawl.get('ErrorMessage', 'None')}")
        print(f"  Start Time: {last_crawl.get('StartTime', 'N/A')}")
        print(f"  End Time: {last_crawl.get('EndTime', 'N/A')}")
        print(f"  Log Group: {last_crawl.get('LogGroup', 'N/A')}")
        print(f"  Log Stream: {last_crawl.get('LogStream', 'N/A')}")
        print(f"  Tables Created: {last_crawl.get('TablesCreated', 0)}")
        print(f"  Tables Updated: {last_crawl.get('TablesUpdated', 0)}")
        
    print(f"\nCrawler Configuration:")
    print(f"  Database: {crawler.get('DatabaseName', 'N/A')}")
    
    if 'Targets' in crawler and 'S3Targets' in crawler['Targets']:
        print(f"  S3 Targets:")
        for target in crawler['Targets']['S3Targets']:
            print(f"    - Path: {target.get('Path', 'N/A')}")
            
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()