#!/usr/bin/env python3
"""
Test script to verify Vanna AI endpoint is working correctly.
Connects to the API and runs a sample query.

Usage:
    python test_query.py [API_URL] [QUESTION]
    
    API_URL: Base URL of the Vanna AI API (default: http://localhost:8000)
    QUESTION: Question to test (default: "What payment methods are used most frequently?")
    
Examples:
    python test_query.py                                    # Test localhost
    python test_query.py http://100.52.54.194:8000         # Test EC2 instance
    python test_query.py http://localhost:8000 "Show me trip volume by hour"  # Custom question
"""
import requests
import json
import sys
import os
import argparse
from typing import Dict, Any

DEFAULT_API_URL = "http://localhost:8000"
DEFAULT_QUESTION = "What payment methods are used most frequently?"

def test_health(api_url: str) -> bool:
    """Test health endpoint"""
    try:
        response = requests.get(f"{api_url}/health", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Health check passed: {data}")
            return True
        else:
            print(f"❌ Health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return False

def test_query(api_url: str, question: str) -> Dict[str, Any]:
    """Test a query via the chat endpoint"""
    try:
        # Use the chat_poll endpoint (simpler than SSE)
        endpoint = f"{api_url}/api/vanna/v2/chat_poll"
        
        payload = {
            "message": question,
            "user_id": "test_user"
        }
        
        print(f"\n🔄 Testing query: {question}")
        print(f"   Using endpoint: {endpoint}")
        
        response = requests.post(endpoint, json=payload, timeout=120)
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Query succeeded")
            
            # Parse chunks to extract useful information
            if 'chunks' in result:
                chunks = result['chunks']
                print(f"   Received {len(chunks)} response chunks")
                
                # Look for dataframe chunks (query results)
                for chunk in chunks:
                    if 'rich' in chunk and chunk['rich']:
                        rich = chunk['rich']
                        if rich.get('type') == 'dataframe' and 'data' in rich.get('data', {}):
                            df_data = rich['data']
                            if df_data.get('row_count', 0) > 0:
                                print(f"   Results: {df_data['row_count']} rows, {df_data['column_count']} columns")
                                if df_data.get('data'):
                                    print(f"   Sample data: {df_data['data'][:2]}")
                        elif rich.get('type') == 'text' and 'data' in rich.get('data', {}):
                            text_content = rich['data'].get('content', '')[:200]
                            if text_content:
                                print(f"   Response: {text_content}...")
            
            return result
        else:
            print(f"❌ Query failed: {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return {}
            
    except Exception as e:
        print(f"❌ Query error: {e}")
        import traceback
        traceback.print_exc()
        return {}

def main():
    """Run all tests"""
    parser = argparse.ArgumentParser(
        description="Test Vanna AI API endpoint",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_query.py                                    # Test localhost
  python test_query.py http://100.52.54.194:8000         # Test EC2 instance
  python test_query.py http://localhost:8000 "Show me trip volume by hour"  # Custom question
        """
    )
    parser.add_argument(
        "api_url",
        nargs="?",
        default=os.getenv("VANNA_API_URL", DEFAULT_API_URL),
        help=f"Base URL of the Vanna AI API (default: {DEFAULT_API_URL} or VANNA_API_URL env var)"
    )
    parser.add_argument(
        "question",
        nargs="?",
        default=DEFAULT_QUESTION,
        help=f"Question to test (default: '{DEFAULT_QUESTION}')"
    )
    
    args = parser.parse_args()
    api_url = args.api_url.rstrip('/')
    
    print("=" * 60)
    print("Vanna AI API Test")
    print("=" * 60)
    print(f"API URL: {api_url}")
    print()
    
    # Test 1: Health check
    if not test_health(api_url):
        print("\n❌ Health check failed, aborting tests")
        sys.exit(1)
    
    # Test 2: Query
    result = test_query(api_url, args.question)
    
    # Check if we got a successful response with chunks
    if result and ('chunks' in result and len(result.get('chunks', [])) > 0):
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Tests failed - no response chunks received")
        sys.exit(1)

if __name__ == "__main__":
    main()
