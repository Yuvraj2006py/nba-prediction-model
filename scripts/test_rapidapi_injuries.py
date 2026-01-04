#!/usr/bin/env python
"""
Test RapidAPI NBA Injuries endpoint.
"""

import http.client
import json
from datetime import date, timedelta

def test_rapidapi_injuries():
    """Test RapidAPI NBA injuries endpoint."""
    
    print("=" * 70)
    print("TESTING RAPIDAPI NBA INJURIES ENDPOINT")
    print("=" * 70)
    
    # API configuration
    host = "nba-injuries-reports.p.rapidapi.com"
    api_key = "49bb49d912msh1622d0ab103a2ccp1482b4jsnd6167f842e4d"
    
    headers = {
        'x-rapidapi-key': api_key,
        'x-rapidapi-host': host
    }
    
    # Test with today's date
    today = date.today()
    test_dates = [
        today.strftime("%Y-%m-%d"),
        (today - timedelta(days=1)).strftime("%Y-%m-%d"),
        (today - timedelta(days=7)).strftime("%Y-%m-%d"),
    ]
    
    print(f"\nAPI Host: {host}")
    print(f"API Key (first 20 chars): {api_key[:20]}...")
    print(f"\nTesting dates: {', '.join(test_dates)}")
    print()
    
    for test_date in test_dates:
        print(f"[TEST] Date: {test_date}")
        print("-" * 70)
        
        try:
            conn = http.client.HTTPSConnection(host)
            endpoint = f"/injuries/nba/{test_date}"
            
            print(f"Endpoint: {endpoint}")
            conn.request("GET", endpoint, headers=headers)
            
            res = conn.getresponse()
            status = res.status
            
            print(f"Status Code: {status}")
            
            if status == 200:
                data = res.read()
                response_text = data.decode("utf-8")
                
                try:
                    # Try to parse as JSON
                    response_json = json.loads(response_text)
                    print(f"[SUCCESS] Got JSON response")
                    print(f"Response keys: {list(response_json.keys()) if isinstance(response_json, dict) else 'Not a dict'}")
                    
                    # Pretty print first few entries
                    if isinstance(response_json, dict):
                        print(f"\nResponse structure:")
                        for key, value in list(response_json.items())[:5]:
                            if isinstance(value, list) and len(value) > 0:
                                print(f"  {key}: list with {len(value)} items")
                                print(f"    First item keys: {list(value[0].keys()) if isinstance(value[0], dict) else 'Not a dict'}")
                            elif isinstance(value, dict):
                                print(f"  {key}: dict with keys: {list(value.keys())}")
                            else:
                                print(f"  {key}: {type(value).__name__}")
                    
                    # If it's a list of injuries
                    if isinstance(response_json, list) and len(response_json) > 0:
                        print(f"\nFound {len(response_json)} injury records")
                        print(f"\nFirst injury record:")
                        first_injury = response_json[0]
                        for key, value in first_injury.items():
                            print(f"  {key}: {value}")
                    
                    # Save sample response
                    with open(f"data/rapidapi_injury_sample_{test_date}.json", "w") as f:
                        json.dump(response_json, f, indent=2)
                    print(f"\n[SAVED] Sample response saved to: data/rapidapi_injury_sample_{test_date}.json")
                    
                except json.JSONDecodeError:
                    print(f"[WARNING] Response is not valid JSON")
                    print(f"Response (first 500 chars): {response_text[:500]}")
                    
            elif status == 401:
                print(f"[ERROR] Unauthorized - Check API key")
                data = res.read()
                print(f"Response: {data.decode('utf-8')[:200]}")
            elif status == 404:
                print(f"[INFO] No data found for date {test_date}")
            else:
                print(f"[ERROR] Unexpected status code: {status}")
                data = res.read()
                print(f"Response: {data.decode('utf-8')[:200]}")
                
            conn.close()
            
        except Exception as e:
            print(f"[ERROR] Exception: {e}")
            import traceback
            traceback.print_exc()
        
        print()
    
    print("=" * 70)
    print("TESTING COMPLETE")
    print("=" * 70)
    
    # Test what endpoints are available
    print("\n[INFO] Testing other potential endpoints...")
    test_endpoints = [
        "/injuries/nba",
        "/injuries/nba/current",
        "/injuries/nba/latest",
    ]
    
    for endpoint in test_endpoints:
        try:
            conn = http.client.HTTPSConnection(host)
            conn.request("GET", endpoint, headers=headers)
            res = conn.getresponse()
            print(f"  {endpoint}: Status {res.status}")
            conn.close()
        except Exception as e:
            print(f"  {endpoint}: Error - {e}")

if __name__ == "__main__":
    test_rapidapi_injuries()

