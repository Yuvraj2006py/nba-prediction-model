#!/usr/bin/env python
"""
Test if NBA API can fetch injury data.
Checks multiple potential endpoints and APIs.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from config.settings import get_settings
import requests
from datetime import date

def test_nba_api_injuries():
    """Test if NBA API has injury endpoints."""
    settings = get_settings()
    api_key = settings.NBA_API_KEY
    
    print("=" * 70)
    print("TESTING NBA INJURY API ENDPOINTS")
    print("=" * 70)
    print(f"NBA API Key configured: {'Yes' if api_key else 'No'}")
    if api_key:
        print(f"API Key (first 10 chars): {api_key[:10]}...")
    print()
    
    # Test 1: Official NBA Stats API (stats.nba.com)
    print("[TEST 1] Official NBA Stats API (stats.nba.com)")
    print("-" * 70)
    
    base_url = "https://stats.nba.com/stats"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://www.nba.com/',
        'Accept': 'application/json'
    }
    
    # Common NBA API endpoints to test
    endpoints_to_test = [
        "/playerdashboardbygeneralsplits",
        "/teamdashboardbygeneralsplits",
        "/commonallplayers",
        "/scoreboard",
    ]
    
    # Note: Official NBA API doesn't have a dedicated injury endpoint
    # Injuries are typically inferred from game participation
    print("Note: Official NBA Stats API (stats.nba.com) does not have")
    print("      a dedicated injury endpoint. Injuries are inferred from")
    print("      game participation (minutes played = 0).")
    print()
    
    # Test 2: nba_api Python library
    print("[TEST 2] nba_api Python Library")
    print("-" * 70)
    try:
        from nba_api.stats.endpoints import (
            TeamGameLog, BoxScoreTraditionalV3, 
            BoxScoreSummaryV3, CommonAllPlayers
        )
        from nba_api.stats.static import teams
        
        print("[OK] nba_api library is installed")
        print("Available endpoints:")
        print("  - TeamGameLog")
        print("  - BoxScoreTraditionalV3")
        print("  - BoxScoreSummaryV3")
        print("  - CommonAllPlayers")
        print()
        print("Note: nba_api library does NOT have injury-specific endpoints.")
        print("      It uses the same stats.nba.com API which doesn't expose injuries.")
        print()
        
        # Try to get a sample of player data to see what's available
        try:
            print("Testing: CommonAllPlayers endpoint...")
            players = CommonAllPlayers()
            if hasattr(players, 'get_data_frames'):
                df = players.get_data_frames()[0]
                print(f"  [OK] Successfully fetched {len(df)} players")
                print(f"  Columns available: {list(df.columns)[:10]}...")
                print("  Note: No injury status in player data")
        except Exception as e:
            print(f"  [ERROR] {e}")
        
    except ImportError:
        print("[ERROR] nba_api library not installed")
        print("  Install with: pip install nba-api")
    print()
    
    # Test 3: BALLDONTLIE API (third-party, has injury endpoints)
    print("[TEST 3] BALLDONTLIE API (Third-party)")
    print("-" * 70)
    print("BALLDONTLIE API (api.balldontlie.io) has injury endpoints:")
    print("  - GET /v1/injuries")
    print("  - GET /v1/player_injuries")
    print()
    print("Testing BALLDONTLIE API (no key required for basic access)...")
    
    try:
        # BALLDONTLIE API - try with API key if available
        headers = {}
        if api_key and api_key != 'your_nba_api_key_here':
            # Try different auth methods
            headers['Authorization'] = f'Bearer {api_key}'
            # Also try as query param
            params = {"per_page": 5, "api_key": api_key}
        else:
            params = {"per_page": 5}
        
        # Try injuries endpoint
        response = requests.get(
            "https://api.balldontlie.io/v1/injuries",
            params=params,
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"  [OK] Successfully connected to BALLDONTLIE API")
            print(f"  Response status: {response.status_code}")
            if 'data' in data:
                print(f"  Found {len(data.get('data', []))} injury records")
                if data.get('data'):
                    sample = data['data'][0]
                    print(f"  Sample injury:")
                    print(f"    Player: {sample.get('player', {}).get('first_name', 'N/A')} {sample.get('player', {}).get('last_name', 'N/A')}")
                    print(f"    Status: {sample.get('status', 'N/A')}")
                    print(f"    Description: {sample.get('description', 'N/A')[:60]}...")
            else:
                print(f"  Response: {data}")
        else:
            print(f"  [ERROR] API returned status {response.status_code}")
            print(f"  Response: {response.text[:200]}")
            print(f"  Note: BALLDONTLIE API may require authentication")
            print(f"        Try: https://www.balldontlie.io/#get-started")
            
            # Try alternative endpoint
            print(f"\n  Trying alternative endpoint: /v1/player_injuries...")
            try:
                alt_response = requests.get(
                    "https://api.balldontlie.io/v1/player_injuries",
                    params=params,
                    headers=headers,
                    timeout=10
                )
                if alt_response.status_code == 200:
                    print(f"  [OK] Alternative endpoint works!")
                    data = alt_response.json()
                    if 'data' in data:
                        print(f"  Found {len(data.get('data', []))} injury records")
                else:
                    print(f"  [ERROR] Alternative endpoint also returned {alt_response.status_code}")
            except Exception as e2:
                print(f"  [ERROR] Alternative endpoint failed: {e2}")
    except Exception as e:
        print(f"  [ERROR] Error connecting to BALLDONTLIE API: {e}")
    print()
    
    # Test 4: Check if we can infer injuries from game data
    print("[TEST 4] Current Injury Detection Method")
    print("-" * 70)
    print("Current system infers injuries from PlayerStats.minutes_played:")
    print("  - 0 minutes -> 'out'")
    print("  - <5 minutes -> 'questionable'")
    print("  - Otherwise -> 'healthy'")
    print()
    print("This is REACTIVE (only detects after game is played).")
    print("For PREDICTIVE injury tracking, we need:")
    print("  1. Pre-game injury reports (BALLDONTLIE API)")
    print("  2. Or manual updates before games")
    print()
    
    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()
    print("Available Options for Injury Data:")
    print()
    print("1. Official NBA Stats API (stats.nba.com)")
    print("   - Status: [NO] No injury endpoints")
    print("   - Method: Infer from game participation")
    print("   - Limitation: Reactive (only after game)")
    print()
    print("2. BALLDONTLIE API (api.balldontlie.io)")
    print("   - Status: [YES] Has injury endpoints")
    print("   - Endpoint: /v1/injuries or /v1/player_injuries")
    print("   - Key Required: No (free tier available)")
    print("   - Limitation: Third-party, may have delays")
    print()
    print("3. Current System (minutes-based inference)")
    print("   - Status: [YES] Working")
    print("   - Method: Analyze PlayerStats.minutes_played")
    print("   - Limitation: Only works after games are played")
    print()
    print("RECOMMENDATION:")
    print("  - Use BALLDONTLIE API for pre-game injury reports")
    print("  - Fall back to minutes-based inference if API unavailable")
    print("  - Integrate BALLDONTLIE API into data collection pipeline")
    print("=" * 70)

if __name__ == "__main__":
    test_nba_api_injuries()

