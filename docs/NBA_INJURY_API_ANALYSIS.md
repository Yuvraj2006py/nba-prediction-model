# NBA Injury API Analysis

## Executive Summary

**Current Status**: The NBA API key in your `.env` file is set to a placeholder value (`your_nba_api_key_here`), and **the official NBA Stats API does not provide injury endpoints**. However, there are alternative APIs that can provide injury data.

## Test Results

### Test Date
January 3, 2026

### API Key Status
- **NBA_API_KEY**: Configured but set to placeholder value (`your_nba_api_key_here`)
- **Location**: `.env` file (lines 9-10)
- **Status**: ⚠️ Not a valid API key

## Available APIs for Injury Data

### 1. Official NBA Stats API (stats.nba.com)

**Status**: ❌ **No Injury Endpoints Available**

**Details:**
- The official NBA Stats API (`stats.nba.com`) does **not** have dedicated injury endpoints
- The `nba_api` Python library (which wraps stats.nba.com) also does **not** provide injury data
- Injuries can only be **inferred** from game participation:
  - Player with `0 minutes` → Likely injured/out
  - Player with `<5 minutes` → Possibly injured/questionable
  - This is **reactive** (only works after games are played)

**Endpoints Tested:**
- `CommonAllPlayers` - ✅ Works, but no injury status
- `TeamGameLog` - ✅ Works, but no injury status
- `BoxScoreTraditionalV3` - ✅ Works, but no injury status

**Limitation**: Cannot get pre-game injury reports

### 2. RapidAPI NBA Injuries Reports (nba-injuries-reports.p.rapidapi.com)

**Status**: ✅ **WORKING - Has Injury Endpoints** (Tested Successfully)

**Details:**
- Third-party API via RapidAPI platform
- Provides daily NBA injury reports
- **Currently working with provided API key**

**Test Results:**
- Endpoint exists: ✅
- Authentication: ✅ (RapidAPI key required)
- Status: ✅ **Successfully tested on 2026-01-04**
- Response: ✅ Returns JSON array of injury records

**API Endpoint:**
```
GET /injuries/nba/{date}
Host: nba-injuries-reports.p.rapidapi.com
Headers:
  x-rapidapi-key: {your_api_key}
  x-rapidapi-host: nba-injuries-reports.p.rapidapi.com
```

**Response Format:**
```json
[
  {
    "date": "2026-01-04",
    "team": "Detroit Pistons",
    "player": "Jalen Duren",
    "status": "Out",
    "reason": "Injury/Illness - Right Ankle; Sprain",
    "reportTime": "02PM"
  },
  {
    "date": "2026-01-04",
    "team": "Detroit Pistons",
    "player": "Tobias Harris",
    "status": "Out",
    "reason": "Injury/Illness - Left Hip; Sprain",
    "reportTime": "02PM"
  }
]
```

**Fields:**
- `date`: Date of injury report (YYYY-MM-DD)
- `team`: Team name (e.g., "Detroit Pistons")
- `player`: Player full name
- `status`: Injury status (e.g., "Out", "Questionable", "Probable")
- `reason`: Detailed injury description
- `reportTime`: Time of injury report

**Rate Limits:**
- Basic plan has daily quota limits
- Test showed 429 (Too Many Requests) after first successful call
- May need to upgrade plan for production use

**How to Use:**
1. API key already provided: `49bb49d912msh1622d0ab103a2ccp1482b4jsnd6167f842e4d`
2. Add to `.env` as `RAPIDAPI_NBA_INJURIES_KEY`
3. Use endpoint: `/injuries/nba/{date}` where date is YYYY-MM-DD format

**API Documentation**: https://rapidapi.com/nichustm/api/nba-injuries-reports

**Test Script**: `scripts/test_rapidapi_injuries.py` (already created and tested)

### 3. BALLDONTLIE API (api.balldontlie.io)

**Status**: ✅ **Has Injury Endpoints** (Requires Authentication)

**Details:**
- Third-party API that provides NBA injury data
- Has dedicated injury endpoints:
  - `GET /v1/injuries` - List of current injuries
  - `GET /v1/player_injuries` - Player-specific injury data

**Test Results:**
- Endpoint exists: ✅
- Authentication required: ✅ (Returned 401 Unauthorized)
- Free tier available: ✅ (According to documentation)

**How to Get Access:**
1. Visit: https://www.balldontlie.io/#get-started
2. Sign up for free API access
3. Get API key
4. Add to `.env` as `BALLDONTLIE_API_KEY`

**API Documentation**: https://docs.balldontlie.io/

**Example Response:**
```json
{
  "data": [
    {
      "player": {
        "id": 12345,
        "first_name": "John",
        "last_name": "Doe",
        "position": "G"
      },
      "status": "Out",
      "description": "Doe (knee) is listed as out for Sunday's game...",
      "return_date": "Jan 10"
    }
  ]
}
```

### 3. Current System (Minutes-Based Inference)

**Status**: ✅ **Working** (Reactive Only)

**How It Works:**
- Analyzes `PlayerStats.minutes_played` from completed games
- Infers injury status:
  - `0 minutes` → `'out'`
  - `<5 minutes` → `'questionable'`
  - Otherwise → `'healthy'`

**Limitation**: 
- Only works **after** games are played
- Cannot predict injuries before games
- May miss players who are "out" but not in the game roster

## Recommendations

### Immediate Actions

1. **✅ USE RAPIDAPI NBA INJURIES** (Recommended - Already Working!)
   - API key already available: `49bb49d912msh1622d0ab103a2ccp1482b4jsnd6167f842e4d`
   - Add to `.env` as `RAPIDAPI_NBA_INJURIES_KEY`
   - Test script already created: `scripts/test_rapidapi_injuries.py`
   - **This is the fastest path to working injury tracking**

2. **Keep Current System as Fallback**
   - Continue using minutes-based inference
   - Use it when API is unavailable or for historical data

3. **Monitor Rate Limits**
   - RapidAPI Basic plan has daily quotas
   - May need to upgrade plan for production use
   - Consider caching injury data to reduce API calls

4. **Alternative: BALLDONTLIE API** (If RapidAPI quota is insufficient)
   - Sign up at https://www.balldontlie.io/#get-started
   - Add `BALLDONTLIE_API_KEY` to `.env` file
   - Can be used as backup or primary if RapidAPI limits are hit

### Implementation Plan

#### Phase 1: Integrate RapidAPI NBA Injuries (PRIORITY)

**Create**: `src/data_collectors/rapidapi_injury_collector.py`

```python
import http.client
import json
from datetime import date
from typing import List, Dict, Optional

class RapidAPIInjuryCollector:
    """Collects injury data from RapidAPI NBA Injuries Reports."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("RAPIDAPI_NBA_INJURIES_KEY")
        self.host = "nba-injuries-reports.p.rapidapi.com"
        self.headers = {
            'x-rapidapi-key': self.api_key,
            'x-rapidapi-host': self.host
        }
    
    def get_injuries_for_date(self, injury_date: date) -> List[Dict]:
        """Get injury list for a specific date."""
        conn = http.client.HTTPSConnection(self.host)
        endpoint = f"/injuries/nba/{injury_date.strftime('%Y-%m-%d')}"
        
        try:
            conn.request("GET", endpoint, headers=self.headers)
            res = conn.getresponse()
            
            if res.status == 200:
                data = res.read()
                return json.loads(data.decode("utf-8"))
            else:
                raise Exception(f"API returned status {res.status}")
        finally:
            conn.close()
    
    def get_today_injuries(self) -> List[Dict]:
        """Get today's injury list."""
        return self.get_injuries_for_date(date.today())
    
    def update_player_injury_status(self, game_date: date):
        """Update PlayerStats.injury_status before games."""
        # Fetch injuries for the game date
        injuries = self.get_injuries_for_date(game_date)
        
        # Map to database and update PlayerStats
        # Implementation details in Phase 2
```

#### Phase 1b: Integrate BALLDONTLIE API (Alternative/Backup)

**Create**: `src/data_collectors/injury_collector.py`

```python
class InjuryCollector:
    """Collects injury data from BALLDONTLIE API."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("BALLDONTLIE_API_KEY")
        self.base_url = "https://api.balldontlie.io/v1"
    
    def get_current_injuries(self) -> List[Dict]:
        """Get current injury list."""
        # Fetch from /v1/injuries endpoint
    
    def get_player_injury(self, player_id: str) -> Optional[Dict]:
        """Get injury status for specific player."""
        # Fetch from /v1/player_injuries endpoint
    
    def update_player_injury_status(self, game_date: date):
        """Update PlayerStats.injury_status before games."""
        # Fetch injuries for upcoming games
        # Update database with pre-game injury status
```

#### Phase 2: Update Data Collection Pipeline

**Modify**: `scripts/daily_workflow.py` or create new script

```python
# Before making predictions:
1. Fetch injury reports from BALLDONTLIE API
2. Update PlayerStats.injury_status for upcoming games
3. Then generate features (which will use updated injury status)
4. Make predictions
```

#### Phase 3: Enhanced Feature Calculation

**Update**: `src/features/team_features.py`

- Use pre-game injury status (from API) instead of inferring from minutes
- Calculate weighted injury impact using player importance
- Add historical injury impact features

## Alternative APIs (If BALLDONTLIE Doesn't Work)

### 1. ClearSports API
- URL: https://www.clearsportsapi.com/
- Provides NBA injury data
- Requires subscription

### 2. Fantasy Nerds API
- URL: https://api.fantasynerds.com/docs/nba
- Provides injury data
- Requires API key

### 3. Web Scraping (Last Resort)
- Scrape team injury reports from official NBA team websites
- Scrape from ESPN, The Score, or other sports news sites
- More fragile but free

## Current Workaround

Until you have a working injury API:

1. **Manual Updates**: Manually update `PlayerStats.injury_status` before games
2. **Minutes-Based Detection**: Continue using current system for completed games
3. **News Monitoring**: Monitor NBA injury reports and update database manually

## Next Steps

1. ✅ **Test completed** - Know what APIs are available
2. ⏳ **Get BALLDONTLIE API key** - Sign up and add to `.env`
3. ⏳ **Implement InjuryCollector** - Create collector class
4. ⏳ **Integrate into pipeline** - Update daily workflow
5. ⏳ **Test end-to-end** - Verify injury data flows correctly

## Code to Test BALLDONTLIE API

Once you have an API key, test it with:

```python
import requests

api_key = "your_balldontlie_api_key"
headers = {"Authorization": f"Bearer {api_key}"}

response = requests.get(
    "https://api.balldontlie.io/v1/injuries",
    headers=headers,
    params={"per_page": 10}
)

if response.status_code == 200:
    injuries = response.json()
    print(f"Found {len(injuries['data'])} current injuries")
    for injury in injuries['data']:
        print(f"{injury['player']['first_name']} {injury['player']['last_name']}: {injury['status']}")
```

## Conclusion

**Your current NBA_API_KEY cannot fetch injuries** because:
1. It's set to a placeholder value
2. The official NBA Stats API doesn't have injury endpoints anyway

**✅ SOLUTION FOUND: RapidAPI NBA Injuries Reports**
- **Status**: Working and tested successfully
- **API Key**: Already available (`49bb49d912msh1622d0ab103a2ccp1482b4jsnd6167f842e4d`)
- **Next Steps**: 
  1. Add `RAPIDAPI_NBA_INJURIES_KEY` to `.env` file
  2. Implement `RapidAPIInjuryCollector` class
  3. Integrate into daily workflow before predictions
  4. Monitor rate limits and upgrade plan if needed

**Alternative Solutions**:
- **BALLDONTLIE API**: Can be used as backup or if RapidAPI quota is insufficient
- **Current System**: Keep minutes-based inference as fallback for historical data

**Implementation**: 
- Follow `ENHANCED_INJURY_TRACKING.md` for full feature design
- Use RapidAPI for pre-game injury reports
- Test scripts available:
  - `scripts/test_rapidapi_injuries.py` - Test RapidAPI endpoint
  - `scripts/test_injury_api.py` - Test all available APIs

