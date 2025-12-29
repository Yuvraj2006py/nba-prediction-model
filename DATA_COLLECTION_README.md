# Data Collection Guide - 3 Seasons

## Overview

This guide explains how to collect all 3 seasons of NBA data (2022-23, 2023-24, 2024-25) for model training.

## Current Status

Run this to check current data status:
```bash
python scripts/check_data_status.py
```

## Collection Scripts

### 1. Test Collection (Small Subset)
**File**: `scripts/test_collection_small.py`

Test the collection pipeline on a small subset before full run:
```bash
python scripts/test_collection_small.py
```

This collects:
- 5 games from Lakers 2024-25 season
- Game details, team stats, and player stats
- Verifies data is stored correctly

### 2. Full Historical Collection
**File**: `scripts/collect_historical_data.py`

Collect all 3 seasons of data:
```bash
python scripts/collect_historical_data.py
```

**What it collects:**
- All games for all 30 teams across 3 seasons
- Game details (scores, dates, teams, winners)
- Team stats (for finished games)
- Player stats (for finished games)

**Features:**
- ✅ Resumable: Skips already collected games
- ✅ Progress tracking: Shows progress for each season
- ✅ Error handling: Continues on errors, logs them
- ✅ Duplicate prevention: Only collects each game once
- ✅ Stats checking: Only collects stats if missing

## Expected Collection Time

**Important**: This will take **several hours** to complete due to:
- API rate limiting (1 second delay between requests)
- ~3,690 games to process
- ~30 teams × 3 seasons = 90 team-season combinations
- ~2 API calls per game (details + stats)
- ~7,380+ API calls total

**Estimated time**: 2-4 hours (depending on API response times)

## Collection Process

The script follows this process for each season:

1. **Game Discovery** (Step 1/4)
   - Iterates through all 30 teams
   - Gets games for each team in the season
   - Deduplicates games (each game appears twice - once per team)
   - Skips games already in database

2. **Game Details** (Step 2/4)
   - Gets full game details for each unique game
   - Stores game information (scores, dates, teams, winners)

3. **Team Stats** (Step 3/4)
   - Collects team statistics for finished games
   - Only collects if stats don't already exist

4. **Player Stats** (Step 4/4)
   - Collects player statistics for finished games
   - Only collects if stats don't already exist

## Interrupting and Resuming

The script is designed to be **resumable**:

- If interrupted (Ctrl+C), progress is saved
- Running again will skip already collected games
- Already collected stats are not re-collected

**To resume after interruption:**
```bash
python scripts/collect_historical_data.py
```

## Monitoring Progress

The script logs progress to:
- Console (real-time updates)
- `logs/data_collection.log` (full log file)

**Progress indicators:**
- Every 10 teams: Shows games found so far
- Every 50 games: Shows processing progress
- End of each season: Summary statistics

## Expected Results

After completion, you should have:

- **Games**: ~3,690 games (1,230 per season)
- **Team Stats**: ~7,380 records (2 per game)
- **Player Stats**: ~150,000+ records (varies by game)

## Verification

After collection, verify data:

```bash
# Check data status
python scripts/check_data_status.py

# Verify POC data (if needed)
python scripts/verify_poc_data.py
```

## Troubleshooting

### API Rate Limiting
- The script includes rate limiting (1 second delay)
- If you see rate limit errors, increase `RATE_LIMIT_DELAY` in `.env`

### Missing Stats
- Some games may not have stats if they're scheduled/future games
- Only finished games have team/player stats
- This is normal and expected

### Database Errors
- Ensure database is initialized: `python scripts/init_database.py`
- Check database connection in `.env` file

### Interrupted Collection
- Simply run the script again - it will resume from where it left off
- Already collected games/stats are skipped

## Next Steps

After collection is complete:

1. **Verify Data**: Run `python scripts/check_data_status.py`
2. **Generate Features**: Use FeatureAggregator to create feature vectors
3. **Train Models**: Proceed to Phase 5 (Model Development)

## Notes

- Collection is **idempotent**: Safe to run multiple times
- Only collects **finished games** with stats
- **Scheduled/future games** are stored but won't have stats until they're played
- Collection respects API rate limits automatically

