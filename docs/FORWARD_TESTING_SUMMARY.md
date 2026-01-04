# Forward Testing System Summary

## Overview

The forward testing system allows you to test predictions on today's games in real-time. This is **not backtesting** - it's forward testing where you:

1. **Before games start**: Make predictions and record betting decisions
2. **After games finish**: Resolve bets and calculate results

## Components

### 1. Team Mapper (`src/backtesting/team_mapper.py`)
- Maps team names from betting API to database team IDs
- Handles various team name formats and variations
- Caches mappings for performance

### 2. Betting Strategies (`src/backtesting/strategies.py`)
Three strategies available:

- **ConfidenceThresholdStrategy**: Bet when confidence exceeds threshold
  - Parameters: `confidence_threshold`, `bet_amount`, `min_confidence`
  
- **ExpectedValueStrategy**: Bet when expected value is positive
  - Parameters: `min_ev`, `bet_fraction`, `max_bet`
  
- **KellyCriterionStrategy**: Use Kelly Criterion for optimal bet sizing
  - Parameters: `kelly_fraction`, `min_confidence`

### 3. Forward Tester (`src/backtesting/forward_tester.py`)
Main orchestrator that:
- Gets today's games
- Makes predictions
- Applies betting strategy
- Records bets
- Resolves bets after games finish

### 4. Scripts

#### `scripts/fetch_today_games.py`
Fetches today's games and odds from betting API.

```bash
python scripts/fetch_today_games.py
```

**What it does:**
1. Fetches odds for today from betting API
2. Creates games in database (if not exist)
3. Stores betting lines
4. Shows summary

#### `scripts/test_today_games.py`
Main testing script for forward testing.

**Setup (before games):**
```bash
python scripts/test_today_games.py \
  --date 2026-01-01 \
  --model nba_classifier \
  --strategy confidence \
  --confidence-threshold 0.60 \
  --bet-amount 100.0
```

**Resolve (after games finish):**
```bash
python scripts/test_today_games.py \
  --resolve \
  --date 2026-01-01
```

**View summary:**
```bash
python scripts/test_today_games.py \
  --summary \
  --date 2026-01-01
```

## Workflow

### Step 1: Fetch Games and Odds
```bash
python scripts/fetch_today_games.py
```

Output:
```
[STEP 1] Fetching odds from API...
[OK] Found 1 games with odds

[STEP 2] Games found:
  1. Houston Rockets @ Brooklyn Nets

[STEP 3] Storing games and odds...
[OK] Stored 26 betting lines
```

### Step 2: Generate Features (if needed)
Features are automatically generated when making predictions, but you can pre-generate:
```bash
python scripts/generate_features.py --season 2025-26
```

### Step 3: Setup Forward Test
```bash
python scripts/test_today_games.py \
  --date 2026-01-01 \
  --model nba_classifier \
  --strategy confidence \
  --confidence-threshold 0.60
```

Output:
```
Games Found: 1
Bets Placed: 1
Current Bankroll: $10,000.00

Bets:
  1. Game: 20260101745751
     Type: moneyline
     Team: 1610612751
     Amount: $100.00
     Odds: 5.80 (decimal)
     Confidence: 87.8%
     Expected Value: 4.092
```

### Step 4: Wait for Games to Finish
Games must finish and scores must be updated in database.

### Step 5: Resolve Bets
```bash
python scripts/test_today_games.py --resolve --date 2026-01-01
```

Output:
```
Bets Resolved: 1
Wins: 1
Losses: 0
Win Rate: 100.0%
Total Profit/Loss: $480.00

[SUCCESS] Profitable day! +$480.00
```

## Strategy Options

### Confidence Strategy
Simple: bet when confidence > threshold
```bash
--strategy confidence \
--confidence-threshold 0.60 \
--bet-amount 100.0
```

### Expected Value Strategy
Bet when EV > minimum threshold
```bash
--strategy ev \
--min-ev 0.05
```

### Kelly Criterion Strategy
Optimal bet sizing using Kelly Criterion
```bash
--strategy kelly \
--kelly-fraction 0.25
```

## Database Tables Used

- **`games`**: Game information
- **`betting_lines`**: Odds data
- **`predictions`**: Model predictions
- **`bets`**: Betting decisions and outcomes

## Notes

1. **Bankroll Management**: Starts at $10,000. Tracks profit/loss across all bets.

2. **Bet Resolution**: Requires games to have `home_score` and `away_score` populated.

3. **Feature Generation**: Features are automatically generated when making predictions if they don't exist.

4. **Model Requirements**: 
   - Classification model (required)
   - Regression model (optional)

5. **API Key**: Must be set in `.env` file:
   ```
   BETTING_API_KEY=your_key_here
   ```

## Example Full Workflow

```bash
# Morning: Fetch games and odds
python scripts/fetch_today_games.py

# Afternoon: Setup bets
python scripts/test_today_games.py \
  --date 2026-01-01 \
  --model nba_classifier \
  --strategy confidence \
  --confidence-threshold 0.60

# Night (after games): Resolve bets
python scripts/test_today_games.py \
  --resolve \
  --date 2026-01-01

# View summary
python scripts/test_today_games.py \
  --summary \
  --date 2026-01-01
```

## Troubleshooting

### No games found
- Check if games exist in database: `SELECT * FROM games WHERE game_date = '2026-01-01'`
- Run `fetch_today_games.py` to fetch from API

### No odds available
- Check if betting lines exist: `SELECT * FROM betting_lines WHERE game_id = '...'`
- Verify API key is correct in `.env`

### No bets placed
- Lower confidence threshold: `--confidence-threshold 0.50`
- Check model predictions: `SELECT * FROM predictions WHERE game_id = '...'`

### Can't resolve bets
- Ensure games have scores: `SELECT * FROM games WHERE game_date = '2026-01-01' AND home_score IS NOT NULL`
- Update scores manually if needed




