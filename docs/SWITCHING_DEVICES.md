# Switching Devices Guide

## Quick Setup on New Device

### 1. Clone the Repository
```bash
git clone https://github.com/Yuvraj2006py/nba-prediction-model.git
cd nba-prediction-model
```

### 2. Verify Files Are Present
```bash
# Check database exists
ls -lh data/nba_predictions.db

# Check models exist
ls -lh data/models/*.pkl
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Verify Everything Works
```bash
# Test prediction service
python scripts/predict_games.py
```

## What's Synced via Git

✅ **Database**: `data/nba_predictions.db` (~64MB)
- All game data, features, predictions
- Complete historical data

✅ **Models**: `data/models/*.pkl` and `data/models/*.json`
- All trained models (nba_v2_classifier, nba_v2_regressor, etc.)
- Model metadata and feature names

✅ **Code**: All source code, scripts, and configuration

## Daily Workflow (Same on Both Devices)

### Morning (Before Games)
```bash
# Fetch today's games
python scripts/fetch_today_games.py

# Make predictions
python scripts/predict_games.py
```

### Evening (After Games Finish)
```bash
# Update scores (if not automatic)
python scripts/fetch_today_games.py

# Evaluate predictions
python scripts/evaluate_model.py --model-name nba_v2_classifier --start-date 2026-01-02 --end-date 2026-01-02
```

### Sync Changes
```bash
# On device 1: Commit and push
git add .
git commit -m "Updated predictions and scores"
git push

# On device 2: Pull latest
git pull
```

## Important Notes

1. **Database Size**: The database is ~64MB. GitHub accepted it, but if you get warnings, consider Git LFS.

2. **Conflicts**: If you make changes on both devices:
   - Database conflicts are rare (SQLite handles this well)
   - Pull before pushing to avoid conflicts
   - If conflicts occur, resolve manually or use the latest version

3. **First Pull**: The first `git pull` on a new device will download ~100MB+ of data (database + models). This is normal.

4. **Automation**: Set up the same automation scripts on both devices using Windows Task Scheduler or cron.

## Troubleshooting

**Problem**: Database file not found after clone
- **Solution**: Check `.gitignore` - make sure `!data/nba_predictions.db` is present

**Problem**: Models not loading
- **Solution**: Verify `data/models/` contains `.pkl` and `.json` files

**Problem**: Large file warnings
- **Solution**: This is normal. GitHub accepts files up to 100MB. For better performance, consider Git LFS later.

