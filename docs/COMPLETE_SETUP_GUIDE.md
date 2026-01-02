# Complete Setup Guide - NBA Prediction Model

## ✅ What's Been Fixed

### 1. Data Loader Updated
- **Fixed**: Data loader now uses `TeamRollingFeatures` table
- **Result**: Validation and test sets now load correctly (499 games each)
- **File**: `src/training/data_loader.py`

### 2. ETL Pipeline Created
- **New**: `scripts/etl_pipeline.py` - Automated data extraction
- **New**: `scripts/transform_features.py` - Feature engineering
- **Result**: Complete 2025-26 season data (501 games, 1,002 rolling features)

### 3. Database Schema Enhanced
- **New**: `TeamRollingFeatures` table for model-ready features
- **Features**: 133 features per game (home + away rolling stats)

## 📊 Current Status

| Component | Status | Details |
|-----------|--------|---------|
| **2025-26 Data** | ✅ Complete | 501 games, 1,002 team stats, 10,612 player stats |
| **Rolling Features** | ✅ Generated | 1,002 records (2 per game) |
| **Data Loader** | ✅ Fixed | Now loads from `TeamRollingFeatures` |
| **Model Training** | 🔄 In Progress | Running with hyperparameter tuning |

## 🚀 Next Steps

### Step 1: Wait for Training to Complete

The model is currently training in the background. This will take **10-20 minutes**.

**What's happening:**
- Training on 3,677 games (2022-25 seasons)
- Validating on 499 games (2025-26 season)
- Testing on 499 games (2025-26 season)
- Tuning 30 hyperparameter combinations
- Training both classification and regression models

**Check progress:**
- Look for files in `data/models/`:
  - `nba_2025_26_final_classification_best.pkl`
  - `nba_2025_26_final_regression_best.pkl`
  - `training_summary_*.json`

### Step 2: Verify Training Results

Once training completes, check the results:

```powershell
# Check the training summary
Get-Content data\models\training_summary_*.json | Select-Object -Last 1 | Get-Content
```

**Look for:**
- ✅ Validation accuracy > 50% (realistic for unseen season)
- ✅ Test accuracy similar to validation (no overfitting)
- ✅ Train accuracy not too high (should be 70-85%)

### Step 3: Test Predictions on Today's Games

```powershell
# 1. Fetch today's games
python scripts/fetch_all_today_games.py

# 2. Make predictions
python scripts/make_predictions.py --model-name nba_2025_26_final_classification_best --start-date 2026-01-02 --end-date 2026-01-02

# 3. Test betting strategy
python scripts/test_today_games.py setup --date 2026-01-02 --model nba_2025_26_final_classification_best --strategy confidence
```

### Step 4: Set Up Daily Updates

Create a daily automation script or run manually:

```powershell
# Daily routine (run in the morning)
# 1. Update games and stats
python scripts/etl_pipeline.py --season 2025-26 --stats-only

# 2. Update rolling features
python scripts/transform_features.py --season 2025-26

# 3. Make predictions for today
python scripts/make_predictions.py --model-name nba_2025_26_final_classification_best --start-date $(Get-Date -Format "yyyy-MM-dd") --end-date $(Get-Date -Format "yyyy-MM-dd")
```

## 📈 Expected Performance

### Realistic Expectations

- **Accuracy**: 55-65% (NBA games are hard to predict)
- **Better than random**: Yes (50% baseline)
- **Better than casino**: Depends on odds and strategy

### What Good Performance Looks Like

- ✅ Validation accuracy: 55-65%
- ✅ Test accuracy: Similar to validation (±2%)
- ✅ Train accuracy: 70-85% (some overfitting is normal)
- ✅ Consistent predictions (not all 100% or 0% confidence)

## 🔧 Troubleshooting

### If Training Fails

1. **Check data availability:**
   ```powershell
   python scripts/check_available_data.py
   ```

2. **Verify rolling features exist:**
   ```powershell
   python -c "from src.database.db_manager import DatabaseManager; from src.database.models import TeamRollingFeatures; db = DatabaseManager(); print(f'Rolling features: {db.get_session().__enter__().query(TeamRollingFeatures).count()}')"
   ```

3. **Regenerate features if needed:**
   ```powershell
   python scripts/transform_features.py --season 2025-26 --full-refresh
   ```

### If Predictions Seem Off

1. **Check model was trained on current data:**
   - Look at training summary timestamp
   - Should be recent (today)

2. **Verify features are being generated:**
   - Check logs for feature generation
   - Ensure no errors in feature calculation

3. **Compare with casino odds:**
   - If model predictions differ significantly from odds, investigate
   - May indicate data quality issues

## 📝 Key Files Reference

| File | Purpose |
|------|---------|
| `scripts/etl_pipeline.py` | Extract game data and stats |
| `scripts/transform_features.py` | Generate rolling features |
| `scripts/train_model.py` | Train models |
| `scripts/make_predictions.py` | Make predictions |
| `scripts/test_today_games.py` | Test betting strategies |
| `src/training/data_loader.py` | Load training data (FIXED) |
| `src/database/models.py` | Database schema (includes TeamRollingFeatures) |

## 🎯 Success Criteria

You'll know everything is working when:

1. ✅ Training completes without errors
2. ✅ Validation/test sets have data (not 0 games)
3. ✅ Model accuracy is 55-65% on validation
4. ✅ Predictions are generated for today's games
5. ✅ Betting strategy produces reasonable decisions

## 📞 Quick Commands Reference

```powershell
# Full training with tuning
python scripts/train_model.py --task both --train-seasons 2022-23,2023-24,2024-25 --val-seasons 2025-26 --test-seasons 2025-26 --model-name nba_2025_26_final --tune --n-iter 30 --exclude-betting-features

# Quick training without tuning
python scripts/train_model.py --task classification --train-seasons 2022-23,2023-24,2024-25 --val-seasons 2025-26 --test-seasons 2025-26 --model-name nba_quick --exclude-betting-features

# Update data
python scripts/etl_pipeline.py --season 2025-26 --stats-only
python scripts/transform_features.py --season 2025-26

# Make predictions
python scripts/make_predictions.py --model-name nba_2025_26_final_classification_best --start-date 2026-01-02 --end-date 2026-01-02
```


