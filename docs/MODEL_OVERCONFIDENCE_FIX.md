# Model Overconfidence Fix - Implementation Summary

## Problem Identified

The model was showing **99.9% confidence** for games that should be closer (e.g., Washington Wizards vs Minnesota Timberwolves, Bucks vs Kings).

### Root Cause Analysis

**Diagnostic Results:**
- **Washington vs Timberwolves (99.9% confidence for Washington):**
  - `home_win_streak = 1.00` (Washington won 1 game)
  - `away_loss_streak = 1.00` (Minnesota lost 1 game)
  - Washington has terrible overall record (18.75% away win pct, 26.67% home win pct)
  - Minnesota has much better record (60% win pct)
  - **Model overreacted to tiny 1-game streak difference**

- **Bucks vs Kings (95.3% confidence for Kings):**
  - `away_loss_streak = 1.00` (Bucks lost 1 game)
  - Both teams have similar recent performance
  - **Model overreacted to single loss streak**

### Core Issue

The model has **68.3% feature importance** on `home_loss_streak` alone, causing it to:
- Overreact to small streak differences (1-2 games)
- Ignore overall team quality/record
- Produce unrealistic confidence scores (99.9% for close games)

## Solution Implemented

### 1. Updated XGBoost Default Parameters

**File:** `src/models/xgboost_model.py`

**Changes:**
- `max_depth`: 6 → **4** (simpler trees prevent overfitting)
- `learning_rate`: 0.1 → **0.05** (more stable training)
- `min_child_weight`: 1 → **3** (require more samples per leaf)
- `gamma`: 0 → **0.1** (minimum loss reduction for splits)
- `reg_alpha`: 0 → **0.5** (L1 regularization)
- `reg_lambda`: 1 → **2.0** (stronger L2 regularization)

**Impact:**
- Reduces overfitting to streak features
- Better balances feature importance
- Produces more realistic confidence scores

### 2. Created Diagnostic Tools

**File:** `scripts/inspect_prediction.py`

**Usage:**
```bash
# Inspect specific game
python scripts/inspect_prediction.py --game-id 20260104750764

# Or by team names
python scripts/inspect_prediction.py --teams "Wizards @ Timberwolves"
```

**Shows:**
- Prediction probabilities
- Feature values (streaks, win %, etc.)
- Extreme values that may cause overconfidence

### 3. Created Retraining Script

**File:** `scripts/retrain_with_regularization.py`

**Usage:**
```bash
python scripts/retrain_with_regularization.py
```

**What it does:**
- Trains new models (`nba_v3_classifier`, `nba_v3_regressor`) with improved regularization
- Compares feature importance with old model
- Shows if streak feature dominance is reduced

### 4. Made Model Names Configurable

**Files:** `config/settings.py`, `scripts/daily_workflow.py`

**Changes:**
- Added `CLASSIFIER_MODEL_NAME` and `REGRESSOR_MODEL_NAME` to settings
- Updated workflow to use configurable model names
- Can be overridden via environment variables

**To switch models after retraining:**
```bash
# Option 1: Set environment variables
set CLASSIFIER_MODEL_NAME=nba_v3_classifier
set REGRESSOR_MODEL_NAME=nba_v3_regressor

# Option 2: Update .env file
CLASSIFIER_MODEL_NAME=nba_v3_classifier
REGRESSOR_MODEL_NAME=nba_v3_regressor
```

## Next Steps

### Step 1: Retrain the Model

```bash
python scripts/retrain_with_regularization.py
```

This will:
- Train new models with better regularization
- Save as `nba_v3_classifier.pkl` and `nba_v3_regressor.pkl`
- Show feature importance comparison

**Expected Results:**
- Streak features should drop from 68% to <40% importance
- More balanced feature distribution
- More realistic confidence scores

### Step 2: Verify Feature Importance

After retraining, check:
- Streak features total importance should be <40% (was 68%)
- Overall team quality features should have higher importance
- More features contributing to predictions

### Step 3: Test New Model

Before switching, test the new model:

```bash
# Make predictions with new model
python -c "
from src.prediction.prediction_service import PredictionService
from src.database.db_manager import DatabaseManager
from src.database.models import Game
from datetime import date

db = DatabaseManager()
pred_service = PredictionService(db)

with db.get_session() as session:
    games = session.query(Game).filter(Game.game_date == date.today()).all()
    for game in games[:2]:
        result = pred_service.predict_game(
            game.game_id,
            model_name='nba_v3_classifier',  # New model
            reg_model_name='nba_v3_regressor'
        )
        if result:
            print(f'{game.game_id}: {result[\"confidence\"]:.1%} confidence')
"
```

### Step 4: Switch to New Model

**Option 1: Environment Variables (Recommended)**
```bash
# Windows PowerShell
$env:CLASSIFIER_MODEL_NAME="nba_v3_classifier"
$env:REGRESSOR_MODEL_NAME="nba_v3_regressor"

# Or add to .env file
CLASSIFIER_MODEL_NAME=nba_v3_classifier
REGRESSOR_MODEL_NAME=nba_v3_regressor
```

**Option 2: Update settings.py directly**
```python
CLASSIFIER_MODEL_NAME: str = os.getenv("CLASSIFIER_MODEL_NAME", "nba_v3_classifier")
REGRESSOR_MODEL_NAME: str = os.getenv("REGRESSOR_MODEL_NAME", "nba_v3_regressor")
```

## Expected Improvements

After retraining and switching to new model:

1. **More Realistic Confidence:**
   - Close games (50/50) should show 55-65% confidence, not 95%+
   - Strong favorites should still show high confidence (80-90%), but not 99.9%

2. **Better Feature Balance:**
   - Streak features: <40% importance (was 68%)
   - Overall team quality: Higher importance
   - More features contributing to decisions

3. **Better Predictions:**
   - Washington (worst record) won't get 99.9% confidence
   - Close matchups (Bucks vs Kings) will show more realistic probabilities

## Monitoring

After switching to new model, monitor:

1. **Calibration Check:**
   ```bash
   python -c "
   from src.monitoring.prediction_monitor import PredictionMonitor
   from src.database.db_manager import DatabaseManager
   
   monitor = PredictionMonitor(DatabaseManager())
   result = monitor.check_confidence_calibration('nba_v3_classifier', days=30)
   print(result)
   "
   ```

2. **Feature Inspection:**
   ```bash
   python scripts/inspect_prediction.py --game-id <game_id>
   ```

3. **Track Accuracy:**
   - Monitor if accuracy remains good with more realistic confidence
   - Check if predictions are more accurate on close games

## Files Modified

1. `src/models/xgboost_model.py` - Updated default parameters
2. `config/settings.py` - Added model name configuration
3. `scripts/daily_workflow.py` - Uses configurable model names
4. `scripts/inspect_prediction.py` - New diagnostic tool
5. `scripts/retrain_with_regularization.py` - New retraining script
6. `docs/MODEL_OVERCONFIDENCE_FIX.md` - This document

## Summary

The fix addresses model overconfidence by:
- **Stronger regularization** to prevent overfitting to streaks
- **Better parameter tuning** to balance feature importance
- **Diagnostic tools** to identify overconfidence issues
- **Retraining process** to create improved models
- **Configurable model names** for easy switching

The new models will better identify close games and produce more realistic confidence scores while maintaining prediction accuracy.
