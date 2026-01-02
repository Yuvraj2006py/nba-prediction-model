# Feature Mismatch Analysis

## Problem
The model `nba_test_fixed_classification` expects **133 features**, but `TeamRollingFeatures` only provides **78 features** (39 per team × 2).

## Root Cause
The model was trained with the **old `FeatureAggregator` system**, which includes:
1. **Team features** (home + away): ~48 features
2. **Matchup features**: ~13 features (h2h records, style differentials)
3. **Contextual features**: ~3 features (conference, division, playoffs)
4. **Injury features**: ~3 features (players out/questionable, injury severity)
5. **Other features**: ~1 feature (home advantage)

**Total: ~63 features** (without betting features)

However, the model was actually trained with **133 features**, which suggests it may have been trained with:
- Additional derived features
- Betting features (if included)
- Or a different version of the feature set

## Current State

### TeamRollingFeatures (New System)
- **78 features total** (39 per team × 2)
- Only includes team-level rolling averages
- Missing:
  - Matchup features (h2h records, style differentials)
  - Injury features (players out/questionable)
  - Some contextual features

### FeatureAggregator (Old System)
- **63 features** (without betting features)
- Includes team, matchup, contextual, and injury features
- Missing: ~70 features to reach 133

## Solution Options

### Option 1: Add Missing Features to TeamRollingFeatures (Recommended)
Add the missing features to the `TeamRollingFeatures` table:
- Matchup features (h2h records, style differentials)
- Injury features (players out/questionable, injury severity)
- Additional contextual features

**Pros:**
- Fast inference (pre-computed)
- Consistent with training data
- No code changes needed

**Cons:**
- Requires database schema changes
- Need to update `transform_features.py` to calculate these

### Option 2: Hybrid Approach
Use `TeamRollingFeatures` for team features, then add matchup/injury features on-the-fly in `PredictionService`.

**Pros:**
- No schema changes
- Flexible

**Cons:**
- Slower inference
- More complex code

### Option 3: Retrain Model with 78 Features
Retrain the model using only the 78 features from `TeamRollingFeatures`.

**Pros:**
- Simple
- Matches current feature set

**Cons:**
- May lose predictive power
- Need to retrain

## Recommended Next Steps

1. **Identify all 133 features** used during training
2. **Add missing features** to `TeamRollingFeatures` table
3. **Update `transform_features.py`** to calculate matchup and injury features
4. **Regenerate features** for all games
5. **Verify feature count** matches model expectations

## Missing Features Breakdown

Based on training summary top features:
- `ts_differential` (matchup)
- `efg_differential` (matchup)
- `h2h_home_avg_score` (matchup)
- `h2h_away_avg_score` (matchup)
- `home_players_questionable` (injury)
- `home_injury_severity_score` (injury)
- `away_injury_severity_score` (injury)
- Plus ~48 more features to reach 133


