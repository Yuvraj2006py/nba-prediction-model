# High Accuracy Investigation Summary

## 🔍 Investigation Results

### **Critical Finding: Test Set Accuracy = 98.80%**

This is **suspiciously high** and indicates possible data leakage or overfitting.

---

## 📊 Feature Importance Analysis

### **Top 5 Most Important Features:**

| Rank | Feature | Importance | Issue |
|------|---------|------------|-------|
| 1 | `home_loss_streak` | **68.3%** | ⚠️ **DOMINANT** - Single feature accounts for 2/3 of model decisions |
| 2 | `home_win_streak` | 12.4% | High importance |
| 3 | `away_loss_streak` | 6.1% | High importance |
| 4 | `away_win_streak` | 3.5% | Moderate importance |
| 5 | `h2h_away_wins` | 1.6% | Low importance |

**Key Observation:** The model is **heavily dependent on streak features**, particularly `home_loss_streak` which alone accounts for 68% of feature importance.

---

## 📈 Feature-Target Correlations

### **Top Correlated Features:**

| Feature | Correlation | Interpretation |
|---------|-------------|----------------|
| `away_win_streak` | -0.6594 | Strong negative correlation |
| `h2h_avg_point_differential` | +0.6257 | Strong positive correlation |
| `home_win_streak` | +0.6243 | Strong positive correlation |
| `home_loss_streak` | -0.6017 | Strong negative correlation |
| `h2h_home_wins` | +0.5645 | Strong positive correlation |

**Analysis:** Streak features and head-to-head features show very strong correlations with the target, which explains the high accuracy.

---

## ⚠️ Potential Issues Identified

### **1. Streak Feature Dominance**
- `home_loss_streak` has **68.3% feature importance**
- This is a **red flag** - no single feature should dominate this much
- Suggests the model may be overfitting to streak patterns

### **2. Suspicious Feature Names**
Found **28 features** with names containing "win", "streak", or "differential":
- `home_win_streak`, `away_win_streak`
- `home_loss_streak`, `away_loss_streak`
- `home_win_pct`, `away_win_pct`
- `h2h_home_wins`, `h2h_away_wins`
- `win_pct_differential`
- `avg_point_differential`

**Note:** These are legitimate features (not direct target leakage), but their high correlation suggests they may be too predictive.

### **3. Test Set Performance**
- **Test Accuracy: 98.80%** (on 2025-26 season)
- This is **suspiciously high** for sports prediction
- Typical NBA prediction models achieve 60-70% accuracy
- 98.8% suggests either:
  - Data leakage (unlikely - we verified temporal constraints)
  - Overfitting to specific patterns
  - Streak features are genuinely very predictive (but this seems too good to be true)

---

## 🎯 Today's Predictions (2026-01-02)

### **Game 1: Miami Heat @ Detroit Pistons**
- **Predicted Winner:** Miami Heat (Away)
- **Home Win Probability:** 13.0%
- **Away Win Probability:** 87.0%
- **Confidence:** 87.0%
- **Predicted Margin:** Detroit by 1.2 points (⚠️ **CONTRADICTION** - predicts away wins but margin favors home)

### **Game 2: Philadelphia 76ers @ Dallas Mavericks**
- **Predicted Winner:** Philadelphia 76ers (Away)
- **Home Win Probability:** 5.4%
- **Away Win Probability:** 94.6%
- **Confidence:** 94.6%
- **Predicted Margin:** Philadelphia by 4.3 points

### **Game 3: Boston Celtics @ Sacramento Kings**
- **Predicted Winner:** Boston Celtics (Away)
- **Home Win Probability:** 7.3%
- **Away Win Probability:** 92.7%
- **Confidence:** 92.7%
- **Predicted Margin:** Boston by 2.8 points

### **Game 4: Utah Jazz @ LA Clippers**
- **Predicted Winner:** Utah Jazz (Away)
- **Home Win Probability:** 4.8%
- **Away Win Probability:** 95.2%
- **Confidence:** 95.2%
- **Predicted Margin:** Utah by 7.1 points

**Observation:** All 4 games predict **away team wins** with very high confidence (87-95%). This is unusual and suggests the model may have a bias.

---

## 🔬 Root Cause Hypothesis

### **Primary Suspect: Streak Feature Calculation**

The `home_loss_streak` feature has **68.3% importance**. This suggests:

1. **Possible Temporal Leakage:** 
   - If `loss_streak` is calculated incorrectly, it might include the current game's result
   - Need to verify: Does `loss_streak` only count games BEFORE the current game?

2. **Streak Features Are Too Predictive:**
   - Teams on long losing streaks tend to lose (obvious)
   - Teams on long winning streaks tend to win (obvious)
   - The model may be learning: "If home team has loss_streak > X, they lose"
   - This is technically correct but may not generalize well

3. **Model Overfitting:**
   - The model memorized streak patterns from training data
   - Test set (2025-26) may have similar patterns, giving false high accuracy
   - Real-world predictions may fail when patterns change

---

## 🎯 Recommended Next Steps

1. **Verify Streak Calculation:**
   - Check `scripts/transform_features.py` to ensure streaks only use past games
   - Verify: `loss_streak` should NOT include the current game

2. **Test on Truly Unseen Data:**
   - Make predictions on future games (not in database yet)
   - Track accuracy over time
   - Compare to baseline (e.g., always predict home team)

3. **Feature Ablation Study:**
   - Retrain model WITHOUT streak features
   - Compare accuracy
   - If accuracy drops significantly, streaks are the issue

4. **Check for Data Quality Issues:**
   - Verify game results are correct
   - Check if there are duplicate games
   - Verify streak calculations manually for a few games

5. **Investigate Prediction Contradiction:**
   - Game 1: Predicts away wins but margin favors home
   - This suggests classification and regression models may be misaligned

---

## 📝 Summary

**The model's 98.8% accuracy is likely due to:**
- **Streak features being extremely predictive** (especially `home_loss_streak` at 68% importance)
- **Possible overfitting** to streak patterns
- **Potential calculation errors** in streak features (needs verification)

**The predictions show:**
- All away teams predicted to win (unusual bias)
- Very high confidence (87-95%)
- One contradiction between classification and regression predictions

**Next Action:** Verify streak feature calculation logic to ensure no temporal leakage.

