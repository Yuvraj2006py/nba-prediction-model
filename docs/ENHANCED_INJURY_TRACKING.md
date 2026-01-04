# Enhanced Injury Tracking Feature

## Overview

This document outlines the design and implementation plan for an enhanced injury tracking system that goes beyond simple player counts to provide weighted, player-importance-based injury impact metrics. This will significantly improve prediction accuracy by accounting for which players are injured and how their absence affects team performance.

## Current State

### Existing Implementation

The current system tracks injuries at a basic level:

**Features:**
- `players_out`: Count of players with `injury_status='out'`
- `players_questionable`: Count of players with `injury_status='questionable'`
- `injury_severity_score`: Simple formula `(players_out * 1.0 + players_questionable * 0.5) / total_players`

**How It Works:**
- Injury status is inferred from `PlayerStats.minutes_played`:
  - `0 minutes` → `'out'`
  - `<5 minutes` → `'questionable'`
  - Otherwise → `'healthy'`
- Calculated in `TeamFeatureCalculator.calculate_injury_impact()`
- Uses most recent game's player stats to determine injury status

**Location:**
- `src/features/team_features.py` - `calculate_injury_impact()` method
- `src/features/feature_aggregator.py` - Aggregates injury features
- `src/database/models.py` - `PlayerStats.injury_status` field

### Limitations

1. **No Player Importance Weighting**: Treats all players equally (star player out = bench player out)
2. **No Historical Context**: Doesn't learn how team performs when specific players are injured
3. **No Specific Player Tracking**: Only counts injuries, doesn't track which players
4. **Reactive Detection**: Only detects injuries after they've already affected a game (via minutes played)
5. **No Offensive/Defensive Impact**: Doesn't estimate how injuries affect scoring or defense
6. **No Relative Advantage**: Doesn't compare injury impact between teams

## Proposed Enhanced System

### Core Concept

Instead of treating all injuries equally, weight them by:
1. **Player Importance**: How valuable is this player to the team?
2. **Historical Impact**: How has the team performed when this player (or similar players) were out?
3. **Positional Impact**: Does the injury create a positional weakness?
4. **Timing**: Is this a key player missing a critical game?

### Key Metrics to Calculate

#### 1. Player Importance Score

For each player, calculate their value to the team:

```python
player_importance = f(
    avg_minutes_per_game,      # Usage/role
    avg_points_per_game,       # Offensive contribution
    avg_assists_per_game,      # Playmaking
    avg_rebounds_per_game,     # Rebounding
    avg_plus_minus,            # Overall impact
    games_played               # Availability
)
```

**Calculation Options:**
- **Simple**: `(points + assists + rebounds) / games_played`
- **Weighted**: `(points * 2 + assists * 1.5 + rebounds * 1.2 + plus_minus * 0.5) / games_played`
- **Advanced**: Use advanced metrics like VORP, BPM, or PER if available
- **Normalized**: Scale to 0-1 range for easier comparison

#### 2. Weighted Injury Impact

Instead of counting injuries, sum their importance:

```python
weighted_injury_score = sum(
    player_importance[player] * injury_severity[player]
    for player in injured_players
)
```

Where:
- `injury_severity['out'] = 1.0`
- `injury_severity['questionable'] = 0.5`
- `injury_severity['healthy'] = 0.0`

#### 3. Historical Performance Impact

For each key player, analyze team performance when they're out:

```python
def calculate_player_impact(team_id, player_id):
    games_with_player = get_games_where_played(team_id, player_id)
    games_without_player = get_games_where_missed(team_id, player_id)
    
    win_pct_with = calculate_win_pct(games_with_player)
    win_pct_without = calculate_win_pct(games_without_player)
    
    point_diff_with = calculate_avg_point_diff(games_with_player)
    point_diff_without = calculate_avg_point_diff(games_without_player)
    
    return {
        'win_pct_delta': win_pct_with - win_pct_without,
        'point_diff_delta': point_diff_with - point_diff_without,
        'offensive_impact': points_for_with - points_for_without,
        'defensive_impact': points_against_with - points_against_without
    }
```

#### 4. Positional Depth Analysis

Check if injury creates a positional weakness:

```python
def check_positional_depth(team_id, injured_players, game_date):
    # Group players by position
    # Check if any position has <2 healthy players
    # Calculate depth score (0-1, lower = weaker depth)
```

## Data Requirements

### Existing Data Sources

1. **PlayerStats Table**
   - `player_id`, `player_name`, `team_id`
   - `minutes_played`, `points`, `assists`, `rebounds`, `plus_minus`
   - `injury_status` (currently inferred)
   - `game_id`, `game_date`

2. **Game Table**
   - `game_id`, `game_date`, `home_team_id`, `away_team_id`
   - `home_score`, `away_score`, `winner`

### Additional Data Needed

1. **Player Positions** (if not already tracked)
   - PG, SG, SF, PF, C
   - Could be inferred from historical lineups or added manually

2. **Real-time Injury Reports** (future enhancement)
   - NBA API injury endpoints
   - Sports news APIs
   - Manual updates before games

3. **Player Roster Information**
   - Which players are on active roster
   - Starter vs bench designation
   - Contract value (proxy for importance)

## Implementation Plan

### Phase 1: Player Importance Calculation

**Goal**: Calculate how important each player is to their team.

**Steps:**
1. Create `calculate_player_importance()` method in `TeamFeatureCalculator`
2. Calculate per-game averages for key stats (last 20 games)
3. Create importance score formula
4. Store/cache player importance scores

**Code Location**: `src/features/team_features.py`

**New Method**:
```python
def calculate_player_importance(
    self,
    player_id: str,
    games_back: int = 20,
    end_date: Optional[date] = None
) -> Dict[str, float]:
    """
    Calculate player's importance to their team.
    
    Returns:
        {
            'importance_score': 0.0-1.0,
            'avg_minutes': float,
            'avg_points': float,
            'avg_assists': float,
            'avg_rebounds': float,
            'avg_plus_minus': float,
            'usage_rate': float  # Minutes / team_total_minutes
        }
    """
```

**Implementation Details:**
- Query `PlayerStats` for player's recent games
- Calculate averages for key metrics
- Normalize to 0-1 scale for importance score
- Handle players with <5 games (use league average as baseline)

### Phase 2: Enhanced Injury Impact Calculation

**Goal**: Replace simple injury counting with weighted importance-based scoring.

**Steps:**
1. Enhance `calculate_injury_impact()` to use player importance
2. Calculate weighted injury scores
3. Identify key players (top 3-5 by importance) who are out
4. Add new features: `weighted_injury_score`, `key_player_out`, etc.

**Code Location**: `src/features/team_features.py`

**Enhanced Method**:
```python
def calculate_injury_impact(
    self,
    team_id: str,
    end_date: Optional[date] = None
) -> Dict[str, Optional[float]]:
    """
    Enhanced injury impact with player importance weighting.
    
    Returns:
        {
            'players_out': int,  # Keep for backward compatibility
            'players_questionable': int,
            'injury_severity_score': float,  # Keep for backward compatibility
            'weighted_injury_score': float,  # NEW: Importance-weighted
            'key_player_out': bool,  # NEW: Is a top-3 player out?
            'key_players_out_count': int,  # NEW: Count of top-5 players out
            'weighted_players_out': float,  # NEW: Weighted count
            'weighted_players_questionable': float  # NEW: Weighted count
        }
    """
```

**Implementation Details:**
- Get current roster and injury status (from most recent game or upcoming game)
- Calculate importance for each player
- Identify top players (e.g., top 5 by importance)
- Weight injuries by importance
- Calculate weighted scores

### Phase 3: Historical Impact Analysis

**Goal**: Learn how teams perform when specific players are injured.

**Steps:**
1. Create `calculate_historical_injury_impact()` method
2. For each key player, find games they missed
3. Compare team performance with vs without player
4. Store historical impact metrics

**Code Location**: `src/features/team_features.py`

**New Method**:
```python
def calculate_historical_injury_impact(
    self,
    team_id: str,
    end_date: Optional[date] = None,
    games_back: int = 50
) -> Dict[str, float]:
    """
    Analyze how team performs when key players are injured.
    
    Returns:
        {
            'avg_win_pct_with_key_players': float,
            'avg_win_pct_without_key_players': float,
            'win_pct_delta': float,  # Negative = worse without
            'avg_point_diff_with': float,
            'avg_point_diff_without': float,
            'point_diff_delta': float,
            'offensive_impact_lost': float,  # Points lost when key players out
            'defensive_impact_lost': float  # Points allowed increase
        }
    """
```

**Implementation Details:**
- Get team's recent games
- For each game, identify which key players were out
- Group games: with key players vs without
- Calculate performance metrics for each group
- Return deltas and impacts

### Phase 4: New Features Integration

**Goal**: Add new injury features to feature pipeline.

**Steps:**
1. Update `FeatureAggregator` to include new features
2. Update `transform_features.py` to calculate new features
3. Update database schema if needed (or calculate on-the-fly)
4. Update model training to include new features

**New Features to Add:**

1. **Weighted Injury Features:**
   - `home_weighted_injury_score`
   - `away_weighted_injury_score`
   - `injury_advantage` (home - away weighted score)

2. **Key Player Features:**
   - `home_key_player_out` (boolean)
   - `away_key_player_out` (boolean)
   - `key_player_advantage` (home - away, -1 to 1)

3. **Historical Impact Features:**
   - `home_historical_injury_impact`
   - `away_historical_injury_impact`
   - `home_offensive_impact_lost`
   - `away_offensive_impact_lost`

4. **Positional Depth Features:**
   - `home_positional_depth_score`
   - `away_positional_depth_score`
   - `positional_advantage`

**Code Locations:**
- `src/features/feature_aggregator.py` - Add to `_calculate_team_features()`
- `scripts/transform_features.py` - Add to `_compute_rolling_averages()`
- `src/database/models.py` - Add columns to `TeamRollingFeatures` if storing

### Phase 5: Real-time Injury Data (Future)

**Goal**: Get injury reports before games, not after.

**Options:**
1. **NBA API**: Check if injury endpoints exist
2. **Sports News APIs**: ESPN, The Score, etc.
3. **Web Scraping**: Scrape injury reports from team websites
4. **Manual Updates**: Allow manual entry before games

**Implementation:**
- Create `InjuryDataCollector` class
- Integrate with data collection pipeline
- Update `PlayerStats.injury_status` before games
- Fall back to minutes-based detection if API unavailable

## Feature Engineering Details

### Player Importance Formula

**Option 1: Simple Weighted Average**
```python
importance = (
    (avg_points * 0.4) +
    (avg_assists * 0.25) +
    (avg_rebounds * 0.2) +
    (avg_plus_minus * 0.15)
) / max_possible_score
```

**Option 2: Usage-Based**
```python
importance = (avg_minutes / 48.0) * 0.5 + (stat_contribution / max_stat) * 0.5
```

**Option 3: Percentile-Based**
```python
# Rank player within team by stats
# Importance = percentile rank (0-1)
importance = percentile_rank(player_stats, team_players)
```

**Recommendation**: Start with Option 1, iterate based on results.

### Weighted Injury Score Formula

```python
weighted_injury_score = sum(
    player_importance[p] * severity_weight[p]
    for p in injured_players
)

where:
    severity_weight['out'] = 1.0
    severity_weight['questionable'] = 0.5
    severity_weight['healthy'] = 0.0
```

**Normalization**: Divide by sum of all player importances to get 0-1 score.

### Historical Impact Calculation

```python
# For each key player (top 5 by importance)
for player in key_players:
    games_with = [g for g in recent_games if player_played(g, player)]
    games_without = [g for g in recent_games if not player_played(g, player)]
    
    if len(games_without) >= 3:  # Need minimum sample
        win_pct_delta = win_pct(games_with) - win_pct(games_without)
        point_diff_delta = avg_point_diff(games_with) - avg_point_diff(games_without)
        
        player_impact[player] = {
            'win_pct_delta': win_pct_delta,
            'point_diff_delta': point_diff_delta
        }

# Aggregate across all key players
avg_win_pct_delta = mean([p['win_pct_delta'] for p in player_impact.values()])
```

## Database Schema Considerations

### Option 1: Calculate On-the-Fly (Recommended for MVP)

No schema changes needed. Calculate all metrics during feature generation.

**Pros:**
- No migration needed
- Always uses latest data
- Flexible to change formulas

**Cons:**
- Slower feature generation
- Can't easily query historical importance

### Option 2: Store Player Importance

Add new table or columns:

```sql
CREATE TABLE player_importance (
    player_id VARCHAR NOT NULL,
    team_id VARCHAR NOT NULL,
    season VARCHAR NOT NULL,
    importance_score FLOAT,
    avg_minutes FLOAT,
    avg_points FLOAT,
    avg_assists FLOAT,
    avg_rebounds FLOAT,
    calculated_date DATE,
    PRIMARY KEY (player_id, team_id, season)
);
```

**Pros:**
- Faster feature generation
- Can track importance over time
- Can query for analysis

**Cons:**
- Requires migration
- Needs update process
- More complex

### Option 3: Store in TeamRollingFeatures

Add columns to existing table:

```python
# In TeamRollingFeatures model
weighted_injury_score = Column(Float, nullable=True)
key_player_out = Column(Boolean, nullable=True)
key_players_out_count = Column(Integer, nullable=True)
historical_injury_impact = Column(Float, nullable=True)
```

**Pros:**
- Fits existing pipeline
- Easy to query
- Consistent with other features

**Cons:**
- Only stores for specific games
- Can't easily query player-level data

**Recommendation**: Start with Option 1, migrate to Option 2 if needed for performance.

## Testing Strategy

### Unit Tests

1. **Player Importance Calculation**
   - Test with various player stat profiles
   - Test edge cases (0 games, missing stats)
   - Verify normalization (0-1 range)

2. **Weighted Injury Impact**
   - Test with different injury scenarios
   - Verify weighting is correct
   - Test with no injuries, all injuries

3. **Historical Impact**
   - Test with sufficient vs insufficient data
   - Verify calculations match expected formulas
   - Test edge cases (player never missed games)

### Integration Tests

1. **Feature Generation**
   - Verify new features appear in feature vectors
   - Check feature values are reasonable
   - Ensure backward compatibility (old features still work)

2. **Model Training**
   - Verify model can train with new features
   - Check feature importance in trained model
   - Compare model performance with/without new features

### Validation Tests

1. **Real-world Scenarios**
   - Test with known injury situations (e.g., star player out)
   - Verify predictions adjust appropriately
   - Compare predictions before/after implementation

2. **Performance Impact**
   - Measure feature generation time
   - Check database query performance
   - Verify no significant slowdown

## Success Metrics

### Quantitative Metrics

1. **Model Performance**
   - Accuracy improvement (target: +2-5%)
   - Better calibration (especially for high-confidence predictions)
   - Reduced margin prediction errors

2. **Feature Importance**
   - New injury features rank in top 20 most important
   - `weighted_injury_score` has higher importance than `injury_severity_score`

3. **Prediction Quality**
   - Better predictions when key players are out
   - Reduced overconfidence in injury-affected games

### Qualitative Metrics

1. **Usefulness**
   - Predictions make sense when reviewing injury situations
   - Model correctly adjusts for star player absences
   - Historical impact aligns with known team performance

2. **Maintainability**
   - Code is well-documented
   - Easy to update formulas
   - Clear separation of concerns

## Implementation Timeline

### Week 1: Foundation
- [ ] Implement `calculate_player_importance()`
- [ ] Add unit tests
- [ ] Validate with sample data

### Week 2: Enhanced Injury Impact
- [ ] Enhance `calculate_injury_impact()`
- [ ] Add weighted injury features
- [ ] Update feature aggregator
- [ ] Integration tests

### Week 3: Historical Analysis
- [ ] Implement `calculate_historical_injury_impact()`
- [ ] Add historical impact features
- [ ] Test with historical data

### Week 4: Integration & Testing
- [ ] Update transform_features.py
- [ ] Full integration testing
- [ ] Model retraining with new features
- [ ] Performance validation

### Week 5: Optimization & Documentation
- [ ] Performance optimization
- [ ] Documentation updates
- [ ] Production deployment
- [ ] Monitor results

## Risks & Mitigations

### Risk 1: Insufficient Historical Data

**Problem**: Not enough games where key players were injured to calculate historical impact.

**Mitigation**:
- Use league-wide averages as fallback
- Use similar players' impact as proxy
- Start with simple importance weighting, add historical later

### Risk 2: Performance Degradation

**Problem**: Calculating player importance for all players slows down feature generation.

**Mitigation**:
- Cache player importance scores
- Calculate only for active roster
- Use efficient database queries
- Consider storing pre-calculated values

### Risk 3: Overfitting

**Problem**: Model overfits to injury patterns that don't generalize.

**Mitigation**:
- Use regularization in model training
- Validate on multiple seasons
- Monitor feature importance
- Use ensemble methods

### Risk 4: Data Quality Issues

**Problem**: Injury status detection is inaccurate (minutes-based inference).

**Mitigation**:
- Improve injury detection logic
- Add manual override capability
- Integrate real-time injury APIs
- Validate against known injury reports

## Future Enhancements

### Short-term (1-3 months)

1. **Positional Analysis**
   - Track positional depth
   - Identify positional mismatches
   - Add positional advantage features

2. **Injury Type Classification**
   - Categorize injuries (minor, major, chronic)
   - Estimate recovery time
   - Factor in injury history

3. **Lineup Impact**
   - Analyze how injuries affect starting lineups
   - Calculate lineup efficiency changes
   - Track bench depth

### Medium-term (3-6 months)

1. **Real-time Injury Integration**
   - NBA API integration
   - Automated injury report parsing
   - Pre-game injury updates

2. **Advanced Metrics**
   - VORP, BPM, PER integration
   - On/off court statistics
   - Lineup net rating changes

3. **Machine Learning for Impact**
   - Train model to predict injury impact
   - Learn team-specific adaptation patterns
   - Predict performance degradation

### Long-term (6+ months)

1. **Predictive Injury Modeling**
   - Predict injury likelihood
   - Fatigue-based injury risk
   - Load management insights

2. **Team Chemistry Metrics**
   - How injuries affect team chemistry
   - Role player step-up analysis
   - Coaching adjustment patterns

## Conclusion

Enhanced injury tracking will significantly improve prediction accuracy by:

1. **Weighting injuries by player importance** - A star player out matters more than a bench player
2. **Learning historical impact** - Understanding how teams adapt to injuries
3. **Providing richer context** - More nuanced features for the model to learn from

The implementation should be done incrementally, starting with player importance calculation and weighted injury scores, then adding historical impact analysis. This allows for validation at each step and reduces risk.

The key to success is:
- **Start simple**: Basic importance weighting is better than nothing
- **Iterate based on results**: Adjust formulas based on model performance
- **Validate thoroughly**: Test with real-world scenarios
- **Monitor continuously**: Track how features affect predictions

With proper implementation, this feature should improve accuracy by 2-5% and significantly reduce prediction errors when key players are injured.

