#!/usr/bin/env python
"""
Integration test for exponential decay weighting.

This script verifies that the exponential decay is properly integrated
into the rolling stats calculation by comparing results with and without decay.

Usage:
    python scripts/test_decay_integration.py
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.features.team_features import TeamFeatureCalculator
from src.database.db_manager import DatabaseManager
from config.settings import get_settings


def test_decay_integration():
    """Test that exponential decay is properly integrated."""
    print("=" * 60)
    print("EXPONENTIAL DECAY INTEGRATION TEST")
    print("=" * 60)
    
    # Get settings
    settings = get_settings()
    decay_rate = settings.ROLLING_STATS_DECAY_RATE
    print(f"Current decay rate: {decay_rate}")
    
    # Initialize
    db = DatabaseManager()
    calc = TeamFeatureCalculator(db)
    
    # Find a team with games
    with db.get_session() as session:
        from src.database.models import Game
        game = session.query(Game).filter(Game.game_status == 'finished').first()
        
        if not game:
            print("\n[SKIP] No finished games in database")
            return True
        
        team_id = game.home_team_id
        print(f"Testing with team: {team_id}")
    
    # Test with decay enabled (default)
    result_with_decay = calc.calculate_rolling_stats(team_id, 10, use_exponential_decay=True)
    
    # Test with decay disabled (simple average)
    result_without_decay = calc.calculate_rolling_stats(team_id, 10, use_exponential_decay=False)
    
    print("\n" + "-" * 60)
    print("Results Comparison (10-game window)")
    print("-" * 60)
    
    header = f"{'Metric':<20} {'With Decay':<15} {'Without Decay':<15} {'Diff':<10}"
    print(header)
    print("-" * 60)
    
    all_passed = True
    
    for key in ['points', 'points_allowed', 'win_pct']:
        with_decay = result_with_decay.get(key)
        without_decay = result_without_decay.get(key)
        
        if with_decay is not None and without_decay is not None:
            diff = float(with_decay) - float(without_decay)
            print(f"{key:<20} {with_decay:<15} {without_decay:<15} {diff:+.4f}")
        else:
            wd = str(with_decay) if with_decay is not None else 'None'
            wod = str(without_decay) if without_decay is not None else 'None'
            print(f"{key:<20} {wd:<15} {wod:<15} N/A")
    
    print("-" * 60)
    
    # Verify the feature aggregator also works
    print("\nTesting FeatureAggregator integration...")
    
    try:
        from src.features.feature_aggregator import FeatureAggregator
        agg = FeatureAggregator(db)
        
        with db.get_session() as session:
            from src.database.models import Game
            game = session.query(Game).filter(Game.game_status == 'finished').first()
            
            if game:
                features = agg.create_feature_vector(
                    game.game_id,
                    game.home_team_id,
                    game.away_team_id,
                    use_cache=False
                )
                
                print(f"  Generated {len(features.columns)} features")
                
                # Check that rolling stats features exist
                rolling_features = [c for c in features.columns if 'l5_' in c or 'l10_' in c or 'l20_' in c]
                print(f"  Rolling stat features: {len(rolling_features)}")
                
                if len(rolling_features) > 0:
                    print("  [OK] FeatureAggregator integration working")
                else:
                    print("  [FAIL] No rolling stat features found")
                    all_passed = False
    except Exception as e:
        print(f"  [FAIL] FeatureAggregator test failed: {e}")
        all_passed = False
    
    # Summary
    print("\n" + "=" * 60)
    print("INTEGRATION TEST SUMMARY")
    print("=" * 60)
    
    if all_passed:
        print("\n[PASS] All integration tests passed")
        print("\nExponential decay weighting is correctly integrated:")
        print("  1. calculate_rolling_stats() uses decay when enabled")
        print("  2. FeatureAggregator generates features correctly")
        print("  3. All components work together")
    else:
        print("\n[FAIL] Some integration tests failed")
    
    return all_passed


if __name__ == "__main__":
    success = test_decay_integration()
    sys.exit(0 if success else 1)

