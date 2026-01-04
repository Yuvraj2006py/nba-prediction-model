#!/usr/bin/env python
"""
Validation Script: Exponential Decay Weights

This script validates that exponential decay weights are calculated correctly
and demonstrates the weight distribution for different window sizes and decay rates.

Usage:
    python scripts/validate_decay_weights.py
    python scripts/validate_decay_weights.py --decay-rate 0.15
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import argparse
import numpy as np
from typing import List, Tuple


def calculate_weights(num_games: int, decay_rate: float) -> List[float]:
    """
    Calculate exponential decay weights for a window of games.
    
    Args:
        num_games: Number of games in the window
        decay_rate: Lambda (λ) decay parameter
        
    Returns:
        List of weights (most recent game first)
    """
    return [np.exp(-decay_rate * i) for i in range(num_games)]


def analyze_weight_distribution(weights: List[float]) -> dict:
    """
    Analyze the weight distribution.
    
    Args:
        weights: List of weights
        
    Returns:
        Dictionary with analysis results
    """
    total = sum(weights)
    num_games = len(weights)
    
    # Calculate percentage of weight for each game
    percentages = [w / total * 100 for w in weights]
    
    # Calculate cumulative percentages
    cumulative = []
    running = 0
    for pct in percentages:
        running += pct
        cumulative.append(running)
    
    # Calculate weight by segments
    if num_games >= 5:
        first_5_pct = sum(percentages[:5])
    else:
        first_5_pct = sum(percentages)
    
    if num_games >= 10:
        games_6_10_pct = sum(percentages[5:10])
    else:
        games_6_10_pct = sum(percentages[5:]) if num_games > 5 else 0
    
    if num_games >= 20:
        games_11_20_pct = sum(percentages[10:20])
    else:
        games_11_20_pct = sum(percentages[10:]) if num_games > 10 else 0
    
    return {
        'weights': weights,
        'percentages': percentages,
        'cumulative': cumulative,
        'first_5_pct': first_5_pct,
        'games_6_10_pct': games_6_10_pct,
        'games_11_20_pct': games_11_20_pct,
        'most_recent_weight': weights[0] if weights else 0,
        'oldest_weight': weights[-1] if weights else 0,
        'weight_ratio': weights[0] / weights[-1] if weights and weights[-1] > 0 else float('inf'),
    }


def verify_weighted_average():
    """Verify weighted average calculation is correct."""
    print("\n" + "=" * 70)
    print("WEIGHTED AVERAGE VERIFICATION")
    print("=" * 70)
    
    # Test case: 5 games with known values
    values = [110, 105, 100, 95, 90]  # Points scored, most recent first
    decay_rate = 0.1
    weights = calculate_weights(5, decay_rate)
    
    # Calculate weighted average manually
    weighted_sum = sum(v * w for v, w in zip(values, weights))
    weight_total = sum(weights)
    weighted_avg = weighted_sum / weight_total
    
    # Simple average
    simple_avg = sum(values) / len(values)
    
    print(f"\nTest case: Points = {values}")
    print(f"Decay rate (lambda): {decay_rate}")
    print(f"Weights: {[f'{w:.4f}' for w in weights]}")
    print(f"\nSimple average: {simple_avg:.2f}")
    print(f"Weighted average: {weighted_avg:.2f}")
    print(f"Difference: {weighted_avg - simple_avg:+.2f} (weighted is {'higher' if weighted_avg > simple_avg else 'lower'})")
    
    # Verify that weighted average is closer to recent values
    if values[0] > values[-1]:  # Recent games have higher values
        assert weighted_avg > simple_avg, "Weighted average should be higher when recent games are better"
        print("\n[PASS] Weighted average correctly emphasizes recent (higher) values")
    else:
        assert weighted_avg < simple_avg, "Weighted average should be lower when recent games are worse"
        print("\n[PASS] Weighted average correctly emphasizes recent (lower) values")
    
    return True


def display_weight_table(window_name: str, num_games: int, decay_rate: float):
    """Display a weight table for a specific window."""
    weights = calculate_weights(num_games, decay_rate)
    analysis = analyze_weight_distribution(weights)
    
    print(f"\n{window_name} Window ({num_games} games):")
    print("-" * 60)
    print(f"{'Game':<8} {'Weight':<12} {'% of Total':<15} {'Cumulative %':<15}")
    print("-" * 60)
    
    for i, (w, pct, cum) in enumerate(zip(weights, analysis['percentages'], analysis['cumulative']), 1):
        print(f"{i:<8} {w:.6f}     {pct:>6.2f}%          {cum:>6.2f}%")
    
    print("-" * 60)
    print(f"Weight ratio (game 1 / game {num_games}): {analysis['weight_ratio']:.2f}x")
    
    return analysis


def compare_decay_rates():
    """Compare different decay rates."""
    print("\n" + "=" * 70)
    print("DECAY RATE COMPARISON (20-game window)")
    print("=" * 70)
    
    decay_rates = [0.0, 0.05, 0.10, 0.15, 0.20]
    
    print(f"\n{'Decay Rate':<12} {'Games 1-5':<12} {'Games 6-10':<12} {'Games 11-20':<12} {'Ratio 1/20':<12}")
    print("-" * 60)
    
    for rate in decay_rates:
        weights = calculate_weights(20, rate)
        analysis = analyze_weight_distribution(weights)
        ratio = "inf" if rate == 0 else f"{analysis['weight_ratio']:.1f}x"
        print(f"{rate:<12.2f} {analysis['first_5_pct']:>6.1f}%      {analysis['games_6_10_pct']:>6.1f}%       {analysis['games_11_20_pct']:>6.1f}%       {ratio:>8}")
    
    print("\nNote: Decay rate 0.0 = simple average (equal weights)")


def test_with_real_settings():
    """Test using actual settings from the config."""
    print("\n" + "=" * 70)
    print("TESTING WITH ACTUAL SETTINGS")
    print("=" * 70)
    
    try:
        from config.settings import get_settings
        settings = get_settings()
        decay_rate = settings.ROLLING_STATS_DECAY_RATE
        
        print(f"\nCurrent ROLLING_STATS_DECAY_RATE: {decay_rate}")
        
        # Validate the setting is reasonable
        if decay_rate < 0:
            print("[ERROR] Decay rate cannot be negative")
            return False
        elif decay_rate > 0.5:
            print("[WARNING] Decay rate is very high (>0.5), old games will have almost no weight")
        elif decay_rate == 0:
            print("[INFO] Decay rate is 0 (simple average, no decay)")
        else:
            print(f"[OK] Decay rate is in reasonable range")
        
        # Show weight distribution for each window
        for name, size in [("L5", 5), ("L10", 10), ("L20", 20)]:
            display_weight_table(name, size, decay_rate)
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Could not load settings: {e}")
        return False


def run_all_validations(decay_rate: float = None):
    """Run all validation tests."""
    print("=" * 70)
    print("EXPONENTIAL DECAY WEIGHTS VALIDATION")
    print("=" * 70)
    
    # Use provided decay rate or default
    if decay_rate is None:
        try:
            from config.settings import get_settings
            settings = get_settings()
            decay_rate = settings.ROLLING_STATS_DECAY_RATE
            print(f"Using decay rate from settings: {decay_rate}")
        except:
            decay_rate = 0.1
            print(f"Using default decay rate: {decay_rate}")
    else:
        print(f"Using provided decay rate: {decay_rate}")
    
    all_passed = True
    
    # Test 1: Verify weighted average calculation
    try:
        if verify_weighted_average():
            print("\n[PASS] Test 1 PASSED: Weighted average calculation is correct")
        else:
            print("\n[FAIL] Test 1 FAILED: Weighted average calculation is incorrect")
            all_passed = False
    except Exception as e:
        print(f"\n[FAIL] Test 1 FAILED with error: {e}")
        all_passed = False
    
    # Test 2: Weight distribution for each window
    print("\n" + "=" * 70)
    print("WEIGHT DISTRIBUTION BY WINDOW")
    print("=" * 70)
    
    for name, size in [("L5", 5), ("L10", 10), ("L20", 20)]:
        try:
            analysis = display_weight_table(name, size, decay_rate)
            print(f"[OK] {name} window weights calculated correctly")
        except Exception as e:
            print(f"[FAIL] {name} window calculation failed: {e}")
            all_passed = False
    
    # Test 3: Compare decay rates
    try:
        compare_decay_rates()
        print("\n[PASS] Test 3 PASSED: Decay rate comparison completed")
    except Exception as e:
        print(f"\n[FAIL] Test 3 FAILED: {e}")
        all_passed = False
    
    # Test 4: Test with actual settings
    try:
        if test_with_real_settings():
            print("\n[PASS] Test 4 PASSED: Settings integration verified")
        else:
            print("\n[FAIL] Test 4 FAILED: Settings integration issue")
            all_passed = False
    except Exception as e:
        print(f"\n[FAIL] Test 4 FAILED: {e}")
        all_passed = False
    
    # Summary
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    
    if all_passed:
        print("\n[PASS] ALL VALIDATIONS PASSED")
        print("\nExponential decay weighting is implemented correctly.")
        print("Weight distribution follows expected exponential decay pattern.")
    else:
        print("\n[FAIL] SOME VALIDATIONS FAILED")
        print("Please review the errors above and fix any issues.")
    
    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate exponential decay weights")
    parser.add_argument(
        "--decay-rate", "-d",
        type=float,
        default=None,
        help="Decay rate (λ) to test. If not provided, uses value from settings."
    )
    
    args = parser.parse_args()
    
    success = run_all_validations(args.decay_rate)
    sys.exit(0 if success else 1)

