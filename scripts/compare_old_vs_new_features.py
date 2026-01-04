#!/usr/bin/env python
"""
Comparison Script: Old vs New Feature Calculation

This script compares rolling statistics calculated with simple averaging (old)
versus exponential decay weighting (new) on sample data.

Usage:
    python scripts/compare_old_vs_new_features.py
    python scripts/compare_old_vs_new_features.py --team LAL --games 10
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import argparse
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Optional, Any

from config.settings import get_settings
from src.database.db_manager import DatabaseManager


def calculate_simple_average(values: List[float]) -> Optional[float]:
    """Calculate simple average (old method)."""
    valid_values = [v for v in values if v is not None]
    if not valid_values:
        return None
    return sum(valid_values) / len(valid_values)


def calculate_weighted_average(values: List[float], decay_rate: float) -> Optional[float]:
    """Calculate exponentially weighted average (new method)."""
    if not values or all(v is None for v in values):
        return None
    
    # Calculate weights
    weights = [np.exp(-decay_rate * i) for i in range(len(values))]
    
    # Filter out None values
    valid_pairs = [(v, w) for v, w in zip(values, weights) if v is not None]
    if not valid_pairs:
        return None
    
    weighted_sum = sum(v * w for v, w in valid_pairs)
    weight_sum = sum(w for _, w in valid_pairs)
    
    if weight_sum == 0:
        return None
    
    return weighted_sum / weight_sum


def compare_averages(values: List[float], stat_name: str, decay_rate: float) -> Dict[str, Any]:
    """Compare simple vs weighted average for a list of values."""
    simple_avg = calculate_simple_average(values)
    weighted_avg = calculate_weighted_average(values, decay_rate)
    
    if simple_avg is None or weighted_avg is None:
        return {
            'stat_name': stat_name,
            'values': values,
            'simple_avg': simple_avg,
            'weighted_avg': weighted_avg,
            'difference': None,
            'pct_difference': None,
        }
    
    difference = weighted_avg - simple_avg
    pct_difference = (difference / simple_avg * 100) if simple_avg != 0 else 0
    
    return {
        'stat_name': stat_name,
        'values': values,
        'simple_avg': round(simple_avg, 2),
        'weighted_avg': round(weighted_avg, 2),
        'difference': round(difference, 2),
        'pct_difference': round(pct_difference, 2),
    }


def get_sample_team_data(db_manager: DatabaseManager, team_id: str, num_games: int = 10) -> Dict[str, List[float]]:
    """Get sample game data for a team."""
    print(f"\nFetching last {num_games} games for team {team_id}...")
    
    # Get finished games
    games = db_manager.get_games(team_id=team_id, limit=num_games * 2)
    finished_games = [
        g for g in games 
        if g.game_status == 'finished' 
        and g.home_score is not None 
        and g.away_score is not None
    ]
    finished_games = sorted(finished_games, key=lambda x: x.game_date, reverse=True)[:num_games]
    
    if not finished_games:
        print(f"  No finished games found for {team_id}")
        return {}
    
    print(f"  Found {len(finished_games)} finished games")
    
    # Extract stats
    points = []
    points_allowed = []
    wins = []
    
    for game in finished_games:
        if game.home_team_id == team_id:
            points.append(float(game.home_score or 0))
            points_allowed.append(float(game.away_score or 0))
        else:
            points.append(float(game.away_score or 0))
            points_allowed.append(float(game.home_score or 0))
        
        wins.append(1.0 if game.winner == team_id else 0.0)
    
    return {
        'points': points,
        'points_allowed': points_allowed,
        'wins': wins,
        'game_dates': [g.game_date for g in finished_games],
    }


def run_comparison(team_id: Optional[str] = None, num_games: int = 10):
    """Run the comparison on real or sample data."""
    settings = get_settings()
    decay_rate = settings.ROLLING_STATS_DECAY_RATE
    
    print("=" * 70)
    print("OLD vs NEW FEATURE COMPARISON")
    print("=" * 70)
    print(f"Decay rate: {decay_rate}")
    print(f"Window size: {num_games} games")
    
    # If team_id provided, use real data; otherwise use synthetic example
    if team_id:
        try:
            db_manager = DatabaseManager()
            data = get_sample_team_data(db_manager, team_id, num_games)
            
            if not data:
                print("\nNo data found. Using synthetic example instead.")
                data = None
        except Exception as e:
            print(f"\nError fetching data: {e}")
            print("Using synthetic example instead.")
            data = None
    else:
        data = None
    
    if data is None:
        # Use synthetic data for demonstration
        print("\n" + "-" * 70)
        print("SYNTHETIC EXAMPLE: Team on a Hot Streak")
        print("-" * 70)
        print("Recent games have higher scores (improving form)")
        
        # Hot streak scenario: recent games are better
        data = {
            'points': [120, 118, 115, 108, 105, 102, 100, 98, 95, 92],  # Most recent first
            'points_allowed': [100, 102, 105, 108, 110, 112, 115, 118, 120, 122],
            'wins': [1, 1, 1, 1, 0, 1, 0, 0, 0, 1],
        }
    
    # Display the values
    print("\nGame-by-game data (most recent first):")
    print("-" * 70)
    
    if 'game_dates' in data:
        print(f"{'Game':<6} {'Date':<12} {'Points':<10} {'Allowed':<10} {'Win':<6}")
        for i, (pts, allowed, win, dt) in enumerate(zip(
            data['points'], data['points_allowed'], data['wins'], data['game_dates']
        ), 1):
            win_str = "W" if win else "L"
            print(f"{i:<6} {str(dt):<12} {pts:<10.0f} {allowed:<10.0f} {win_str:<6}")
    else:
        print(f"{'Game':<6} {'Points':<10} {'Allowed':<10} {'Win':<6}")
        for i, (pts, allowed, win) in enumerate(zip(
            data['points'], data['points_allowed'], data['wins']
        ), 1):
            win_str = "W" if win else "L"
            print(f"{i:<6} {pts:<10.0f} {allowed:<10.0f} {win_str:<6}")
    
    # Compare averages
    print("\n" + "-" * 70)
    print("COMPARISON: Simple Average vs Weighted Average")
    print("-" * 70)
    
    stats_to_compare = [
        ('Points Scored', data['points']),
        ('Points Allowed', data['points_allowed']),
        ('Win %', data['wins']),
    ]
    
    print(f"{'Statistic':<20} {'Simple Avg':<15} {'Weighted Avg':<15} {'Difference':<12} {'% Change':<10}")
    print("-" * 70)
    
    for stat_name, values in stats_to_compare:
        result = compare_averages(values, stat_name, decay_rate)
        
        if result['simple_avg'] is None:
            print(f"{stat_name:<20} {'N/A':<15} {'N/A':<15}")
        else:
            # Format win % differently
            if stat_name == 'Win %':
                simple = f"{result['simple_avg'] * 100:.1f}%"
                weighted = f"{result['weighted_avg'] * 100:.1f}%"
                diff = f"{result['difference'] * 100:+.1f}%"
            else:
                simple = f"{result['simple_avg']:.1f}"
                weighted = f"{result['weighted_avg']:.1f}"
                diff = f"{result['difference']:+.1f}"
            
            pct_change = f"{result['pct_difference']:+.1f}%"
            print(f"{stat_name:<20} {simple:<15} {weighted:<15} {diff:<12} {pct_change:<10}")
    
    # Analysis
    print("\n" + "-" * 70)
    print("ANALYSIS")
    print("-" * 70)
    
    pts_result = compare_averages(data['points'], 'Points', decay_rate)
    
    if pts_result['difference'] is not None:
        if pts_result['difference'] > 0:
            print(f"[OK] Recent scoring is BETTER than average (+{pts_result['difference']:.1f} points)")
            print("     Weighted average correctly reflects improving offensive form.")
        elif pts_result['difference'] < 0:
            print(f"[OK] Recent scoring is WORSE than average ({pts_result['difference']:.1f} points)")
            print("     Weighted average correctly reflects declining offensive form.")
        else:
            print("[OK] Scoring is consistent across the window.")
    
    allowed_result = compare_averages(data['points_allowed'], 'Allowed', decay_rate)
    
    if allowed_result['difference'] is not None:
        if allowed_result['difference'] < 0:
            print(f"[OK] Recent defense is BETTER than average ({allowed_result['difference']:.1f} points allowed)")
            print("     Weighted average correctly reflects improving defensive form.")
        elif allowed_result['difference'] > 0:
            print(f"[OK] Recent defense is WORSE than average (+{allowed_result['difference']:.1f} points allowed)")
            print("     Weighted average correctly reflects declining defensive form.")
        else:
            print("[OK] Defense is consistent across the window.")
    
    print("\n" + "=" * 70)
    print("CONCLUSION")
    print("=" * 70)
    print("\nExponential decay weighting correctly:")
    print("  1. Emphasizes recent performance over older games")
    print("  2. Captures hot streaks and slumps more accurately")
    print("  3. Provides a more responsive measure of current form")
    print(f"\nWith decay rate = {decay_rate}:")
    print(f"  - Most recent game has 100% weight")
    print(f"  - Game {num_games} has {np.exp(-decay_rate * (num_games-1)) * 100:.1f}% weight")
    print(f"  - Weight ratio (game 1 / game {num_games}): {1 / np.exp(-decay_rate * (num_games-1)):.1f}x")


def test_edge_cases():
    """Test edge cases for the weighted average function."""
    print("\n" + "=" * 70)
    print("EDGE CASE TESTING")
    print("=" * 70)
    
    settings = get_settings()
    decay_rate = settings.ROLLING_STATS_DECAY_RATE
    
    test_cases = [
        ("Empty list", []),
        ("Single value", [100.0]),
        ("All None", [None, None, None]),
        ("Some None", [100.0, None, 90.0]),
        ("All same values", [100.0, 100.0, 100.0]),
        ("Increasing trend", [100.0, 90.0, 80.0, 70.0, 60.0]),  # Most recent first
        ("Decreasing trend", [60.0, 70.0, 80.0, 90.0, 100.0]),  # Most recent first
    ]
    
    print(f"\n{'Test Case':<25} {'Simple Avg':<15} {'Weighted Avg':<15} {'Result':<10}")
    print("-" * 70)
    
    all_passed = True
    for name, values in test_cases:
        simple = calculate_simple_average(values)
        weighted = calculate_weighted_average(values, decay_rate)
        
        simple_str = f"{simple:.2f}" if simple is not None else "None"
        weighted_str = f"{weighted:.2f}" if weighted is not None else "None"
        
        # Verify expected behavior
        if name == "Empty list" and simple is None and weighted is None:
            result = "[PASS]"
        elif name == "All None" and simple is None and weighted is None:
            result = "[PASS]"
        elif name == "Single value" and simple == weighted:
            result = "[PASS]"
        elif name == "All same values" and abs(simple - weighted) < 0.01:
            result = "[PASS]"
        elif name == "Increasing trend" and weighted > simple:
            result = "[PASS]"
        elif name == "Decreasing trend" and weighted < simple:
            result = "[PASS]"
        elif name == "Some None" and simple is not None and weighted is not None:
            result = "[PASS]"
        else:
            result = "[FAIL]"
            all_passed = False
        
        print(f"{name:<25} {simple_str:<15} {weighted_str:<15} {result:<10}")
    
    print("-" * 70)
    if all_passed:
        print("[PASS] All edge cases handled correctly")
    else:
        print("[FAIL] Some edge cases failed")
    
    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare old vs new feature calculation")
    parser.add_argument(
        "--team", "-t",
        type=str,
        default=None,
        help="Team ID to analyze (e.g., 'LAL', '1610612747'). If not provided, uses synthetic data."
    )
    parser.add_argument(
        "--games", "-g",
        type=int,
        default=10,
        help="Number of games to analyze (default: 10)"
    )
    
    args = parser.parse_args()
    
    # Run main comparison
    run_comparison(args.team, args.games)
    
    # Run edge case tests
    test_edge_cases()

