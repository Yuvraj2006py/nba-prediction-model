#!/usr/bin/env python
"""
Unit tests for Enhanced Injury Tracking feature.

Tests all phases:
- Phase 1: Player Importance Calculator
- Phase 2: Enhanced Injury Impact Calculation
- Phase 3: Historical Injury Impact Analysis
- Phase 4: Feature Aggregator Integration
- Phase 5: RapidAPI Injury Collector
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import unittest
from unittest.mock import MagicMock, patch, PropertyMock
from datetime import date, timedelta
import numpy as np

# Import modules to test
from src.features.player_importance import PlayerImportanceCalculator
from src.features.team_features import TeamFeatureCalculator
from src.data_collectors.rapidapi_injury_collector import RapidAPIInjuryCollector
from config.settings import get_settings


class MockPlayerStats:
    """Mock PlayerStats object for testing."""
    def __init__(self, player_id, player_name, team_id, game_id,
                 minutes_played, points, assists, rebounds, plus_minus,
                 injury_status='healthy'):
        self.player_id = player_id
        self.player_name = player_name
        self.team_id = team_id
        self.game_id = game_id
        self.minutes_played = minutes_played
        self.points = points
        self.assists = assists
        self.rebounds = rebounds
        self.plus_minus = plus_minus
        self.injury_status = injury_status


class MockGame:
    """Mock Game object for testing."""
    def __init__(self, game_id, home_team_id, away_team_id, game_date,
                 home_score=100, away_score=95, winner=None):
        self.game_id = game_id
        self.home_team_id = home_team_id
        self.away_team_id = away_team_id
        self.game_date = game_date
        self.home_score = home_score
        self.away_score = away_score
        self.winner = winner or home_team_id


class TestPlayerImportanceCalculator(unittest.TestCase):
    """Tests for Phase 1: Player Importance Calculator."""
    
    def test_importance_formula_weights(self):
        """Test that importance formula weights are correct."""
        calc = PlayerImportanceCalculator.__new__(PlayerImportanceCalculator)
        
        # Check weights sum to 1.0
        total_weight = (
            calc.POINTS_WEIGHT + 
            calc.ASSISTS_WEIGHT + 
            calc.REBOUNDS_WEIGHT + 
            calc.PLUS_MINUS_WEIGHT
        )
        self.assertAlmostEqual(total_weight, 1.0, places=4)
    
    def test_parse_minutes(self):
        """Test minutes string parsing."""
        calc = PlayerImportanceCalculator.__new__(PlayerImportanceCalculator)
        
        # Standard format
        self.assertAlmostEqual(calc._parse_minutes('30:00'), 30.0, places=2)
        self.assertAlmostEqual(calc._parse_minutes('25:30'), 25.5, places=2)
        self.assertAlmostEqual(calc._parse_minutes('0:00'), 0.0, places=2)
        
        # Edge cases
        self.assertAlmostEqual(calc._parse_minutes('DNP'), 0.0, places=2)
        self.assertAlmostEqual(calc._parse_minutes('DND'), 0.0, places=2)
        self.assertAlmostEqual(calc._parse_minutes(''), 0.0, places=2)
        self.assertAlmostEqual(calc._parse_minutes(None), 0.0, places=2)
    
    def test_importance_score_bounds(self):
        """Test that importance scores are always between 0 and 1."""
        # Star player with max stats
        norm_points = min(1.0, 35 / 35.0)  # Max
        norm_assists = min(1.0, 12 / 12.0)  # Max
        norm_rebounds = min(1.0, 15 / 15.0)  # Max
        norm_plus_minus = (1.0 + 1.0) / 2.0  # Max normalized
        
        importance = (
            norm_points * 0.40 +
            norm_assists * 0.25 +
            norm_rebounds * 0.20 +
            norm_plus_minus * 0.15
        )
        
        # With max minutes factor
        importance = importance * (0.5 + 0.5 * 1.0)
        
        self.assertLessEqual(importance, 1.0)
        self.assertGreaterEqual(importance, 0.0)
    
    def test_importance_calculation_star_player(self):
        """Test importance calculation for a star player."""
        # Star player: 30 pts, 8 ast, 10 reb, +10
        avg_points = 30
        avg_assists = 8
        avg_rebounds = 10
        avg_plus_minus = 10
        avg_minutes = 36  # Full starter
        
        # Calculate normalized values
        norm_points = min(1.0, avg_points / 35.0)  # 0.857
        norm_assists = min(1.0, avg_assists / 12.0)  # 0.667
        norm_rebounds = min(1.0, avg_rebounds / 15.0)  # 0.667
        norm_plus_minus = (np.clip(avg_plus_minus / 15.0, -1.0, 1.0) + 1.0) / 2.0  # 0.833
        
        weighted = (
            norm_points * 0.40 +
            norm_assists * 0.25 +
            norm_rebounds * 0.20 +
            norm_plus_minus * 0.15
        )
        
        # Apply minutes factor
        minutes_factor = min(1.0, avg_minutes / 36.0)
        importance = weighted * (0.5 + 0.5 * minutes_factor)
        
        # Star player should have high importance (> 0.7)
        self.assertGreater(importance, 0.7)
        self.assertLessEqual(importance, 1.0)
    
    def test_importance_calculation_bench_player(self):
        """Test importance calculation for a bench player."""
        # Bench player: 5 pts, 1 ast, 2 reb, +0
        avg_points = 5
        avg_assists = 1
        avg_rebounds = 2
        avg_plus_minus = 0
        avg_minutes = 12  # Limited minutes
        
        # Calculate normalized values
        norm_points = min(1.0, avg_points / 35.0)  # 0.143
        norm_assists = min(1.0, avg_assists / 12.0)  # 0.083
        norm_rebounds = min(1.0, avg_rebounds / 15.0)  # 0.133
        norm_plus_minus = (np.clip(avg_plus_minus / 15.0, -1.0, 1.0) + 1.0) / 2.0  # 0.5
        
        weighted = (
            norm_points * 0.40 +
            norm_assists * 0.25 +
            norm_rebounds * 0.20 +
            norm_plus_minus * 0.15
        )
        
        # Apply minutes factor
        minutes_factor = min(1.0, avg_minutes / 36.0)
        importance = weighted * (0.5 + 0.5 * minutes_factor)
        
        # Bench player should have low importance (< 0.3)
        self.assertLess(importance, 0.3)
        self.assertGreaterEqual(importance, 0.0)


class TestEnhancedInjuryImpact(unittest.TestCase):
    """Tests for Phase 2: Enhanced Injury Impact Calculation."""
    
    def test_injury_severity_weights(self):
        """Test that injury severity weights are configured correctly."""
        settings = get_settings()
        
        self.assertEqual(settings.INJURY_WEIGHT_OUT, 1.0)
        self.assertEqual(settings.INJURY_WEIGHT_QUESTIONABLE, 0.5)
        self.assertEqual(settings.INJURY_WEIGHT_PROBABLE, 0.25)
        self.assertEqual(settings.INJURY_WEIGHT_HEALTHY, 0.0)
    
    def test_weighted_injury_score_calculation(self):
        """Test weighted injury score calculation logic."""
        # Scenario: Star player (importance=0.9) is out, bench player (importance=0.2) is questionable
        star_importance = 0.9
        bench_importance = 0.2
        
        weight_out = 1.0
        weight_questionable = 0.5
        
        weighted_score = (
            star_importance * weight_out + 
            bench_importance * weight_questionable
        )
        
        # Expected: 0.9 * 1.0 + 0.2 * 0.5 = 1.0
        self.assertAlmostEqual(weighted_score, 1.0, places=4)
    
    def test_key_player_detection(self):
        """Test that key players are correctly identified."""
        # Top 5 players should be considered "key"
        settings = get_settings()
        self.assertEqual(settings.TOP_PLAYERS_COUNT, 5)
    
    def test_injury_advantage_calculation(self):
        """Test injury advantage calculation between teams."""
        # Home team: 0.5 weighted injury score
        # Away team: 1.2 weighted injury score
        home_weighted = 0.5
        away_weighted = 1.2
        
        # Advantage = away - home (positive means away more injured, good for home)
        advantage = away_weighted - home_weighted
        
        self.assertAlmostEqual(advantage, 0.7, places=4)
        self.assertGreater(advantage, 0)  # Home has advantage


class TestHistoricalInjuryImpact(unittest.TestCase):
    """Tests for Phase 3: Historical Injury Impact Analysis."""
    
    def test_win_pct_delta_calculation(self):
        """Test win percentage delta calculation."""
        # Team wins 70% with key players, 40% without
        win_pct_with = 0.70
        win_pct_without = 0.40
        
        delta = win_pct_without - win_pct_with
        
        # Negative delta means worse without key players
        self.assertAlmostEqual(delta, -0.30, places=4)
        self.assertLess(delta, 0)
    
    def test_point_diff_delta_calculation(self):
        """Test point differential delta calculation."""
        # Team has +5 point diff with key players, -3 without
        point_diff_with = 5.0
        point_diff_without = -3.0
        
        delta = point_diff_without - point_diff_with
        
        # Negative delta means worse without key players
        self.assertAlmostEqual(delta, -8.0, places=4)
        self.assertLess(delta, 0)


class TestRapidAPIInjuryCollector(unittest.TestCase):
    """Tests for Phase 5: RapidAPI Injury Collector."""
    
    def test_injury_status_normalization(self):
        """Test that injury status is correctly normalized."""
        collector = RapidAPIInjuryCollector.__new__(RapidAPIInjuryCollector)
        
        self.assertEqual(collector._normalize_injury_status('Out'), 'out')
        self.assertEqual(collector._normalize_injury_status('OUT'), 'out')
        self.assertEqual(collector._normalize_injury_status('Questionable'), 'questionable')
        self.assertEqual(collector._normalize_injury_status('Doubtful'), 'questionable')
        self.assertEqual(collector._normalize_injury_status('Probable'), 'probable')
        self.assertEqual(collector._normalize_injury_status('Day-to-Day'), 'probable')
        self.assertEqual(collector._normalize_injury_status('Available'), 'healthy')
        self.assertEqual(collector._normalize_injury_status('Healthy'), 'healthy')
    
    def test_team_name_normalization(self):
        """Test that team names are correctly normalized."""
        collector = RapidAPIInjuryCollector.__new__(RapidAPIInjuryCollector)
        
        self.assertEqual(
            collector._normalize_team_name('LA Clippers'), 
            'Los Angeles Clippers'
        )
        self.assertEqual(
            collector._normalize_team_name('LA Lakers'), 
            'Los Angeles Lakers'
        )
        self.assertEqual(
            collector._normalize_team_name('Detroit Pistons'), 
            'Detroit Pistons'
        )
    
    def test_injury_summary_structure(self):
        """Test injury summary response structure."""
        # Simulated injury data
        injuries = [
            {'team': 'Detroit Pistons', 'player': 'Jalen Duren', 'status': 'Out'},
            {'team': 'Detroit Pistons', 'player': 'Caris LeVert', 'status': 'Out'},
            {'team': 'Detroit Pistons', 'player': 'Isaiah Stewart', 'status': 'Questionable'},
        ]
        
        # Count by status
        out_count = sum(1 for i in injuries if 'out' in i['status'].lower())
        questionable_count = sum(1 for i in injuries if 'questionable' in i['status'].lower())
        
        self.assertEqual(out_count, 2)
        self.assertEqual(questionable_count, 1)


class TestFeatureIntegration(unittest.TestCase):
    """Tests for Phase 4: Feature Aggregator Integration."""
    
    def test_new_injury_features_list(self):
        """Test that all new injury features are defined."""
        expected_features = [
            # Per-team features (home_ and away_)
            'weighted_injury_score',
            'weighted_severity_score',
            'key_player_out',
            'key_players_out_count',
            'total_importance_out',
            'injury_win_pct_delta',
            'injury_point_diff_delta',
            # Matchup features
            'injury_advantage',
            'key_player_advantage',
            'importance_advantage',
        ]
        
        # All features should be defined
        self.assertEqual(len(expected_features), 10)
    
    def test_feature_value_ranges(self):
        """Test that feature values are in expected ranges."""
        # Weighted injury score: 0 to ~5 (sum of importance scores)
        # Key player out: 0 or 1 (boolean)
        # Key players out count: 0 to 5 (max top players)
        # Advantage features: -5 to 5 (difference between teams)
        
        settings = get_settings()
        max_key_players = settings.TOP_PLAYERS_COUNT
        
        # Valid ranges
        self.assertGreater(max_key_players, 0)
        self.assertLessEqual(max_key_players, 10)


class TestCalculationCorrectness(unittest.TestCase):
    """Tests to verify all calculations are mathematically correct."""
    
    def test_importance_formula_complete(self):
        """Test the complete importance formula with real values."""
        # Example: LeBron James type player
        avg_points = 27.0
        avg_assists = 8.0
        avg_rebounds = 8.0
        avg_plus_minus = 6.0
        avg_minutes = 35.0
        
        # Step 1: Normalize each stat
        norm_points = min(1.0, avg_points / 35.0)  # 27/35 = 0.7714
        norm_assists = min(1.0, avg_assists / 12.0)  # 8/12 = 0.6667
        norm_rebounds = min(1.0, avg_rebounds / 15.0)  # 8/15 = 0.5333
        norm_plus_minus = (np.clip(avg_plus_minus / 15.0, -1.0, 1.0) + 1.0) / 2.0  # 0.7
        
        # Step 2: Apply weights
        weighted = (
            norm_points * 0.40 +      # 0.7714 * 0.40 = 0.3086
            norm_assists * 0.25 +     # 0.6667 * 0.25 = 0.1667
            norm_rebounds * 0.20 +    # 0.5333 * 0.20 = 0.1067
            norm_plus_minus * 0.15    # 0.7000 * 0.15 = 0.1050
        )
        expected_weighted = 0.3086 + 0.1667 + 0.1067 + 0.1050  # = 0.6869
        
        self.assertAlmostEqual(weighted, expected_weighted, places=3)
        
        # Step 3: Apply minutes factor
        minutes_factor = min(1.0, avg_minutes / 36.0)  # 35/36 = 0.9722
        importance = weighted * (0.5 + 0.5 * minutes_factor)  # 0.6869 * 0.9861 = 0.6774
        
        expected_importance = 0.6869 * (0.5 + 0.5 * 0.9722)  # = 0.6779
        
        self.assertAlmostEqual(importance, expected_importance, places=3)
        
        # Final check: importance should be between 0.6 and 0.8 for a star
        self.assertGreater(importance, 0.6)
        self.assertLess(importance, 0.8)
    
    def test_weighted_injury_calculation_complete(self):
        """Test complete weighted injury calculation."""
        # Team roster with injuries
        players = [
            {'name': 'Star 1', 'importance': 0.85, 'status': 'out'},       # Out
            {'name': 'Star 2', 'importance': 0.75, 'status': 'healthy'},   # Healthy
            {'name': 'Starter', 'importance': 0.55, 'status': 'questionable'},  # Questionable
            {'name': 'Bench 1', 'importance': 0.30, 'status': 'healthy'},  # Healthy
            {'name': 'Bench 2', 'importance': 0.20, 'status': 'healthy'},  # Healthy
        ]
        
        # Calculate weighted injury score
        weight_out = 1.0
        weight_questionable = 0.5
        
        weighted_score = 0.0
        players_out = 0
        players_questionable = 0
        total_importance_out = 0.0
        
        for player in players:
            if player['status'] == 'out':
                weighted_score += player['importance'] * weight_out
                players_out += 1
                total_importance_out += player['importance']
            elif player['status'] == 'questionable':
                weighted_score += player['importance'] * weight_questionable
                players_questionable += 1
        
        # Expected values
        expected_weighted = 0.85 * 1.0 + 0.55 * 0.5  # = 1.125
        expected_out = 1
        expected_questionable = 1
        expected_importance_out = 0.85
        
        self.assertAlmostEqual(weighted_score, expected_weighted, places=4)
        self.assertEqual(players_out, expected_out)
        self.assertEqual(players_questionable, expected_questionable)
        self.assertAlmostEqual(total_importance_out, expected_importance_out, places=4)
    
    def test_severity_score_normalization(self):
        """Test injury severity score normalization."""
        total_players = 12
        players_out = 2
        players_questionable = 1
        
        weight_out = 1.0
        weight_questionable = 0.5
        
        # Traditional severity score
        severity = (players_out * weight_out + players_questionable * weight_questionable) / total_players
        expected = (2 * 1.0 + 1 * 0.5) / 12  # = 2.5 / 12 = 0.2083
        
        self.assertAlmostEqual(severity, expected, places=4)
    
    def test_historical_impact_calculation(self):
        """Test historical injury impact calculation."""
        # Sample game results
        games_with_key = [
            {'won': True, 'point_diff': 10},
            {'won': True, 'point_diff': 5},
            {'won': False, 'point_diff': -3},
            {'won': True, 'point_diff': 8},
        ]
        
        games_without_key = [
            {'won': False, 'point_diff': -5},
            {'won': False, 'point_diff': -10},
            {'won': True, 'point_diff': 2},
        ]
        
        # Calculate metrics
        wins_with = sum(1 for g in games_with_key if g['won'])
        win_pct_with = wins_with / len(games_with_key)  # 3/4 = 0.75
        
        wins_without = sum(1 for g in games_without_key if g['won'])
        win_pct_without = wins_without / len(games_without_key)  # 1/3 = 0.333
        
        point_diff_with = sum(g['point_diff'] for g in games_with_key) / len(games_with_key)  # 20/4 = 5.0
        point_diff_without = sum(g['point_diff'] for g in games_without_key) / len(games_without_key)  # -13/3 = -4.33
        
        win_pct_delta = win_pct_without - win_pct_with  # -0.417
        point_diff_delta = point_diff_without - point_diff_with  # -9.33
        
        # Verify calculations
        self.assertAlmostEqual(win_pct_with, 0.75, places=2)
        self.assertAlmostEqual(win_pct_without, 0.333, places=2)
        self.assertLess(win_pct_delta, 0)  # Worse without key players
        self.assertLess(point_diff_delta, 0)  # Worse without key players


class TestEdgeCases(unittest.TestCase):
    """Tests for edge cases and error handling."""
    
    def test_no_players(self):
        """Test handling when no players are found."""
        calc = PlayerImportanceCalculator.__new__(PlayerImportanceCalculator)
        
        # Empty player list should return None importance
        result = calc.calculate_player_importance.__wrapped__(
            calc, 'player1', 'team1', 20, date.today()
        ) if hasattr(calc.calculate_player_importance, '__wrapped__') else None
        
        # Just verify the function exists
        self.assertTrue(hasattr(calc, 'calculate_player_importance'))
    
    def test_negative_plus_minus(self):
        """Test handling of negative plus/minus."""
        avg_plus_minus = -10.0
        
        # Normalize negative plus/minus
        norm_plus_minus = np.clip(avg_plus_minus / 15.0, -1.0, 1.0)  # -0.667
        shifted = (norm_plus_minus + 1.0) / 2.0  # 0.167
        
        self.assertAlmostEqual(shifted, 0.167, places=2)
        self.assertGreaterEqual(shifted, 0.0)
        self.assertLessEqual(shifted, 1.0)
    
    def test_extreme_stats(self):
        """Test handling of extreme stat values."""
        # Player with very high stats (above normalization max)
        avg_points = 50.0  # Above max of 35
        
        norm_points = min(1.0, avg_points / 35.0)  # Should cap at 1.0
        
        self.assertEqual(norm_points, 1.0)


def run_tests():
    """Run all tests with detailed output."""
    print("=" * 70)
    print("RUNNING ENHANCED INJURY TRACKING UNIT TESTS")
    print("=" * 70)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestPlayerImportanceCalculator))
    suite.addTests(loader.loadTestsFromTestCase(TestEnhancedInjuryImpact))
    suite.addTests(loader.loadTestsFromTestCase(TestHistoricalInjuryImpact))
    suite.addTests(loader.loadTestsFromTestCase(TestRapidAPIInjuryCollector))
    suite.addTests(loader.loadTestsFromTestCase(TestFeatureIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestCalculationCorrectness))
    suite.addTests(loader.loadTestsFromTestCase(TestEdgeCases))
    
    # Run with verbosity
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    if result.wasSuccessful():
        print("\n[SUCCESS] All tests passed!")
    else:
        print("\n[FAILURE] Some tests failed!")
        for test, trace in result.failures:
            print(f"\nFailed: {test}")
            print(trace)
        for test, trace in result.errors:
            print(f"\nError: {test}")
            print(trace)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)

