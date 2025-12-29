"""Integration tests for FeatureAggregator."""

import unittest
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datetime import date
from src.database.db_manager import DatabaseManager
from src.features.feature_aggregator import FeatureAggregator
import pandas as pd


class TestFeatureAggregator(unittest.TestCase):
    """Integration tests for FeatureAggregator."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for the class."""
        cls.db_manager = DatabaseManager()
        cls.aggregator = FeatureAggregator(db_manager=cls.db_manager)
    
    def test_init(self):
        """Test aggregator initialization."""
        self.assertIsNotNone(self.aggregator)
        self.assertIsNotNone(self.aggregator.team_calc)
        self.assertIsNotNone(self.aggregator.matchup_calc)
        self.assertIsNotNone(self.aggregator.contextual_calc)
        self.assertIsNotNone(self.aggregator.betting_calc)
    
    def test_create_feature_vector_structure(self):
        """Test that feature vector has correct structure."""
        # Use a known game ID
        game_id = '0022401199'
        game = self.db_manager.get_game(game_id)
        
        if not game:
            self.skipTest(f"Game {game_id} not found in database")
        
        home_team_id = game.home_team_id
        away_team_id = game.away_team_id
        game_date = game.game_date
        
        # Create feature vector
        features_df = self.aggregator.create_feature_vector(
            game_id=game_id,
            home_team_id=home_team_id,
            away_team_id=away_team_id,
            end_date=game_date,
            use_cache=False
        )
        
        # Verify structure
        self.assertIsInstance(features_df, pd.DataFrame)
        self.assertEqual(len(features_df), 1)  # Single row
        
        # Check for expected feature columns
        expected_features = [
            'home_offensive_rating',
            'away_offensive_rating',
            'home_defensive_rating',
            'away_defensive_rating',
            'home_net_rating',
            'away_net_rating',
            'home_pace',
            'away_pace',
            'home_rest_days',
            'away_rest_days',
            'is_home_advantage'
        ]
        
        for feature in expected_features:
            self.assertIn(feature, features_df.columns, f"Missing feature: {feature}")
    
    def test_feature_vector_has_team_features(self):
        """Test that team features are included."""
        game_id = '0022401199'
        game = self.db_manager.get_game(game_id)
        
        if not game:
            self.skipTest(f"Game {game_id} not found in database")
        
        features_df = self.aggregator.create_feature_vector(
            game_id=game_id,
            home_team_id=game.home_team_id,
            away_team_id=game.away_team_id,
            end_date=game.game_date,
            use_cache=False
        )
        
        # Check team features exist
        team_features = [
            'home_offensive_rating',
            'home_defensive_rating',
            'home_net_rating',
            'home_pace',
            'away_offensive_rating',
            'away_defensive_rating',
            'away_net_rating',
            'away_pace'
        ]
        
        for feature in team_features:
            self.assertIn(feature, features_df.columns)
    
    def test_feature_vector_has_matchup_features(self):
        """Test that matchup features are included."""
        game_id = '0022401199'
        game = self.db_manager.get_game(game_id)
        
        if not game:
            self.skipTest(f"Game {game_id} not found in database")
        
        features_df = self.aggregator.create_feature_vector(
            game_id=game_id,
            home_team_id=game.home_team_id,
            away_team_id=game.away_team_id,
            end_date=game.game_date,
            use_cache=False
        )
        
        # Check matchup features exist
        matchup_features = [
            'h2h_home_wins',
            'h2h_away_wins',
            'h2h_total_games',
            'pace_differential'
        ]
        
        for feature in matchup_features:
            self.assertIn(feature, features_df.columns)
    
    def test_feature_vector_has_contextual_features(self):
        """Test that contextual features are included."""
        game_id = '0022401199'
        game = self.db_manager.get_game(game_id)
        
        if not game:
            self.skipTest(f"Game {game_id} not found in database")
        
        features_df = self.aggregator.create_feature_vector(
            game_id=game_id,
            home_team_id=game.home_team_id,
            away_team_id=game.away_team_id,
            end_date=game.game_date,
            use_cache=False
        )
        
        # Check contextual features exist
        contextual_features = [
            'home_rest_days',
            'away_rest_days',
            'home_is_b2b',
            'away_is_b2b',
            'is_home_advantage'
        ]
        
        for feature in contextual_features:
            self.assertIn(feature, features_df.columns)
    
    def test_save_and_retrieve_features(self):
        """Test feature caching functionality."""
        game_id = '0022401199'
        game = self.db_manager.get_game(game_id)
        
        if not game:
            self.skipTest(f"Game {game_id} not found in database")
        
        # Create and save features
        features_df = self.aggregator.create_feature_vector(
            game_id=game_id,
            home_team_id=game.home_team_id,
            away_team_id=game.away_team_id,
            end_date=game.game_date,
            use_cache=True
        )
        
        # Retrieve from cache
        cached_df = self.aggregator.get_features_from_db(game_id)
        
        self.assertIsNotNone(cached_df)
        self.assertIsInstance(cached_df, pd.DataFrame)
        # Should have similar structure (may have different column order)
        self.assertGreater(len(cached_df.columns), 10)


if __name__ == '__main__':
    unittest.main()

