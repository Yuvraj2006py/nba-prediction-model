"""Unit tests for MatchupFeatureCalculator."""

import unittest
from unittest.mock import Mock, patch
from datetime import date
from src.features.matchup_features import MatchupFeatureCalculator
from src.database.db_manager import DatabaseManager
from src.database.models import Game


class TestMatchupFeatureCalculator(unittest.TestCase):
    """Test cases for MatchupFeatureCalculator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = Mock(spec=DatabaseManager)
        self.calculator = MatchupFeatureCalculator(db_manager=self.db_manager)
    
    def test_init(self):
        """Test calculator initialization."""
        self.assertIsNotNone(self.calculator)
        self.assertEqual(self.calculator.db_manager, self.db_manager)
        self.assertIsNotNone(self.calculator.team_calculator)
    
    def test_get_head_to_head_record(self):
        """Test H2H record calculation."""
        # Create mock H2H games
        game1 = Mock(spec=Game)
        game1.game_date = date(2024, 1, 1)
        game1.home_team_id = '1610612737'
        game1.away_team_id = '1610612738'
        game1.winner = '1610612737'
        
        game2 = Mock(spec=Game)
        game2.game_date = date(2024, 1, 2)
        game2.home_team_id = '1610612738'
        game2.away_team_id = '1610612737'
        game2.winner = '1610612738'
        
        game3 = Mock(spec=Game)
        game3.game_date = date(2024, 1, 3)
        game3.home_team_id = '1610612737'
        game3.away_team_id = '1610612738'
        game3.winner = '1610612737'
        
        self.db_manager.get_games.return_value = [game1, game2, game3]
        
        result = self.calculator.get_head_to_head_record('1610612737', '1610612738', 5, date.today())
        
        self.assertEqual(result['team1_wins'], 2)
        self.assertEqual(result['team2_wins'], 1)
        self.assertEqual(result['total_games'], 3)
    
    def test_get_head_to_head_record_no_games(self):
        """Test H2H record when no games exist."""
        self.db_manager.get_games.return_value = []
        
        result = self.calculator.get_head_to_head_record('1610612737', '1610612738', 5, date.today())
        
        self.assertEqual(result['team1_wins'], 0)
        self.assertEqual(result['team2_wins'], 0)
        self.assertEqual(result['total_games'], 0)


if __name__ == '__main__':
    unittest.main()

