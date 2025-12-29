"""Unit tests for ContextualFeatureCalculator."""

import unittest
from unittest.mock import Mock, patch
from datetime import date, timedelta
from src.features.contextual_features import ContextualFeatureCalculator
from src.database.db_manager import DatabaseManager
from src.database.models import Game, Team


class TestContextualFeatureCalculator(unittest.TestCase):
    """Test cases for ContextualFeatureCalculator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = Mock(spec=DatabaseManager)
        self.calculator = ContextualFeatureCalculator(db_manager=self.db_manager)
    
    def test_init(self):
        """Test calculator initialization."""
        self.assertIsNotNone(self.calculator)
        self.assertEqual(self.calculator.db_manager, self.db_manager)
    
    def test_calculate_rest_days(self):
        """Test rest days calculation."""
        # Create mock previous game
        prev_game = Mock(spec=Game)
        prev_game.game_date = date.today() - timedelta(days=2)
        
        self.db_manager.get_games.return_value = [prev_game]
        
        result = self.calculator.calculate_rest_days('1610612737', date.today())
        
        self.assertEqual(result, 2)
    
    def test_calculate_rest_days_no_previous_game(self):
        """Test rest days when no previous game exists."""
        self.db_manager.get_games.return_value = []
        
        result = self.calculator.calculate_rest_days('1610612737', date.today())
        
        self.assertIsNone(result)
    
    def test_is_back_to_back_true(self):
        """Test B2B detection when true."""
        prev_game = Mock(spec=Game)
        prev_game.game_date = date.today() - timedelta(days=0)  # Same day
        
        self.db_manager.get_games.return_value = [prev_game]
        
        result = self.calculator.is_back_to_back('1610612737', date.today())
        
        self.assertTrue(result)
    
    def test_is_back_to_back_false(self):
        """Test B2B detection when false."""
        prev_game = Mock(spec=Game)
        prev_game.game_date = date.today() - timedelta(days=2)
        
        self.db_manager.get_games.return_value = [prev_game]
        
        result = self.calculator.is_back_to_back('1610612737', date.today())
        
        self.assertFalse(result)
    
    def test_is_home_game_true(self):
        """Test home game detection when true."""
        game = Mock(spec=Game)
        game.home_team_id = '1610612737'
        game.away_team_id = '1610612738'
        
        self.db_manager.get_game.return_value = game
        
        result = self.calculator.is_home_game('1610612737', '0022401199')
        
        self.assertTrue(result)
    
    def test_is_home_game_false(self):
        """Test home game detection when false."""
        game = Mock(spec=Game)
        game.home_team_id = '1610612738'
        game.away_team_id = '1610612737'
        
        self.db_manager.get_game.return_value = game
        
        result = self.calculator.is_home_game('1610612737', '0022401199')
        
        self.assertFalse(result)
    
    def test_get_conference_matchup_same(self):
        """Test conference matchup when same conference."""
        team1 = Mock(spec=Team)
        team1.conference = 'Eastern'
        
        team2 = Mock(spec=Team)
        team2.conference = 'Eastern'
        
        self.db_manager.get_team.side_effect = [team1, team2]
        
        result = self.calculator.get_conference_matchup('1610612737', '1610612738')
        
        self.assertTrue(result)
    
    def test_get_conference_matchup_different(self):
        """Test conference matchup when different conferences."""
        team1 = Mock(spec=Team)
        team1.conference = 'Eastern'
        
        team2 = Mock(spec=Team)
        team2.conference = 'Western'
        
        self.db_manager.get_team.side_effect = [team1, team2]
        
        result = self.calculator.get_conference_matchup('1610612737', '1610612738')
        
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()

