"""Unit tests for TeamFeatureCalculator."""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import date, timedelta
from src.features.team_features import TeamFeatureCalculator
from src.database.db_manager import DatabaseManager
from src.database.models import TeamStats, Game


class TestTeamFeatureCalculator(unittest.TestCase):
    """Test cases for TeamFeatureCalculator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = Mock(spec=DatabaseManager)
        self.calculator = TeamFeatureCalculator(db_manager=self.db_manager)
    
    def test_init(self):
        """Test calculator initialization."""
        self.assertIsNotNone(self.calculator)
        self.assertEqual(self.calculator.db_manager, self.db_manager)
    
    def test_calculate_possessions(self):
        """Test possessions calculation."""
        # Create mock team stats
        stat = Mock(spec=TeamStats)
        stat.field_goals_attempted = 80
        stat.rebounds_offensive = 10
        stat.turnovers = 12
        stat.free_throws_attempted = 20
        
        possessions = self.calculator._calculate_possessions(stat)
        
        # Formula: FGA - ORB + TOV + (0.44 * FTA)
        expected = 80 - 10 + 12 + (0.44 * 20)  # = 90.8
        self.assertAlmostEqual(possessions, expected, places=1)
    
    def test_calculate_offensive_rating_insufficient_data(self):
        """Test offensive rating with insufficient data."""
        self.db_manager.get_team_stats_history.return_value = []
        
        result = self.calculator.calculate_offensive_rating('1610612737', 10, date.today())
        
        self.assertIsNone(result)
    
    def test_calculate_offensive_rating_sufficient_data(self):
        """Test offensive rating with sufficient data."""
        # Create mock stats
        stat1 = Mock(spec=TeamStats)
        stat1.points = 100
        stat1.field_goals_attempted = 80
        stat1.rebounds_offensive = 10
        stat1.turnovers = 12
        stat1.free_throws_attempted = 20
        
        stat2 = Mock(spec=TeamStats)
        stat2.points = 110
        stat2.field_goals_attempted = 85
        stat2.rebounds_offensive = 12
        stat2.turnovers = 10
        stat2.free_throws_attempted = 18
        
        stat3 = Mock(spec=TeamStats)
        stat3.points = 105
        stat3.field_goals_attempted = 82
        stat3.rebounds_offensive = 11
        stat3.turnovers = 11
        stat3.free_throws_attempted = 19
        
        self.db_manager.get_team_stats_history.return_value = [stat1, stat2, stat3]
        
        result = self.calculator.calculate_offensive_rating('1610612737', 10, date.today())
        
        self.assertIsNotNone(result)
        self.assertGreater(result, 0)
        self.assertIsInstance(result, float)
    
    def test_calculate_win_percentage(self):
        """Test win percentage calculation."""
        # Create mock games
        game1 = Mock(spec=Game)
        game1.game_date = date.today() - timedelta(days=3)
        game1.home_team_id = '1610612737'
        game1.away_team_id = '1610612738'
        game1.winner = '1610612737'
        
        game2 = Mock(spec=Game)
        game2.game_date = date.today() - timedelta(days=2)
        game2.home_team_id = '1610612738'
        game2.away_team_id = '1610612737'
        game2.winner = '1610612738'
        
        game3 = Mock(spec=Game)
        game3.game_date = date.today() - timedelta(days=1)
        game3.home_team_id = '1610612737'
        game3.away_team_id = '1610612739'
        game3.winner = '1610612737'
        
        self.db_manager.get_games.return_value = [game1, game2, game3]
        
        result = self.calculator.calculate_win_percentage('1610612737', 3, False, date.today())
        
        self.assertIsNotNone(result)
        # 2 wins out of 3 games = 66.67%
        self.assertAlmostEqual(result, 66.67, places=1)
    
    def test_calculate_avg_points_for(self):
        """Test average points calculation."""
        stat1 = Mock(spec=TeamStats)
        stat1.points = 100
        
        stat2 = Mock(spec=TeamStats)
        stat2.points = 110
        
        stat3 = Mock(spec=TeamStats)
        stat3.points = 105
        
        self.db_manager.get_team_stats_history.return_value = [stat1, stat2, stat3]
        
        result = self.calculator.calculate_avg_points_for('1610612737', 10, date.today())
        
        self.assertIsNotNone(result)
        # (100 + 110 + 105) / 3 = 105
        self.assertEqual(result, 105.0)


if __name__ == '__main__':
    unittest.main()

