"""Unit tests for BettingFeatureCalculator."""

import unittest
from unittest.mock import Mock
from src.features.betting_features import BettingFeatureCalculator
from src.database.db_manager import DatabaseManager
from src.database.models import BettingLine, Game


class TestBettingFeatureCalculator(unittest.TestCase):
    """Test cases for BettingFeatureCalculator."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = Mock(spec=DatabaseManager)
        self.calculator = BettingFeatureCalculator(db_manager=self.db_manager)
    
    def test_init(self):
        """Test calculator initialization."""
        self.assertIsNotNone(self.calculator)
        self.assertEqual(self.calculator.db_manager, self.db_manager)
    
    def test_american_to_probability_positive(self):
        """Test American to probability conversion for positive odds."""
        # +130 odds
        prob = self.calculator._american_to_probability(130)
        
        # Expected: 100 / (130 + 100) = 100 / 230 = 0.4348
        self.assertAlmostEqual(prob, 0.4348, places=4)
    
    def test_american_to_probability_negative(self):
        """Test American to probability conversion for negative odds."""
        # -150 odds
        prob = self.calculator._american_to_probability(-150)
        
        # Expected: 150 / (150 + 100) = 150 / 250 = 0.6
        self.assertAlmostEqual(prob, 0.6, places=4)
    
    def test_get_consensus_spread(self):
        """Test consensus spread calculation."""
        line1 = Mock(spec=BettingLine)
        line1.point_spread_home = -5.5
        
        line2 = Mock(spec=BettingLine)
        line2.point_spread_home = -6.0
        
        line3 = Mock(spec=BettingLine)
        line3.point_spread_home = -5.0
        
        self.db_manager.get_betting_lines.return_value = [line1, line2, line3]
        
        result = self.calculator.get_consensus_spread('0022401199')
        
        # Average: (-5.5 + -6.0 + -5.0) / 3 = -5.5
        self.assertAlmostEqual(result, -5.5, places=1)
    
    def test_get_consensus_spread_no_lines(self):
        """Test consensus spread when no lines exist."""
        self.db_manager.get_betting_lines.return_value = []
        
        result = self.calculator.get_consensus_spread('0022401199')
        
        self.assertIsNone(result)
    
    def test_get_moneyline_implied_prob_home(self):
        """Test moneyline implied probability for home team."""
        game = Mock(spec=Game)
        game.home_team_id = '1610612737'
        game.away_team_id = '1610612738'
        
        line1 = Mock(spec=BettingLine)
        line1.moneyline_home = -150
        
        line2 = Mock(spec=BettingLine)
        line2.moneyline_home = -140
        
        self.db_manager.get_game.return_value = game
        self.db_manager.get_betting_lines.return_value = [line1, line2]
        
        result = self.calculator.get_moneyline_implied_prob('0022401199', '1610612737')
        
        # -150 -> 0.6, -140 -> 0.5833, average -> ~0.5917
        self.assertIsNotNone(result)
        self.assertGreater(result, 0.5)
        self.assertLess(result, 1.0)
    
    def test_calculate_value(self):
        """Test expected value calculation."""
        # If model thinks 70% chance, but odds imply 60% chance
        ev = self.calculator.calculate_value(0.6, 0.7)
        
        # decimal_odds = 1/0.6 = 1.667
        # ev = (0.7 * 1.667) - 1 = 1.167 - 1 = 0.167
        self.assertAlmostEqual(ev, 0.1667, places=3)
        self.assertGreater(ev, 0)  # Positive value bet


if __name__ == '__main__':
    unittest.main()

