"""Unit tests for NBA API Collector."""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import date
from src.data_collectors.nba_api_collector import NBAPICollector
from src.database.db_manager import DatabaseManager


class TestNBAPICollector(unittest.TestCase):
    """Test cases for NBAPICollector."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = Mock(spec=DatabaseManager)
        self.collector = NBAPICollector(db_manager=self.db_manager)
    
    def test_init(self):
        """Test collector initialization."""
        self.assertIsNotNone(self.collector)
        self.assertEqual(self.collector.db_manager, self.db_manager)
        self.assertIsNotNone(self.collector.settings)
    
    def test_rate_limit(self):
        """Test rate limiting."""
        import time
        start = time.time()
        self.collector._rate_limit()
        elapsed = time.time() - start
        # Should wait at least the rate limit delay
        self.assertGreaterEqual(elapsed, self.collector.rate_limit_delay * 0.9)
    
    @patch('src.data_collectors.nba_api_collector.teams')
    def test_collect_all_teams(self, mock_teams):
        """Test team collection."""
        mock_teams.get_teams.return_value = [
            {
                'id': 1610612737,
                'full_name': 'Atlanta Hawks',
                'abbreviation': 'ATL',
                'city': 'Atlanta'
            }
        ]
        
        teams = self.collector.collect_all_teams()
        
        self.assertEqual(len(teams), 1)
        self.assertEqual(teams[0]['team_id'], '1610612737')
        self.assertEqual(teams[0]['team_name'], 'Atlanta Hawks')
        self.db_manager.insert_team.assert_called_once()
    
    def test_extract_team_stats_from_api(self):
        """Test team stats extraction."""
        stats_dict = {
            'points': 100,
            'fieldGoalsMade': 40,
            'fieldGoalsAttempted': 80,
            'threePointersMade': 10,
            'threePointersAttempted': 25,
            'freeThrowsMade': 10,
            'freeThrowsAttempted': 12,
            'offensiveRebounds': 10,
            'defensiveRebounds': 30,
            'rebounds': 40,
            'assists': 25,
            'steals': 8,
            'blocks': 5,
            'turnovers': 12,
            'fouls': 20
        }
        
        result = self.collector._extract_team_stats_from_api(
            stats_dict,
            '0022401199',
            '1610612757',
            True
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['game_id'], '0022401199')
        self.assertEqual(result['team_id'], '1610612757')
        self.assertTrue(result['is_home'])
        self.assertEqual(result['points'], 100)
        self.assertEqual(result['field_goals_made'], 40)
        self.assertEqual(result['field_goals_attempted'], 80)
        self.assertGreater(result['field_goal_percentage'], 0)
    
    def test_extract_player_stats_from_api(self):
        """Test player stats extraction."""
        # Use actual API structure with nested statistics
        player_dict = {
            'personId': 203076,
            'firstName': 'LeBron',
            'familyName': 'James',
            'nameI': 'L. James',
            'statistics': {
                'minutes': '35:30',
                'points': 25,
                'reboundsTotal': 8,
                'assists': 10,
                'fieldGoalsMade': 10,
                'fieldGoalsAttempted': 18,
                'threePointersMade': 3,
                'threePointersAttempted': 7,
                'freeThrowsMade': 2,
                'freeThrowsAttempted': 3,
                'plusMinusPoints': 15.0
            }
        }
        
        result = self.collector._extract_player_stats_from_api(
            player_dict,
            '0022401199',
            '1610612747'
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['game_id'], '0022401199')
        self.assertEqual(result['team_id'], '1610612747')
        self.assertEqual(result['player_id'], '203076')
        self.assertEqual(result['player_name'], 'LeBron James')
        self.assertEqual(result['points'], 25)
        self.assertEqual(result['rebounds'], 8)
        self.assertEqual(result['assists'], 10)
        self.assertEqual(result['injury_status'], 'healthy')
    
    def test_extract_player_stats_injury_detection(self):
        """Test injury status detection from minutes."""
        # Player with no minutes (injured) - using actual API structure
        player_dict = {
            'personId': 203076,
            'firstName': 'Test',
            'familyName': 'Player',
            'nameI': 'T. Player',
            'statistics': {
                'minutes': '0:00',
                'points': 0,
                'reboundsTotal': 0,
                'assists': 0,
                'fieldGoalsMade': 0,
                'fieldGoalsAttempted': 0,
                'threePointersMade': 0,
                'threePointersAttempted': 0,
                'freeThrowsMade': 0,
                'freeThrowsAttempted': 0
            }
        }
        
        result = self.collector._extract_player_stats_from_api(
            player_dict,
            '0022401199',
            '1610612747'
        )
        
        # Should return None for players with no stats
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()

