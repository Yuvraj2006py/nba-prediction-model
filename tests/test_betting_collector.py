"""Unit tests for Betting Odds Collector."""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import date
import requests
from src.data_collectors.betting_odds_collector import BettingOddsCollector
from src.database.db_manager import DatabaseManager


class TestBettingOddsCollector(unittest.TestCase):
    """Test cases for BettingOddsCollector."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = Mock(spec=DatabaseManager)
        # Mock settings with API key
        with patch('src.data_collectors.betting_odds_collector.get_settings') as mock_settings:
            mock_settings.return_value.BETTING_API_KEY = 'test_api_key'
            mock_settings.return_value.BETTING_API_BASE_URL = 'https://api.the-odds-api.com/v4'
            mock_settings.return_value.RATE_LIMIT_DELAY = 0.1
            mock_settings.return_value.MAX_RETRIES = 3
            mock_settings.return_value.RETRY_DELAY = 1.0
            
            self.collector = BettingOddsCollector(db_manager=self.db_manager)
    
    def test_init(self):
        """Test collector initialization."""
        self.assertIsNotNone(self.collector)
        self.assertEqual(self.collector.db_manager, self.db_manager)
        self.assertEqual(self.collector.api_key, 'test_api_key')
    
    def test_init_no_api_key(self):
        """Test initialization without API key."""
        with patch('src.data_collectors.betting_odds_collector.get_settings') as mock_settings:
            mock_settings.return_value.BETTING_API_KEY = None
            mock_settings.return_value.BETTING_API_BASE_URL = 'https://api.the-odds-api.com/v4'
            mock_settings.return_value.RATE_LIMIT_DELAY = 0.1
            mock_settings.return_value.MAX_RETRIES = 3
            mock_settings.return_value.RETRY_DELAY = 1.0
            
            collector = BettingOddsCollector()
            self.assertIsNone(collector.api_key)
    
    @patch('src.data_collectors.betting_odds_collector.requests.get')
    def test_get_sports_success(self, mock_get):
        """Test successful sports fetch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {'key': 'basketball_nba', 'title': 'NBA'},
            {'key': 'basketball_ncaab', 'title': 'NCAAB'}
        ]
        mock_get.return_value = mock_response
        
        sports = self.collector.get_sports()
        
        self.assertEqual(len(sports), 2)
        self.assertEqual(sports[0]['key'], 'basketball_nba')
        mock_get.assert_called_once()
    
    @patch('src.data_collectors.betting_odds_collector.requests.get')
    def test_get_sports_failure(self, mock_get):
        """Test failed sports fetch."""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response
        
        sports = self.collector.get_sports()
        
        self.assertEqual(len(sports), 0)
    
    @patch('src.data_collectors.betting_odds_collector.requests.get')
    def test_get_nba_odds_success(self, mock_get):
        """Test successful NBA odds fetch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                'id': 'test_game_1',
                'sport_key': 'basketball_nba',
                'home_team': 'Lakers',
                'away_team': 'Warriors',
                'commence_time': '2024-01-15T20:00:00Z',
                'bookmakers': []
            }
        ]
        mock_get.return_value = mock_response
        
        odds = self.collector.get_nba_odds()
        
        self.assertEqual(len(odds), 1)
        self.assertEqual(odds[0]['home_team'], 'Lakers')
    
    def test_extract_betting_line_moneyline(self):
        """Test betting line extraction for moneyline."""
        market = {
            'key': 'h2h',
            'outcomes': [
                {'name': 'Lakers', 'price': -150},
                {'name': 'Warriors', 'price': 130}
            ]
        }
        
        line = self.collector._extract_betting_line('0022401199', 'draftkings', market)
        
        self.assertIsNotNone(line)
        self.assertEqual(line['game_id'], '0022401199')
        self.assertEqual(line['sportsbook'], 'draftkings')
        self.assertIsNotNone(line['moneyline_home'])
        self.assertIsNotNone(line['moneyline_away'])
    
    def test_extract_betting_line_spread(self):
        """Test betting line extraction for point spread."""
        market = {
            'key': 'spreads',
            'outcomes': [
                {'name': 'Lakers', 'point': -5.5, 'price': -110},
                {'name': 'Warriors', 'point': 5.5, 'price': -110}
            ]
        }
        
        line = self.collector._extract_betting_line('0022401199', 'draftkings', market)
        
        self.assertIsNotNone(line)
        self.assertIsNotNone(line['point_spread_home'])
        self.assertIsNotNone(line['point_spread_away'])
    
    def test_extract_betting_line_totals(self):
        """Test betting line extraction for over/under."""
        market = {
            'key': 'totals',
            'outcomes': [
                {'name': 'Over', 'point': 220.5, 'price': -110},
                {'name': 'Under', 'point': 220.5, 'price': -110}
            ]
        }
        
        line = self.collector._extract_betting_line('0022401199', 'draftkings', market)
        
        self.assertIsNotNone(line)
        self.assertEqual(line['over_under'], 220.5)


if __name__ == '__main__':
    unittest.main()

