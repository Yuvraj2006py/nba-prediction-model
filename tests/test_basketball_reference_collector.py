"""Unit tests for Basketball Reference collector."""

import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import date
from bs4 import BeautifulSoup

from src.data_collectors.basketball_reference_collector import BasketballReferenceCollector
from src.database.db_manager import DatabaseManager
from src.database.models import Game, Team


class TestBasketballReferenceCollector(unittest.TestCase):
    """Test cases for BasketballReferenceCollector."""

    def setUp(self):
        """Set up test fixtures."""
        self.db_manager = Mock(spec=DatabaseManager)
        self.collector = BasketballReferenceCollector(db_manager=self.db_manager)

    def test_team_abbrev_mapping(self):
        """Test team abbreviation mapping."""
        # Test known mappings
        self.assertEqual(self.collector.TEAM_ABBREV_MAP['BKN'], 'BRK')
        self.assertEqual(self.collector.TEAM_ABBREV_MAP['CHA'], 'CHO')
        self.assertEqual(self.collector.TEAM_ABBREV_MAP['PHX'], 'PHO')
        # Test that most teams map to themselves
        self.assertEqual(self.collector.TEAM_ABBREV_MAP['LAL'], 'LAL')
        self.assertEqual(self.collector.TEAM_ABBREV_MAP['BOS'], 'BOS')

    def test_get_team_abbrev(self):
        """Test getting team abbreviation from database."""
        # Mock team
        mock_team = Mock(spec=Team)
        mock_team.team_abbreviation = 'LAL'
        self.db_manager.get_team.return_value = mock_team
        
        abbrev = self.collector._get_team_abbrev('1610612747')
        self.assertEqual(abbrev, 'LAL')
        self.db_manager.get_team.assert_called_once_with('1610612747')

    def test_get_team_abbrev_not_found(self):
        """Test getting abbreviation when team not found."""
        self.db_manager.get_team.return_value = None
        
        abbrev = self.collector._get_team_abbrev('9999999999')
        self.assertIsNone(abbrev)

    def test_build_boxscore_url(self):
        """Test building boxscore URL."""
        game_date = date(2023, 10, 24)
        home_abbrev = 'LAL'
        
        url = self.collector._build_boxscore_url(game_date, home_abbrev)
        expected = "https://www.basketball-reference.com/boxscores/202310240LAL.html"
        self.assertEqual(url, expected)

    def test_parse_float(self):
        """Test float parsing."""
        self.assertEqual(self.collector._parse_float('45.5'), 45.5)
        self.assertEqual(self.collector._parse_float('45'), 45.0)
        self.assertEqual(self.collector._parse_float('—'), 0.0)
        self.assertEqual(self.collector._parse_float(''), 0.0)
        self.assertEqual(self.collector._parse_float(None), 0.0)
        self.assertEqual(self.collector._parse_float('1,234.5'), 1234.5)

    def test_parse_int(self):
        """Test integer parsing."""
        self.assertEqual(self.collector._parse_int('45'), 45)
        self.assertEqual(self.collector._parse_int('45.7'), 45)
        self.assertEqual(self.collector._parse_int('—'), 0)
        self.assertEqual(self.collector._parse_int(''), 0)
        self.assertEqual(self.collector._parse_int(None), 0)
        self.assertEqual(self.collector._parse_int('1,234'), 1234)

    def test_parse_minutes(self):
        """Test minutes parsing."""
        self.assertEqual(self.collector._parse_minutes('35:24'), '35:24')
        self.assertEqual(self.collector._parse_minutes('5:00'), '5:00')
        self.assertEqual(self.collector._parse_minutes('—'), '0:00')
        self.assertEqual(self.collector._parse_minutes(''), '0:00')
        self.assertEqual(self.collector._parse_minutes('25'), '25:00')
        self.assertEqual(self.collector._parse_minutes(None), '0:00')

    @patch('src.data_collectors.basketball_reference_collector.BasketballReferenceCollector._fetch_page')
    def test_collect_game_stats_no_game(self, mock_fetch):
        """Test collecting stats when game not found."""
        self.db_manager.get_game.return_value = None
        
        result = self.collector.collect_game_stats('0022300123')
        self.assertEqual(result, {'team_stats': [], 'player_stats': []})
        mock_fetch.assert_not_called()

    @patch('src.data_collectors.basketball_reference_collector.BasketballReferenceCollector._fetch_page')
    def test_collect_game_stats_no_team_abbrev(self, mock_fetch):
        """Test collecting stats when team abbreviation not found."""
        mock_game = Mock(spec=Game)
        mock_game.game_id = '0022300123'
        mock_game.home_team_id = '1610612747'
        mock_game.away_team_id = '1610612737'
        mock_game.game_date = date(2023, 10, 24)
        
        self.db_manager.get_game.return_value = mock_game
        self.db_manager.get_team.return_value = None  # Team not found
        
        result = self.collector.collect_game_stats('0022300123')
        self.assertEqual(result, {'team_stats': [], 'player_stats': []})
        mock_fetch.assert_not_called()

    def test_extract_team_stats_from_table(self):
        """Test extracting team stats from HTML table."""
        # Create a mock HTML table
        html = """
        <table class="sortable stats_table">
            <thead>
                <tr>
                    <th>Team</th>
                    <th>MP</th>
                    <th>FG</th>
                    <th>FGA</th>
                    <th>FG%</th>
                    <th>3P</th>
                    <th>3PA</th>
                    <th>3P%</th>
                    <th>FT</th>
                    <th>FTA</th>
                    <th>FT%</th>
                    <th>ORB</th>
                    <th>DRB</th>
                    <th>TRB</th>
                    <th>AST</th>
                    <th>STL</th>
                    <th>BLK</th>
                    <th>TOV</th>
                    <th>PF</th>
                    <th>PTS</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>Team Totals</td>
                    <td>240</td>
                    <td>42</td>
                    <td>90</td>
                    <td>.467</td>
                    <td>12</td>
                    <td>35</td>
                    <td>.343</td>
                    <td>18</td>
                    <td>22</td>
                    <td>.818</td>
                    <td>12</td>
                    <td>35</td>
                    <td>47</td>
                    <td>28</td>
                    <td>8</td>
                    <td>5</td>
                    <td>14</td>
                    <td>20</td>
                    <td>114</td>
                </tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        
        result = self.collector._extract_team_stats_from_table(
            table, '0022300123', '1610612747', True
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(result['game_id'], '0022300123')
        self.assertEqual(result['team_id'], '1610612747')
        self.assertEqual(result['is_home'], True)
        self.assertEqual(result['points'], 114)
        self.assertEqual(result['field_goals_made'], 42)
        self.assertEqual(result['field_goals_attempted'], 90)
        self.assertEqual(result['rebounds_total'], 47)
        self.assertEqual(result['assists'], 28)

    def test_extract_player_stats_from_table(self):
        """Test extracting player stats from HTML table."""
        # Create a mock HTML table
        html = """
        <table class="sortable stats_table">
            <thead>
                <tr>
                    <th>Player</th>
                    <th>MP</th>
                    <th>FG</th>
                    <th>FGA</th>
                    <th>3P</th>
                    <th>3PA</th>
                    <th>FT</th>
                    <th>FTA</th>
                    <th>TRB</th>
                    <th>AST</th>
                    <th>PTS</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><a href="/players/j/jamesle01.html">LeBron James</a></td>
                    <td>35:24</td>
                    <td>12</td>
                    <td>22</td>
                    <td>3</td>
                    <td>8</td>
                    <td>5</td>
                    <td>6</td>
                    <td>8</td>
                    <td>10</td>
                    <td>32</td>
                </tr>
            </tbody>
        </table>
        """
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        
        results = self.collector._extract_player_stats_from_table(
            table, '0022300123', '1610612747'
        )
        
        self.assertEqual(len(results), 1)
        player = results[0]
        self.assertEqual(player['game_id'], '0022300123')
        self.assertEqual(player['team_id'], '1610612747')
        self.assertEqual(player['player_name'], 'LeBron James')
        self.assertEqual(player['points'], 32)
        self.assertEqual(player['rebounds'], 8)
        self.assertEqual(player['assists'], 10)
        self.assertEqual(player['minutes_played'], '35:24')

    @patch('src.data_collectors.basketball_reference_collector.requests.Session.get')
    def test_fetch_page_success(self, mock_get):
        """Test successful page fetch."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'<html><body>Test</body></html>'
        mock_get.return_value = mock_response
        
        soup = self.collector._fetch_page('https://www.basketball-reference.com/boxscores/test.html')
        
        self.assertIsNotNone(soup)
        self.assertIsInstance(soup, BeautifulSoup)

    @patch('src.data_collectors.basketball_reference_collector.requests.Session.get')
    def test_fetch_page_failure(self, mock_get):
        """Test page fetch failure."""
        import requests
        mock_get.side_effect = requests.exceptions.RequestException("Connection error")
        
        soup = self.collector._fetch_page('https://www.basketball-reference.com/boxscores/test.html')
        
        self.assertIsNone(soup)


if __name__ == '__main__':
    unittest.main()
