"""Unit tests for Basketball Reference CSV collection script."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import unittest
import tempfile
import csv
from unittest.mock import Mock, patch, MagicMock
from datetime import date

from scripts.collect_seasons_csv_bball_ref import (
    parse_schedule_csv,
    import_csv_games_to_db,
    TEAM_DATA,
    SEASONS
)


class TestParseScheduleCSV(unittest.TestCase):
    """Test CSV parsing functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_dir_path = Path(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def create_test_csv(self, filename: str, rows: list, header: list = None):
        """Helper to create a test CSV file."""
        csv_path = self.temp_dir_path / filename
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if header:
                writer.writerow(header)
            writer.writerows(rows)
        return csv_path
    
    def test_parse_csv_with_valid_data(self):
        """Test parsing a valid CSV file."""
        header = ['Date', 'Visitor/Neutral', 'Visitor PTS', 'Home/Neutral', 'Home PTS']
        rows = [
            ['Tue, Oct 18, 2022', 'BOS', '126', 'PHI', '117'],
            ['Wed, Oct 19, 2022', 'LAL', '109', 'GSW', '123']
        ]
        
        csv_path = self.create_test_csv('test.csv', rows, header)
        games = parse_schedule_csv(csv_path, '2022-23')
        
        self.assertEqual(len(games), 2)
        self.assertEqual(games[0]['season'], '2022-23')
        self.assertEqual(games[0]['away_team_id'], '1610612738')  # BOS
        self.assertEqual(games[0]['home_team_id'], '1610612755')  # PHI
        self.assertEqual(games[0]['away_score'], 126)
        self.assertEqual(games[0]['home_score'], 117)
    
    def test_parse_csv_with_different_date_formats(self):
        """Test parsing CSV with different date formats."""
        header = ['Date', 'Visitor/Neutral', 'Visitor PTS', 'Home/Neutral', 'Home PTS']
        rows = [
            ['Oct 18, 2022', 'BOS', '126', 'PHI', '117'],
            ['2022-10-19', 'LAL', '109', 'GSW', '123'],
            ['10/20/2022', 'MIA', '98', 'BOS', '111']
        ]
        
        csv_path = self.create_test_csv('test.csv', rows, header)
        games = parse_schedule_csv(csv_path, '2022-23')
        
        self.assertEqual(len(games), 3)
        for game in games:
            self.assertIsInstance(game['game_date'], date)
    
    def test_parse_csv_with_team_abbreviations(self):
        """Test parsing CSV with team abbreviations."""
        header = ['Date', 'Visitor/Neutral', 'Visitor PTS', 'Home/Neutral', 'Home PTS']
        rows = [
            ['Tue, Oct 18, 2022', 'BOS', '126', 'PHI', '117'],
            ['Wed, Oct 19, 2022', 'LAL', '109', 'GSW', '123']
        ]
        
        csv_path = self.create_test_csv('test.csv', rows, header)
        games = parse_schedule_csv(csv_path, '2022-23')
        
        # Verify team IDs are correct
        self.assertEqual(games[0]['away_team_id'], '1610612738')  # BOS
        self.assertEqual(games[0]['home_team_id'], '1610612755')  # PHI
        self.assertEqual(games[1]['away_team_id'], '1610612747')  # LAL
        self.assertEqual(games[1]['home_team_id'], '1610612744')  # GSW
    
    def test_parse_csv_with_missing_scores(self):
        """Test parsing CSV with missing scores (scheduled games)."""
        header = ['Date', 'Visitor/Neutral', 'Visitor PTS', 'Home/Neutral', 'Home PTS']
        rows = [
            ['Tue, Oct 18, 2022', 'BOS', '126', 'PHI', '117'],
            ['Wed, Oct 19, 2022', 'LAL', '', 'GSW', '']
        ]
        
        csv_path = self.create_test_csv('test.csv', rows, header)
        games = parse_schedule_csv(csv_path, '2022-23')
        
        self.assertEqual(len(games), 2)
        self.assertEqual(games[0]['game_status'], 'finished')
        # Second game should still be parsed but with None scores
        self.assertIsNotNone(games[1]['game_date'])
    
    def test_parse_csv_with_empty_rows(self):
        """Test parsing CSV with empty rows."""
        header = ['Date', 'Visitor/Neutral', 'Visitor PTS', 'Home/Neutral', 'Home PTS']
        rows = [
            ['Tue, Oct 18, 2022', 'BOS', '126', 'PHI', '117'],
            ['', '', '', '', ''],
            ['Wed, Oct 19, 2022', 'LAL', '109', 'GSW', '123']
        ]
        
        csv_path = self.create_test_csv('test.csv', rows, header)
        games = parse_schedule_csv(csv_path, '2022-23')
        
        # Should skip empty row
        self.assertEqual(len(games), 2)
    
    def test_parse_csv_generates_correct_game_ids(self):
        """Test that game IDs are generated correctly."""
        header = ['Date', 'Visitor/Neutral', 'Visitor PTS', 'Home/Neutral', 'Home PTS']
        rows = [
            ['Tue, Oct 18, 2022', 'BOS', '126', 'PHI', '117']
        ]
        
        csv_path = self.create_test_csv('test.csv', rows, header)
        games = parse_schedule_csv(csv_path, '2022-23')
        
        expected_game_id = '20221018BOSPHI'
        self.assertEqual(games[0]['game_id'], expected_game_id)
    
    def test_parse_csv_all_teams_recognized(self):
        """Test that all teams in TEAM_DATA can be parsed."""
        header = ['Date', 'Visitor/Neutral', 'Visitor PTS', 'Home/Neutral', 'Home PTS']
        
        # Create rows for all teams
        rows = []
        team_abbrevs = list(TEAM_DATA.keys())
        for i, away_abbrev in enumerate(team_abbrevs[:5]):  # Test first 5 teams
            home_abbrev = team_abbrevs[(i + 1) % len(team_abbrevs)]
            rows.append([f'Tue, Oct {18 + i}, 2022', away_abbrev, '100', home_abbrev, '110'])
        
        csv_path = self.create_test_csv('test.csv', rows, header)
        games = parse_schedule_csv(csv_path, '2022-23')
        
        self.assertEqual(len(games), 5)
        for game in games:
            self.assertIn(game['away_team_id'], [data[0] for data in TEAM_DATA.values()])
            self.assertIn(game['home_team_id'], [data[0] for data in TEAM_DATA.values()])


class TestImportCSVGamesToDB(unittest.TestCase):
    """Test database import functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_db_manager = Mock()
        self.mock_db_manager.get_game = Mock(return_value=None)
        self.mock_db_manager.insert_game = Mock()
        self.mock_db_manager.get_session = Mock()
    
    def test_import_new_games(self):
        """Test importing new games."""
        games = [
            {
                'game_id': '20221018BOSPHI',
                'season': '2022-23',
                'game_date': date(2022, 10, 18),
                'away_team_id': '1610612738',
                'home_team_id': '1610612755',
                'away_score': 126,
                'home_score': 117
            }
        ]
        
        self.mock_db_manager.get_game.return_value = None
        
        stats = import_csv_games_to_db(games, self.mock_db_manager, replace_existing=True)
        
        self.assertEqual(stats['games_imported'], 1)
        self.assertEqual(stats['games_updated'], 0)
        self.assertEqual(stats['games_skipped'], 0)
        self.assertEqual(stats['errors'], 0)
        self.mock_db_manager.insert_game.assert_called_once()
    
    def test_import_updates_existing_games(self):
        """Test updating existing games."""
        existing_game = Mock()
        existing_game.game_id = '20221018BOSPHI'
        
        games = [
            {
                'game_id': '20221018BOSPHI',
                'season': '2022-23',
                'game_date': date(2022, 10, 18),
                'away_team_id': '1610612738',
                'home_team_id': '1610612755',
                'away_score': 126,
                'home_score': 117
            }
        ]
        
        self.mock_db_manager.get_game.return_value = existing_game
        mock_session = MagicMock()
        self.mock_db_manager.get_session.return_value.__enter__ = Mock(return_value=mock_session)
        self.mock_db_manager.get_session.return_value.__exit__ = Mock(return_value=None)
        
        stats = import_csv_games_to_db(games, self.mock_db_manager, replace_existing=True)
        
        self.assertEqual(stats['games_imported'], 0)
        self.assertEqual(stats['games_updated'], 1)
        self.assertEqual(stats['games_skipped'], 0)
        mock_session.merge.assert_called_once()
        mock_session.commit.assert_called_once()
    
    def test_import_skips_existing_games_when_not_replacing(self):
        """Test skipping existing games when replace_existing=False."""
        existing_game = Mock()
        existing_game.game_id = '20221018BOSPHI'
        
        games = [
            {
                'game_id': '20221018BOSPHI',
                'season': '2022-23',
                'game_date': date(2022, 10, 18),
                'away_team_id': '1610612738',
                'home_team_id': '1610612755',
                'away_score': 126,
                'home_score': 117
            }
        ]
        
        self.mock_db_manager.get_game.return_value = existing_game
        
        stats = import_csv_games_to_db(games, self.mock_db_manager, replace_existing=False)
        
        self.assertEqual(stats['games_imported'], 0)
        self.assertEqual(stats['games_updated'], 0)
        self.assertEqual(stats['games_skipped'], 1)
        self.mock_db_manager.insert_game.assert_not_called()
    
    def test_import_handles_errors_gracefully(self):
        """Test that import handles errors gracefully."""
        games = [
            {
                'game_id': '20221018BOSPHI',
                'season': '2022-23',
                'game_date': date(2022, 10, 18),
                'away_team_id': '1610612738',
                'home_team_id': '1610612755',
                'away_score': 126,
                'home_score': 117
            }
        ]
        
        self.mock_db_manager.get_game.side_effect = Exception("Database error")
        
        stats = import_csv_games_to_db(games, self.mock_db_manager, replace_existing=True)
        
        self.assertEqual(stats['errors'], 1)
        self.assertEqual(stats['games_imported'], 0)


class TestSeasonsConfiguration(unittest.TestCase):
    """Test season configuration."""
    
    def test_seasons_list_contains_expected_seasons(self):
        """Test that SEASONS list contains expected values."""
        expected_seasons = ['2022-23', '2023-24', '2024-25']
        self.assertEqual(SEASONS, expected_seasons)
    
    def test_all_seasons_have_correct_format(self):
        """Test that all seasons have the correct format."""
        for season in SEASONS:
            parts = season.split('-')
            self.assertEqual(len(parts), 2)
            self.assertTrue(parts[0].isdigit())
            self.assertTrue(parts[1].isdigit())
            # Second part should be 2 digits
            self.assertEqual(len(parts[1]), 2)


class TestTeamData(unittest.TestCase):
    """Test team data configuration."""
    
    def test_all_teams_have_required_fields(self):
        """Test that all teams have required fields."""
        for abbrev, team_data in TEAM_DATA.items():
            self.assertEqual(len(team_data), 5, f"Team {abbrev} should have 5 fields")
            team_id, team_name, city, conference, division = team_data
            
            # Validate fields
            self.assertTrue(team_id.startswith('161061'), f"Team ID should start with 161061: {team_id}")
            self.assertIsInstance(team_name, str)
            self.assertIsInstance(city, str)
            self.assertIn(conference, ['Eastern', 'Western'])
            self.assertIsInstance(division, str)
    
    def test_team_abbreviations_are_three_letters(self):
        """Test that all team abbreviations are 3 letters."""
        for abbrev in TEAM_DATA.keys():
            self.assertEqual(len(abbrev), 3, f"Abbreviation {abbrev} should be 3 letters")
            self.assertTrue(abbrev.isupper(), f"Abbreviation {abbrev} should be uppercase")
    
    def test_team_ids_are_unique(self):
        """Test that all team IDs are unique."""
        team_ids = [data[0] for data in TEAM_DATA.values()]
        self.assertEqual(len(team_ids), len(set(team_ids)), "All team IDs should be unique")
    
    def test_expected_number_of_teams(self):
        """Test that we have the expected number of teams (30 NBA teams)."""
        self.assertEqual(len(TEAM_DATA), 30, "Should have 30 NBA teams")


if __name__ == '__main__':
    unittest.main()
