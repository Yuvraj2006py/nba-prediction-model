"""Unit tests for stats CSV export functionality."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import unittest
import tempfile
import csv
import shutil
from datetime import date, datetime
from unittest.mock import Mock, patch, MagicMock

from scripts.export_stats_to_csv import (
    export_team_stats_to_csv,
    export_player_stats_to_csv,
    export_all_stats_to_csv,
    _get_team_stats_headers,
    _get_player_stats_headers
)
from src.database.db_manager import DatabaseManager
from src.database.models import TeamStats, PlayerStats, Game, Team


class TestExportStatsToCSV(unittest.TestCase):
    """Test CSV export functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_dir_path = Path(self.temp_dir)
        self.db_manager = DatabaseManager()
    
    def tearDown(self):
        """Clean up test fixtures."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_get_team_stats_headers(self):
        """Test that team stats headers are correct."""
        headers = _get_team_stats_headers()
        
        # Check essential headers are present
        self.assertIn('game_id', headers)
        self.assertIn('game_date', headers)
        self.assertIn('season', headers)
        self.assertIn('team_id', headers)
        self.assertIn('points', headers)
        self.assertIn('field_goal_percentage', headers)
        self.assertIn('rebounds_total', headers)
        self.assertIn('assists', headers)
        
        # Check all expected headers
        expected_headers = [
            'game_id', 'game_date', 'season', 'team_id', 'team_name',
            'team_abbreviation', 'is_home', 'points', 'field_goals_made',
            'field_goals_attempted', 'field_goal_percentage'
        ]
        for header in expected_headers:
            self.assertIn(header, headers)
    
    def test_get_player_stats_headers(self):
        """Test that player stats headers are correct."""
        headers = _get_player_stats_headers()
        
        # Check essential headers are present
        self.assertIn('game_id', headers)
        self.assertIn('game_date', headers)
        self.assertIn('season', headers)
        self.assertIn('player_id', headers)
        self.assertIn('player_name', headers)
        self.assertIn('team_id', headers)
        self.assertIn('points', headers)
        self.assertIn('rebounds', headers)
        self.assertIn('assists', headers)
        
        # Check all expected headers
        expected_headers = [
            'game_id', 'game_date', 'season', 'player_id', 'player_name',
            'team_id', 'team_name', 'points', 'rebounds', 'assists'
        ]
        for header in expected_headers:
            self.assertIn(header, headers)
    
    def test_export_team_stats_empty_database(self):
        """Test exporting team stats when database is empty."""
        # Use a season that doesn't exist to test empty case
        csv_path = export_team_stats_to_csv(
            self.db_manager,
            season='2099-00',  # Future season that won't exist
            output_dir=self.temp_dir_path
        )
        
        self.assertTrue(csv_path.exists())
        
        # Check that CSV has headers but no data rows
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows = list(reader)
        
        self.assertIsNotNone(headers)
        self.assertEqual(len(rows), 0)
        self.assertIn('game_id', headers)
        self.assertIn('points', headers)
    
    def test_export_player_stats_empty_database(self):
        """Test exporting player stats when database is empty."""
        # Use a season that doesn't exist to test empty case
        csv_path = export_player_stats_to_csv(
            self.db_manager,
            season='2099-00',  # Future season that won't exist
            output_dir=self.temp_dir_path
        )
        
        self.assertTrue(csv_path.exists())
        
        # Check that CSV has headers but no data rows
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            rows = list(reader)
        
        self.assertIsNotNone(headers)
        self.assertEqual(len(rows), 0)
        self.assertIn('game_id', headers)
        self.assertIn('player_name', headers)
    
    def test_export_all_stats_empty_database(self):
        """Test exporting all stats when database is empty."""
        # Use a season that doesn't exist to test empty case
        result = export_all_stats_to_csv(
            self.db_manager,
            seasons=['2099-00'],  # Future season that won't exist
            output_dir=self.temp_dir_path
        )
        
        self.assertIn('team_stats_files', result)
        self.assertIn('player_stats_files', result)
        self.assertEqual(len(result['team_stats_files']), 1)
        self.assertEqual(len(result['player_stats_files']), 1)
        
        # Check files exist
        self.assertTrue(result['team_stats_files'][0].exists())
        self.assertTrue(result['player_stats_files'][0].exists())
        
        # Check files have headers but no data
        with open(result['team_stats_files'][0], 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            self.assertEqual(len(rows), 0)
    
    def test_export_team_stats_with_real_data(self):
        """Test exporting team stats with real data from database."""
        # Test with actual data if available
        csv_path = export_team_stats_to_csv(
            self.db_manager,
            season='2022-23',
            output_dir=self.temp_dir_path
        )
        
        self.assertTrue(csv_path.exists())
        
        # Verify CSV content
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Should have data if database has stats for 2022-23
        if len(rows) > 0:
            row = rows[0]
            # Verify required fields are present
            self.assertIn('game_id', row)
            self.assertIn('team_id', row)
            self.assertIn('team_name', row)
            self.assertIn('points', row)
            self.assertIn('field_goals_made', row)
            self.assertIn('field_goal_percentage', row)
            
            # Verify data types (points should be numeric)
            try:
                int(row['points'])
            except ValueError:
                self.fail("Points should be numeric")
    
    def test_export_player_stats_with_real_data(self):
        """Test exporting player stats with real data from database."""
        # Test with actual data if available
        csv_path = export_player_stats_to_csv(
            self.db_manager,
            season='2022-23',
            output_dir=self.temp_dir_path
        )
        
        self.assertTrue(csv_path.exists())
        
        # Verify CSV content
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Should have data if database has stats for 2022-23
        if len(rows) > 0:
            row = rows[0]
            # Verify required fields are present
            self.assertIn('game_id', row)
            self.assertIn('player_id', row)
            self.assertIn('player_name', row)
            self.assertIn('team_id', row)
            self.assertIn('points', row)
            self.assertIn('rebounds', row)
            self.assertIn('assists', row)
            
            # Verify data types (points should be numeric)
            try:
                int(row['points'])
            except ValueError:
                self.fail("Points should be numeric")
    
    def test_export_handles_none_values(self):
        """Test that None values are handled correctly in CSV."""
        # Test with real data - check that None values are exported as empty strings
        csv_path = export_team_stats_to_csv(
            self.db_manager,
            season='2022-23',
            output_dir=self.temp_dir_path
        )
        
        # Verify None values are exported as empty strings
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if len(rows) > 0:
            # Check that optional fields with None values are empty strings
            for row in rows[:10]:  # Check first 10 rows
                # These fields can be None, so they should be empty strings if None
                if 'offensive_rating' in row:
                    # If it's empty, it should be an empty string, not 'None'
                    if row['offensive_rating'] == '':
                        pass  # Correct
                    elif row['offensive_rating'] == 'None':
                        self.fail("None values should be exported as empty strings, not 'None'")
    
    def test_export_multiple_seasons(self):
        """Test exporting multiple seasons."""
        # Use seasons that might not exist to avoid dependency on actual data
        result = export_all_stats_to_csv(
            self.db_manager,
            seasons=['2098-99', '2099-00'],
            output_dir=self.temp_dir_path
        )
        
        self.assertEqual(len(result['team_stats_files']), 2)
        self.assertEqual(len(result['player_stats_files']), 2)
        
        # Check file names contain season
        team_files = [f.name for f in result['team_stats_files']]
        self.assertTrue(any('2098-99' in f for f in team_files))
        self.assertTrue(any('2099-00' in f for f in team_files))
    
    def test_csv_data_integrity(self):
        """Test that exported CSV data matches database."""
        # Export a season that has data
        csv_path = export_team_stats_to_csv(
            self.db_manager,
            season='2022-23',
            output_dir=self.temp_dir_path
        )
        
        if not csv_path.exists():
            self.skipTest("CSV file not created - no data in database")
        
        # Read CSV and verify against database
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            csv_rows = list(reader)
        
        if len(csv_rows) == 0:
            self.skipTest("No data in CSV - database may be empty")
        
        # Check first few rows match database
        sample_row = csv_rows[0]
        game_id = sample_row['game_id']
        team_id = sample_row['team_id']
        
        with self.db_manager.get_session() as session:
            db_stat = session.query(TeamStats).filter_by(
                game_id=game_id,
                team_id=team_id
            ).first()
        
        if db_stat:
            # Verify key fields match
            self.assertEqual(str(db_stat.points), sample_row['points'])
            self.assertEqual(str(db_stat.field_goals_made), sample_row['field_goals_made'])
            self.assertEqual(str(db_stat.assists), sample_row['assists'])
            self.assertEqual(str(db_stat.rebounds_total), sample_row['rebounds_total'])


if __name__ == '__main__':
    unittest.main()
