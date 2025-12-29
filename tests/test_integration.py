"""Integration tests for data collection pipeline."""

import unittest
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.db_manager import DatabaseManager
from src.data_collectors.nba_api_collector import NBAPICollector
from src.data_collectors.betting_odds_collector import BettingOddsCollector


class TestIntegration(unittest.TestCase):
    """Integration tests for the data collection pipeline."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for the class."""
        cls.db_manager = DatabaseManager()
        cls.nba_collector = NBAPICollector(db_manager=cls.db_manager)
        cls.betting_collector = BettingOddsCollector(db_manager=cls.db_manager)
    
    def test_nba_collector_team_stats_collection(self):
        """Test that team stats can be collected for a real game."""
        # Use a known game ID from the POC (finished game)
        game_id = '0022401199'
        
        # Collect team stats
        team_stats = self.nba_collector.collect_team_stats(game_id)
        
        # Verify we got stats
        self.assertIsNotNone(team_stats)
        self.assertGreater(len(team_stats), 0, "Should have at least one team's stats")
        
        # Verify stats structure
        for stats in team_stats:
            self.assertIn('game_id', stats)
            self.assertIn('team_id', stats)
            self.assertIn('points', stats)
            self.assertIn('field_goals_made', stats)
            self.assertIn('rebounds_total', stats)
            self.assertEqual(stats['game_id'], game_id)
            self.assertGreater(stats['points'], 0, "Team should have scored points")
    
    def test_nba_collector_player_stats_collection(self):
        """Test that player stats can be collected for a real game."""
        # Use a known game ID from the POC (finished game)
        game_id = '0022401199'
        
        # Collect player stats
        player_stats = self.nba_collector.collect_player_stats(game_id)
        
        # Verify we got stats
        self.assertIsNotNone(player_stats)
        self.assertGreater(len(player_stats), 0, "Should have at least one player's stats")
        
        # Verify stats structure
        for stats in player_stats[:5]:  # Check first 5 players
            self.assertIn('game_id', stats)
            self.assertIn('player_id', stats)
            self.assertIn('team_id', stats)
            self.assertIn('player_name', stats)
            self.assertIn('points', stats)
            self.assertEqual(stats['game_id'], game_id)
    
    def test_betting_collector_api_connection(self):
        """Test that betting collector can connect to API."""
        # Test getting sports
        sports = self.betting_collector.get_sports()
        self.assertIsNotNone(sports)
        self.assertGreater(len(sports), 0, "Should have at least one sport")
        
        # Verify NBA is in the list
        nba_sport = next((s for s in sports if 'nba' in s.get('key', '').lower()), None)
        self.assertIsNotNone(nba_sport, "NBA sport should be available")
    
    def test_betting_collector_odds_fetch(self):
        """Test that betting collector can fetch NBA odds."""
        # Fetch NBA odds
        odds = self.betting_collector.get_nba_odds()
        
        # Should get some odds (may be empty if no games, but API should work)
        self.assertIsNotNone(odds)
        # Note: We don't assert length > 0 because there may be no games scheduled
    
    def test_end_to_end_data_collection(self):
        """Test end-to-end: collect game, team stats, and player stats."""
        # Use a known finished game
        game_id = '0022401199'
        
        # Step 1: Get game details
        game_details = self.nba_collector.get_game_details(game_id)
        self.assertIsNotNone(game_details, "Should get game details")
        self.assertEqual(game_details['game_id'], game_id)
        
        # Step 2: Collect team stats
        team_stats = self.nba_collector.collect_team_stats(game_id)
        self.assertGreater(len(team_stats), 0, "Should collect team stats")
        
        # Step 3: Collect player stats
        player_stats = self.nba_collector.collect_player_stats(game_id)
        self.assertGreater(len(player_stats), 0, "Should collect player stats")
        
        # Verify data consistency
        # Team stats should have 2 entries (home and away)
        self.assertEqual(len(team_stats), 2, "Should have stats for both teams")
        
        # Player stats should have multiple entries
        self.assertGreater(len(player_stats), 10, "Should have stats for multiple players")


if __name__ == '__main__':
    unittest.main()

