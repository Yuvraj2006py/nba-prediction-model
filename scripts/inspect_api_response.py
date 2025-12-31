"""Inspect actual NBA API response structure to understand data format."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_collectors.nba_api_collector import NBAPICollector
from src.database.db_manager import DatabaseManager
from src.database.models import Game
from nba_api.stats.endpoints import BoxScoreTraditionalV3
import json

def inspect_api_response():
    """Inspect a sample API response to see what fields are available."""
    print("=" * 70)
    print("NBA API Response Structure Inspection")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    collector = NBAPICollector(db_manager)
    
    # Get a sample game from 2022-23 season
    with db_manager.get_session() as session:
        game = session.query(Game).filter(
            Game.season == '2022-23',
            Game.game_status == 'finished'
        ).order_by(Game.game_date).first()
    
    if not game:
        print("No games found in database")
        return
    
    print(f"\nInspecting game: {game.game_id} ({game.game_date})")
    print(f"Teams: {game.away_team_id} @ {game.home_team_id}")
    
    try:
        # Get the actual API response
        collector._rate_limit()
        boxscore = BoxScoreTraditionalV3(game_id=game.game_id)
        boxscore_data = boxscore.get_dict()
        
        if 'boxScoreTraditional' not in boxscore_data:
            print("No boxScoreTraditional in response")
            return
        
        bst = boxscore_data['boxScoreTraditional']
        
        print(f"\nResponse structure:")
        print(f"  Top-level keys: {list(bst.keys())}")
        
        # Check home team stats
        if 'homeTeam' in bst:
            home_team = bst['homeTeam']
            print(f"\n  Home team keys: {list(home_team.keys())}")
            
            if 'statistics' in home_team:
                stats = home_team['statistics']
                print(f"\n  Home team statistics keys: {list(stats.keys())}")
                
                # Check for rebound-related fields
                rebound_keys = [k for k in stats.keys() if 'rebound' in k.lower() or 'reb' in k.lower()]
                print(f"\n  Rebound-related keys: {rebound_keys}")
                
                if rebound_keys:
                    for key in rebound_keys:
                        print(f"    {key}: {stats.get(key)}")
                else:
                    print(f"  [WARNING] No rebound-related keys found!")
                    print(f"  All available keys: {list(stats.keys())}")
                    
                    # Show sample of all stats
                    print(f"\n  Sample statistics (first 20):")
                    for i, (key, value) in enumerate(list(stats.items())[:20]):
                        print(f"    {key}: {value}")
        
        # Check away team stats
        if 'awayTeam' in bst:
            away_team = bst['awayTeam']
            if 'statistics' in away_team:
                stats = away_team['statistics']
                rebound_keys = [k for k in stats.keys() if 'rebound' in k.lower() or 'reb' in k.lower()]
                if rebound_keys:
                    print(f"\n  Away team rebound keys: {rebound_keys}")
                    for key in rebound_keys:
                        print(f"    {key}: {stats.get(key)}")
        
        # Also check player stats structure
        if 'homeTeam' in bst and 'players' in bst['homeTeam']:
            players = bst['homeTeam']['players']
            if players:
                first_player = players[0]
                if 'statistics' in first_player:
                    player_stats = first_player['statistics']
                    rebound_keys = [k for k in player_stats.keys() if 'rebound' in k.lower() or 'reb' in k.lower()]
                    if rebound_keys:
                        print(f"\n  Player rebound keys: {rebound_keys}")
                        print(f"    Sample player stats: {first_player.get('firstName', '')} {first_player.get('lastName', '')}")
                        for key in rebound_keys:
                            print(f"      {key}: {player_stats.get(key)}")
        
    except Exception as e:
        print(f"Error inspecting API response: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    inspect_api_response()
