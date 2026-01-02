"""Quick script to check if game IDs match between games and betting lines."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from datetime import date
from src.database.db_manager import DatabaseManager
from src.database.models import Game, BettingLine, Team

def main():
    db = DatabaseManager()
    today = date(2026, 1, 1)
    
    with db.get_session() as session:
        games = session.query(Game).filter(
            Game.game_date == today,
            Game.home_score.is_(None)
        ).all()
        
        print(f"\n{'='*70}")
        print(f"Checking Odds for Games on {today}")
        print(f"{'='*70}\n")
        
        for game in games:
            home_team = session.query(Team).filter_by(team_id=game.home_team_id).first()
            away_team = session.query(Team).filter_by(team_id=game.away_team_id).first()
            
            home_name = home_team.team_name if home_team else game.home_team_id
            away_name = away_team.team_name if away_team else game.away_team_id
            
            lines = session.query(BettingLine).filter_by(game_id=game.game_id).all()
            
            print(f"{away_name} @ {home_name}")
            print(f"  Game ID: {game.game_id}")
            print(f"  Betting Lines: {len(lines)}")
            
            if lines:
                # Show first line
                line = lines[0]
                print(f"  Sportsbook: {line.sportsbook}")
                print(f"  Moneyline: Home {line.moneyline_home}, Away {line.moneyline_away}")
                print(f"  Spread: Home {line.point_spread_home}, Away {line.point_spread_away}")
                print(f"  Over/Under: {line.over_under}")
            else:
                print(f"  [NO ODDS FOUND]")
                
                # Check if there are any lines with similar IDs
                all_lines = session.query(BettingLine).all()
                print(f"  Checking for similar game IDs in betting lines...")
                for bl in all_lines[:10]:  # Check first 10
                    if today.strftime('%Y%m%d') in bl.game_id:
                        print(f"    Found line with game_id: {bl.game_id}")
            print()

if __name__ == "__main__":
    main()



