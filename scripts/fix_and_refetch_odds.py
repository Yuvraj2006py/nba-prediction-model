"""Fix odds by deleting incorrect betting lines and re-fetching with corrected logic."""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from datetime import date
from src.database.db_manager import DatabaseManager
from src.database.models import Game, BettingLine
from src.data_collectors.betting_odds_collector import BettingOddsCollector

def fix_odds_for_date(target_date: date):
    """Delete incorrect betting lines and re-fetch with corrected logic."""
    db = DatabaseManager()
    collector = BettingOddsCollector(db)
    
    print(f"=" * 80)
    print(f"FIXING ODDS FOR {target_date}")
    print(f"=" * 80)
    
    with db.get_session() as session:
        # Get all games for the date
        games = session.query(Game).filter(Game.game_date == target_date).all()
        print(f"\nFound {len(games)} games for {target_date}")
        
        # Delete all existing betting lines for these games
        deleted_count = 0
        for game in games:
            lines = session.query(BettingLine).filter(BettingLine.game_id == game.game_id).all()
            for line in lines:
                session.delete(line)
                deleted_count += 1
        
        session.commit()
        print(f"Deleted {deleted_count} existing betting lines")
    
    # Re-fetch odds with corrected logic
    print(f"\nRe-fetching odds with corrected team matching...")
    odds_data = collector.get_odds_for_date(target_date)
    
    if odds_data:
        stored = collector.parse_and_store_odds(odds_data, preferred_sportsbook='draftkings')
        print(f"Stored {stored} betting lines with corrected odds")
        
        # Verify the odds are correct
        print(f"\nVerifying odds...")
        with db.get_session() as session:
            for game in games:
                bl = session.query(BettingLine).filter(
                    BettingLine.game_id == game.game_id,
                    BettingLine.sportsbook == 'draftkings'
                ).order_by(BettingLine.created_at.desc()).first()
                
                if bl:
                    from src.database.models import Team
                    home_team = session.query(Team).filter(Team.team_id == game.home_team_id).first()
                    away_team = session.query(Team).filter(Team.team_id == game.away_team_id).first()
                    
                    print(f"  {away_team.team_name if away_team else 'AWAY'} @ {home_team.team_name if home_team else 'HOME'}")
                    print(f"    Home: {bl.moneyline_home}, Away: {bl.moneyline_away}")
    else:
        print("No odds data available from API")

if __name__ == '__main__':
    today = date.today()
    fix_odds_for_date(today)

