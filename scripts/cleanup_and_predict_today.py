"""
Clean up incorrectly dated games and predict only today's actual games.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

from datetime import date
from src.database.db_manager import DatabaseManager
from src.prediction.prediction_service import PredictionService
from src.database.models import Game

def get_team_name(team_id: str, db: DatabaseManager) -> str:
    """Get team name from team ID."""
    team = db.get_team(team_id)
    if team:
        return f"{team.team_name} ({team.team_abbreviation})"
    return team_id

def main():
    """Clean up and predict today's games."""
    today = date.today()
    
    db = DatabaseManager()
    
    print("=" * 70)
    print(f"CLEANING UP AND PREDICTING GAMES FOR {today}")
    print("=" * 70)
    
    # Today's actual matchups (away @ home)
    # These are the 10 games for January 2, 2026
    todays_matchups = [
        ('Brooklyn Nets', 'Washington Wizards'),
        ('San Antonio Spurs', 'Indiana Pacers'),
        ('Denver Nuggets', 'Cleveland Cavaliers'),
        ('Atlanta Hawks', 'New York Knicks'),
        ('Orlando Magic', 'Chicago Bulls'),
        ('Charlotte Hornets', 'Milwaukee Bucks'),
        ('Portland Trail Blazers', 'New Orleans Pelicans'),
        ('Sacramento Kings', 'Phoenix Suns'),
        ('Oklahoma City Thunder', 'Golden State Warriors'),
        ('Memphis Grizzlies', 'Los Angeles Lakers'),
    ]
    
    # Step 1: Find games that match today's actual matchups
    print(f"\n[STEP 1] Finding today's actual games...")
    
    with db.get_session() as session:
        all_games = session.query(Game).filter(Game.game_date == today).all()
        print(f"  Total games dated {today}: {len(all_games)}")
        
        # Find matching games
        todays_games = []
        for game in all_games:
            away_team = db.get_team(game.away_team_id)
            home_team = db.get_team(game.home_team_id)
            
            if away_team and home_team:
                for away_name, home_name in todays_matchups:
                    if (away_name.lower() in away_team.team_name.lower() or 
                        away_team.team_name.lower() in away_name.lower()) and \
                       (home_name.lower() in home_team.team_name.lower() or 
                        home_team.team_name.lower() in home_name.lower()):
                        todays_games.append(game)
                        break
        
        print(f"  Found {len(todays_games)} matching games for today")
        
        # Step 2: Remove games that are not today's actual games
        if len(todays_games) < len(all_games):
            games_to_keep = [g.game_id for g in todays_games]
            games_to_remove = [g for g in all_games if g.game_id not in games_to_keep]
            
            print(f"\n[STEP 2] Removing {len(games_to_remove)} incorrectly dated games...")
            
            for game in games_to_remove:
                session.delete(game)
            
            session.commit()
            print(f"  [OK] Removed {len(games_to_remove)} games")
        
        # Step 3: Verify remaining games
        remaining_games = session.query(Game).filter(Game.game_date == today).all()
        print(f"\n[STEP 3] Remaining games for {today}: {len(remaining_games)}")
        
        for i, game in enumerate(remaining_games, 1):
            away_team = db.get_team(game.away_team_id)
            home_team = db.get_team(game.home_team_id)
            away_name = away_team.team_name if away_team else game.away_team_id
            home_name = home_team.team_name if home_team else game.home_team_id
            print(f"  {i}. {away_name} @ {home_name}")
    
    # Step 4: Generate features and predict
    print(f"\n[STEP 4] Generating predictions...")
    print("=" * 70)
    
    prediction_service = PredictionService(db)
    
    with db.get_session() as session:
        games = session.query(Game).filter(Game.game_date == today).order_by(Game.game_id).all()
    
    if not games:
        print(f"\nNo games found for {today}")
        return
    
    predictions = []
    for game in games:
        try:
            result = prediction_service.predict_game(
                game_id=game.game_id,
                model_name='nba_v2_classifier',
                reg_model_name='nba_v2_regressor'
            )
            
            if result:
                predictions.append((game, result))
                away_name = get_team_name(game.away_team_id, db)
                home_name = get_team_name(game.home_team_id, db)
                winner_name = get_team_name(result['predicted_winner'], db)
                
                print(f'\n[{len(predictions)}] {away_name} @ {home_name}')
                print(f'    Game ID: {game.game_id}')
                print(f'    Winner: {winner_name}')
                print(f'    Home Win Prob: {result["win_probability_home"]:.1%}')
                print(f'    Away Win Prob: {result["win_probability_away"]:.1%}')
                print(f'    Confidence: {result["confidence"]:.1%}')
                
                if result.get('predicted_point_differential') is not None:
                    diff = result['predicted_point_differential']
                    if diff > 0:
                        print(f'    Margin: {home_name} by {diff:.1f} pts')
                    elif diff < 0:
                        print(f'    Margin: {away_name} by {abs(diff):.1f} pts')
                    else:
                        print(f'    Margin: Tie')
            else:
                print(f'\nFailed to predict game {game.game_id}')
        except Exception as e:
            print(f'\nError predicting {game.game_id}: {e}')
            import traceback
            traceback.print_exc()
    
    print('\n' + '=' * 70)
    print(f'Total predictions: {len(predictions)}/{len(games)}')
    print('=' * 70)

if __name__ == '__main__':
    main()

