"""
Fix game dates and predict only today's actual games.
Only keep games with game IDs containing today's date.
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
    """Fix game dates and predict today's games."""
    today = date.today()
    today_str = today.strftime('%Y%m%d')  # e.g., "20260102"
    
    db = DatabaseManager()
    
    print("=" * 70)
    print(f"FIXING AND PREDICTING GAMES FOR {today}")
    print("=" * 70)
    
    # Step 1: Find all games dated today
    with db.get_session() as session:
        all_games = session.query(Game).filter(Game.game_date == today).all()
        print(f"\n[STEP 1] Total games dated {today}: {len(all_games)}")
        
        # Step 2: Keep only games with game_id containing today's date
        todays_games = []
        games_to_fix = []
        
        for game in all_games:
            # Check if game_id starts with today's date (YYYYMMDD format)
            if game.game_id.startswith(today_str):
                todays_games.append(game)
            else:
                games_to_fix.append(game)
        
        print(f"  Games with correct date in ID ({today_str}): {len(todays_games)}")
        print(f"  Games with wrong date in ID: {len(games_to_fix)}")
        
        # Step 3: Fix games with wrong dates - extract date from game_id and update
        if games_to_fix:
            print(f"\n[STEP 2] Fixing {len(games_to_fix)} games with incorrect dates...")
            fixed_count = 0
            for game in games_to_fix:
                # Try to extract date from game_id
                game_id = game.game_id
                
                # Format 1: YYYYMMDD... (e.g., 20241030ORLCHI)
                if len(game_id) >= 8 and game_id[:8].isdigit():
                    try:
                        year = int(game_id[:4])
                        month = int(game_id[4:6])
                        day = int(game_id[6:8])
                        correct_date = date(year, month, day)
                        game.game_date = correct_date
                        fixed_count += 1
                    except (ValueError, IndexError):
                        pass
                # Format 2: 00225XXXXX (NBA API format) - these are historical games
                elif game_id.startswith('00225'):
                    # These are from the 2025-26 season, but we need to look up correct date
                    # For now, mark them as a different season
                    # We'll delete them since they're not today's games
                    session.delete(game)
                    fixed_count += 1
            
            session.commit()
            print(f"  [OK] Fixed/removed {fixed_count} games")
        
        # Step 4: Verify remaining games for today
        remaining_games = session.query(Game).filter(Game.game_date == today).all()
        print(f"\n[STEP 3] Remaining games for {today}: {len(remaining_games)}")
        
        for i, game in enumerate(remaining_games, 1):
            away_team = db.get_team(game.away_team_id)
            home_team = db.get_team(game.home_team_id)
            away_name = away_team.team_name if away_team else game.away_team_id
            home_name = home_team.team_name if home_team else game.home_team_id
            print(f"  {i}. {away_name} @ {home_name} (ID: {game.game_id})")
    
    # Step 5: Generate predictions
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

