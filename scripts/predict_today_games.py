"""
Predict today's NBA games using the trained model.
Automatically uses today's date - no hardcoding needed.
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
    """Predict today's games."""
    today = date.today()
    today_str = today.strftime('%Y%m%d')  # e.g., "20260102"
    
    db = DatabaseManager()
    prediction_service = PredictionService(db)
    
    # Get games for today ONLY - filter by game_id starting with today's date
    with db.get_session() as session:
        games = session.query(Game).filter(
            Game.game_date == today
        ).order_by(Game.game_id).all()
    
    # Verify all games have today's date in their game_id
    verified_games = [g for g in games if g.game_id.startswith(today_str)]
    
    if not verified_games:
        print(f"\nNo games found for {today}")
        print("Run 'python scripts/fetch_today_games.py' first to fetch today's games.")
        return
    
    print(f'\nPredicting {len(verified_games)} games for {today}')
    print('=' * 70)
    
    predictions = []
    for game in verified_games:
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
    print(f'Total predictions: {len(predictions)}/{len(verified_games)}')
    print('=' * 70)

if __name__ == '__main__':
    main()
