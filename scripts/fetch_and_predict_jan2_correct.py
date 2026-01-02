"""
Fetch January 2nd games (from API Jan 3 UTC) and make predictions.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from datetime import date, datetime, timedelta
from src.database.db_manager import DatabaseManager
from src.backtesting.team_mapper import TeamMapper
from src.data_collectors.betting_odds_collector import BettingOddsCollector
from src.prediction.prediction_service import PredictionService
from src.database.models import Game

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_team_name(team_id: str, db: DatabaseManager) -> str:
    """Get team name from team ID."""
    team = db.get_team(team_id)
    if team:
        return f"{team.team_name} ({team.team_abbreviation})"
    return team_id


def main():
    """Fetch Jan 2 games and make predictions."""
    # Target date is Jan 2, 2026 in US time
    target_date_us = date(2026, 1, 2)
    # But API returns Jan 3 UTC (which is Jan 2 US evening)
    target_date_api = date(2026, 1, 3)
    
    print("=" * 70)
    print(f"FETCHING AND PREDICTING GAMES FOR JANUARY 2, 2026 (US TIME)")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    team_mapper = TeamMapper(db_manager)
    odds_collector = BettingOddsCollector(db_manager)
    
    # Step 1: Fetch games from API (they'll be dated Jan 3 UTC)
    print(f"\n[STEP 1] Fetching games from API...")
    all_odds = odds_collector.get_nba_odds()
    print(f"[INFO] API returned {len(all_odds)} total games")
    
    # Filter for games that are Jan 2 in US time (Jan 3 early morning UTC)
    jan2_games = []
    for game_odds in all_odds:
        commence_time = game_odds.get('commence_time', '')
        if commence_time:
            try:
                if 'T' in commence_time:
                    if commence_time.endswith('Z'):
                        game_datetime_utc = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                    else:
                        game_datetime_utc = datetime.fromisoformat(commence_time)
                    
                    game_date_utc = game_datetime_utc.date()
                    hour_utc = game_datetime_utc.hour
                    
                    # Games on Jan 3 UTC before 6 AM are likely Jan 2 US evening
                    if game_date_utc == target_date_api and hour_utc < 6:
                        jan2_games.append(game_odds)
                        home = game_odds.get('home_team', 'Unknown')
                        away = game_odds.get('away_team', 'Unknown')
                        print(f"  [FOUND] {away} @ {home} - UTC: {game_datetime_utc}")
            except Exception as e:
                logger.debug(f"Error parsing: {e}")
    
    if jan2_games:
        print(f"\n[OK] Found {len(jan2_games)} games for Jan 2 (US time)")
        stored_count = odds_collector.parse_and_store_odds(jan2_games)
        print(f"[OK] Stored {stored_count} betting lines")
    else:
        print(f"\n[WARNING] No games found for Jan 2")
        print("  Using existing games in database...")
    
    # Step 2: Verify/update game dates in database
    print(f"\n[STEP 2] Verifying games for {target_date_us}...")
    with db_manager.get_session() as session:
        games = session.query(Game).filter(
            Game.game_date == target_date_us
        ).all()
        
        print(f"Found {len(games)} games in database for {target_date_us}")
        
        # If we have games dated Jan 3, update them to Jan 2
        games_jan3 = session.query(Game).filter(
            Game.game_date == target_date_api
        ).all()
        
        if games_jan3:
            print(f"Found {len(games_jan3)} games dated {target_date_api}, updating to {target_date_us}...")
            for game in games_jan3:
                game.game_date = target_date_us
            session.commit()
            print(f"[OK] Updated {len(games_jan3)} games to {target_date_us}")
    
    # Step 3: Generate features
    print(f"\n[STEP 3] Generating features...")
    from scripts.transform_features import FeatureTransformer
    transformer = FeatureTransformer(season='2025-26', db_manager=db_manager)
    transformer.run(full_refresh=False)
    
    # Step 4: Make predictions
    print(f"\n[STEP 4] Making predictions...")
    prediction_service = PredictionService(db_manager)
    
    with db_manager.get_session() as session:
        games = session.query(Game).filter(
            Game.game_date == target_date_us
        ).all()
    
    print(f"\nPredicting {len(games)} games for {target_date_us}\n")
    print('=' * 70)
    
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
                away_name = get_team_name(game.away_team_id, db_manager)
                home_name = get_team_name(game.home_team_id, db_manager)
                winner_name = get_team_name(result['predicted_winner'], db_manager)
                
                print(f'\n[{len(predictions)}] {away_name} @ {home_name}')
                print(f'    Game ID: {game.game_id}')
                print(f'    Date: {game.game_date}')
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

