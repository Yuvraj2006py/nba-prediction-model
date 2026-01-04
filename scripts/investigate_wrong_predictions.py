#!/usr/bin/env python
"""
Investigate incorrect predictions in detail.
Shows which games were wrong, why, and patterns.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import argparse
from datetime import date, timedelta
from collections import defaultdict
from src.database.db_manager import DatabaseManager
from src.database.models import Game, Prediction, TeamRollingFeatures

def investigate_incorrect_predictions(model_name: str, days: int = 30):
    """Investigate incorrect predictions in detail."""
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    db = DatabaseManager()
    
    print("=" * 80)
    print(f"INVESTIGATING INCORRECT PREDICTIONS - {model_name}")
    print("=" * 80)
    print(f"Date range: {start_date} to {end_date}")
    print("=" * 80)
    
    with db.get_session() as session:
        games = session.query(Game).join(Prediction).filter(
            Game.game_date >= start_date,
            Game.game_date <= end_date,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            Prediction.model_name == model_name
        ).order_by(Game.game_date.desc()).all()
        
        if not games:
            print(f"\nNo games found with predictions and results")
            return
        
        print(f"\nAnalyzing {len(games)} games...\n")
        
        correct_games = []
        incorrect_games = []
        
        for game in games:
            prediction = session.query(Prediction).filter_by(
                game_id=game.game_id,
                model_name=model_name
            ).first()
            
            if not prediction:
                continue
            
            # Get team names
            home_team = db.get_team(game.home_team_id)
            away_team = db.get_team(game.away_team_id)
            actual_winner_team = db.get_team(game.winner) if game.winner else None
            predicted_winner_team = db.get_team(prediction.predicted_winner) if prediction.predicted_winner else None
            
            home_name = home_team.team_name if home_team else game.home_team_id
            away_name = away_team.team_name if away_team else game.away_team_id
            actual_winner_name = actual_winner_team.team_name if actual_winner_team else (game.winner or "Unknown")
            predicted_winner_name = predicted_winner_team.team_name if predicted_winner_team else (prediction.predicted_winner or "Unknown")
            
            actual_winner = game.winner
            predicted_winner = prediction.predicted_winner
            is_correct = actual_winner == predicted_winner
            
            # Calculate margins
            actual_margin = game.home_score - game.away_score
            predicted_margin = prediction.predicted_point_differential if prediction.predicted_point_differential else 0
            
            game_info = {
                'game_id': game.game_id,
                'game_date': game.game_date,
                'home_team_id': game.home_team_id,
                'away_team_id': game.away_team_id,
                'home_name': home_name,
                'away_name': away_name,
                'home_score': game.home_score,
                'away_score': game.away_score,
                'actual_winner': actual_winner,
                'actual_winner_name': actual_winner_name,
                'predicted_winner': predicted_winner,
                'predicted_winner_name': predicted_winner_name,
                'confidence': prediction.confidence,
                'home_prob': prediction.win_probability_home,
                'away_prob': prediction.win_probability_away,
                'predicted_margin': predicted_margin,
                'actual_margin': actual_margin,
                'margin_error': abs(predicted_margin - actual_margin) if predicted_margin else None
            }
            
            if is_correct:
                correct_games.append(game_info)
            else:
                incorrect_games.append(game_info)
        
        # Summary
        total = len(correct_games) + len(incorrect_games)
        accuracy = len(correct_games) / total if total > 0 else 0
        
        print(f"SUMMARY:")
        print(f"  Total games: {total}")
        print(f"  Correct: {len(correct_games)} ({len(correct_games)/total:.1%})")
        print(f"  Incorrect: {len(incorrect_games)} ({len(incorrect_games)/total:.1%})")
        print(f"  Overall accuracy: {accuracy:.1%}")
        print()
        
        # Group incorrect by confidence
        if incorrect_games:
            print("=" * 80)
            print("INCORRECT PREDICTIONS BY CONFIDENCE LEVEL")
            print("=" * 80)
            
            confidence_bins = {
                'Low (50-60%)': [],
                'Medium (60-70%)': [],
                'High (70-80%)': [],
                'Very High (80-90%)': [],
                'Extreme (90-100%)': []
            }
            
            for game in incorrect_games:
                conf = game['confidence']
                if conf < 0.60:
                    confidence_bins['Low (50-60%)'].append(game)
                elif conf < 0.70:
                    confidence_bins['Medium (60-70%)'].append(game)
                elif conf < 0.80:
                    confidence_bins['High (70-80%)'].append(game)
                elif conf < 0.90:
                    confidence_bins['Very High (80-90%)'].append(game)
                else:
                    confidence_bins['Extreme (90-100%)'].append(game)
            
            for bin_name, games_in_bin in confidence_bins.items():
                if games_in_bin:
                    print(f"\n{bin_name}: {len(games_in_bin)} games")
                    print("-" * 80)
                    for game in sorted(games_in_bin, key=lambda x: x['confidence'], reverse=True):
                        print(f"\n  Game: {game['away_name']} @ {game['home_name']}")
                        print(f"    Date: {game['game_date']}")
                        print(f"    Score: {game['away_name']} {game['away_score']} @ {game['home_name']} {game['home_score']}")
                        print(f"    Actual Winner: {game['actual_winner_name']} (won by {abs(game['actual_margin'])} pts)")
                        print(f"    Predicted Winner: {game['predicted_winner_name']}")
                        print(f"    Confidence: {game['confidence']:.1%}")
                        print(f"    Home Prob: {game['home_prob']:.1%} | Away Prob: {game['away_prob']:.1%}")
                        if game['predicted_margin']:
                            print(f"    Predicted Margin: {game['predicted_margin']:.1f} pts")
                            print(f"    Actual Margin: {game['actual_margin']} pts")
                            if game['margin_error']:
                                print(f"    Margin Error: {game['margin_error']:.1f} pts")
        
        # Detailed incorrect predictions
        if incorrect_games:
            print("\n" + "=" * 80)
            print("DETAILED INCORRECT PREDICTIONS")
            print("=" * 80)
            
            for i, game in enumerate(sorted(incorrect_games, key=lambda x: x['confidence'], reverse=True), 1):
                print(f"\n[{i}] {game['away_name']} @ {game['home_name']}")
                print(f"    Game ID: {game['game_id']}")
                print(f"    Date: {game['game_date']}")
                print(f"    Score: {game['away_name']} {game['away_score']} @ {game['home_name']} {game['home_score']}")
                print()
                print(f"    PREDICTION:")
                print(f"      Winner: {game['predicted_winner_name']}")
                print(f"      Confidence: {game['confidence']:.1%}")
                print(f"      Home Win Prob: {game['home_prob']:.1%}")
                print(f"      Away Win Prob: {game['away_prob']:.1%}")
                if game['predicted_margin']:
                    if game['predicted_margin'] > 0:
                        print(f"      Predicted Margin: {game['home_name']} by {game['predicted_margin']:.1f} pts")
                    else:
                        print(f"      Predicted Margin: {game['away_name']} by {abs(game['predicted_margin']):.1f} pts")
                print()
                print(f"    ACTUAL RESULT:")
                print(f"      Winner: {game['actual_winner_name']}")
                if game['actual_margin'] > 0:
                    print(f"      Margin: {game['home_name']} won by {game['actual_margin']} pts")
                else:
                    print(f"      Margin: {game['away_name']} won by {abs(game['actual_margin'])} pts")
                print()
                
                # Try to get features for analysis
                try:
                    home_features = session.query(TeamRollingFeatures).filter_by(
                        game_id=game['game_id'],
                        team_id=game['home_team_id']
                    ).first()
                    
                    away_features = session.query(TeamRollingFeatures).filter_by(
                        game_id=game['game_id'],
                        team_id=game['away_team_id']
                    ).first()
                    
                    if home_features and away_features:
                        print(f"    KEY FEATURES:")
                        if home_features.l5_points is not None:
                            print(f"      Home Team (L5): {home_features.l5_points:.1f} pts/game, "
                                  f"{home_features.l5_win_pct:.1%} win%")
                        if away_features.l5_points is not None:
                            print(f"      Away Team (L5): {away_features.l5_points:.1f} pts/game, "
                                  f"{away_features.l5_win_pct:.1%} win%")
                        if home_features.l10_points is not None:
                            print(f"      Home Team (L10): {home_features.l10_points:.1f} pts/game, "
                                  f"{home_features.l10_win_pct:.1%} win%")
                        if away_features.l10_points is not None:
                            print(f"      Away Team (L10): {away_features.l10_points:.1f} pts/game, "
                                  f"{away_features.l10_win_pct:.1%} win%")
                except Exception as e:
                    pass  # Features might not be available
                
                print("-" * 80)
        
        # Analysis
        if incorrect_games:
            print("\n" + "=" * 80)
            print("PATTERN ANALYSIS")
            print("=" * 80)
            
            # Average confidence of incorrect predictions
            avg_conf_incorrect = sum(g['confidence'] for g in incorrect_games) / len(incorrect_games)
            avg_conf_correct = sum(g['confidence'] for g in correct_games) / len(correct_games) if correct_games else 0
            
            print(f"\nConfidence Analysis:")
            print(f"  Average confidence (correct): {avg_conf_correct:.1%}")
            print(f"  Average confidence (incorrect): {avg_conf_incorrect:.1%}")
            print(f"  Difference: {avg_conf_incorrect - avg_conf_correct:+.1%}")
            
            if avg_conf_incorrect > avg_conf_correct:
                print(f"  WARNING: Model is MORE confident in incorrect predictions!")
            
            # Margin errors
            margin_errors = [g['margin_error'] for g in incorrect_games if g['margin_error'] is not None]
            if margin_errors:
                avg_margin_error = sum(margin_errors) / len(margin_errors)
                print(f"\nMargin Prediction Errors:")
                print(f"  Average margin error: {avg_margin_error:.1f} pts")
                print(f"  Max margin error: {max(margin_errors):.1f} pts")
                print(f"  Min margin error: {min(margin_errors):.1f} pts")
            
            # Home vs Away prediction errors
            home_predicted_wins = sum(1 for g in incorrect_games if g['predicted_winner'] == g['home_team_id'])
            away_predicted_wins = sum(1 for g in incorrect_games if g['predicted_winner'] == g['away_team_id'])
            
            home_actual_wins = sum(1 for g in incorrect_games if g['actual_winner'] == g['home_team_id'])
            away_actual_wins = sum(1 for g in incorrect_games if g['actual_winner'] == g['away_team_id'])
            
            print(f"\nPrediction Bias:")
            print(f"  Predicted home wins: {home_predicted_wins} | Actual home wins: {home_actual_wins}")
            print(f"  Predicted away wins: {away_predicted_wins} | Actual away wins: {away_actual_wins}")
            
            if home_predicted_wins > home_actual_wins:
                print(f"  WARNING: Model over-predicts home team wins")
            elif away_predicted_wins > away_actual_wins:
                print(f"  WARNING: Model over-predicts away team wins")
        
        print("\n" + "=" * 80)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Investigate incorrect predictions")
    parser.add_argument("--model-name", type=str, default="nba_v2_classifier",
                       help="Model name to analyze")
    parser.add_argument("--days", type=int, default=30,
                       help="Number of days to analyze")
    
    args = parser.parse_args()
    investigate_incorrect_predictions(args.model_name, args.days)

