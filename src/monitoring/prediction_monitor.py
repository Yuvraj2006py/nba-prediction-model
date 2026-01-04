"""Monitoring and alerting for prediction service."""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import date, datetime, timedelta
from collections import defaultdict
import pandas as pd
import numpy as np

from src.database.db_manager import DatabaseManager
from src.database.models import Game, Prediction
from config.settings import get_settings

logger = logging.getLogger(__name__)


class PredictionMonitor:
    """
    Monitor prediction performance and send alerts.
    
    Tracks:
    - Prediction accuracy over time
    - Confidence calibration
    - Model performance degradation
    - Missing predictions
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize prediction monitor.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.settings = get_settings()
        self.alerts: List[Dict[str, Any]] = []
    
    def check_prediction_accuracy(
        self,
        model_name: str,
        days: int = 7,
        min_games: int = 10,
        accuracy_threshold: float = 0.55
    ) -> Dict[str, Any]:
        """
        Check prediction accuracy for recent games.
        
        Args:
            model_name: Model name to check
            days: Number of days back to check
            min_games: Minimum number of games required
            accuracy_threshold: Minimum acceptable accuracy
            
        Returns:
            Dictionary with accuracy metrics and alerts
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        with self.db_manager.get_session() as session:
            # Get finished games with predictions
            games = session.query(Game).join(Prediction).filter(
                Game.game_date >= start_date,
                Game.game_date <= end_date,
                Game.home_score.isnot(None),
                Game.away_score.isnot(None),
                Prediction.model_name == model_name
            ).all()
            
            if len(games) < min_games:
                return {
                    'status': 'insufficient_data',
                    'games': len(games),
                    'min_games': min_games,
                    'message': f"Only {len(games)} games found, need at least {min_games}"
                }
            
            # Calculate accuracy
            correct = 0
            total = 0
            
            for game in games:
                prediction = session.query(Prediction).filter_by(
                    game_id=game.game_id,
                    model_name=model_name
                ).first()
                
                if not prediction or not prediction.predicted_winner:
                    continue
                
                # Determine actual winner
                if game.home_score > game.away_score:
                    actual_winner = game.home_team_id
                elif game.away_score > game.home_score:
                    actual_winner = game.away_team_id
                else:
                    continue  # Skip ties
                
                if prediction.predicted_winner == actual_winner:
                    correct += 1
                total += 1
            
            if total == 0:
                return {
                    'status': 'no_predictions',
                    'message': "No valid predictions found"
                }
            
            accuracy = correct / total
            
            result = {
                'status': 'ok' if accuracy >= accuracy_threshold else 'alert',
                'model_name': model_name,
                'period_days': days,
                'total_games': total,
                'correct_predictions': correct,
                'accuracy': accuracy,
                'threshold': accuracy_threshold,
                'date_range': (start_date, end_date)
            }
            
            if accuracy < accuracy_threshold:
                result['alert'] = f"Accuracy {accuracy:.1%} below threshold {accuracy_threshold:.1%}"
                self.alerts.append({
                    'type': 'low_accuracy',
                    'model': model_name,
                    'accuracy': accuracy,
                    'threshold': accuracy_threshold,
                    'games': total,
                    'timestamp': datetime.now()
                })
            
            return result
    
    def check_missing_predictions(
        self,
        model_name: str,
        days: int = 1
    ) -> Dict[str, Any]:
        """
        Check for upcoming games without predictions.
        
        Args:
            model_name: Model name to check
            days: Number of days ahead to check
            
        Returns:
            Dictionary with missing prediction info
        """
        end_date = date.today() + timedelta(days=days)
        
        with self.db_manager.get_session() as session:
            # Get upcoming games
            upcoming_games = session.query(Game).filter(
                Game.game_date >= date.today(),
                Game.game_date <= end_date,
                Game.home_score.is_(None)  # Not finished
            ).all()
            
            # Get games with predictions
            games_with_predictions = session.query(Game).join(Prediction).filter(
                Game.game_date >= date.today(),
                Game.game_date <= end_date,
                Game.home_score.is_(None),
                Prediction.model_name == model_name
            ).all()
            
            predicted_game_ids = {g.game_id for g in games_with_predictions}
            missing_games = [g for g in upcoming_games if g.game_id not in predicted_game_ids]
            
            result = {
                'status': 'ok' if len(missing_games) == 0 else 'alert',
                'model_name': model_name,
                'total_upcoming': len(upcoming_games),
                'with_predictions': len(games_with_predictions),
                'missing': len(missing_games),
                'missing_game_ids': [g.game_id for g in missing_games]
            }
            
            if len(missing_games) > 0:
                result['alert'] = f"{len(missing_games)} games missing predictions"
                self.alerts.append({
                    'type': 'missing_predictions',
                    'model': model_name,
                    'count': len(missing_games),
                    'game_ids': [g.game_id for g in missing_games],
                    'timestamp': datetime.now()
                })
            
            return result
    
    def check_confidence_calibration(
        self,
        model_name: str,
        days: int = 30,
        min_games: int = 50
    ) -> Dict[str, Any]:
        """
        Check if model confidence is well-calibrated.
        
        Args:
            model_name: Model name to check
            days: Number of days back to check
            min_games: Minimum number of games required
            
        Returns:
            Dictionary with calibration metrics
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        
        with self.db_manager.get_session() as session:
            games = session.query(Game).join(Prediction).filter(
                Game.game_date >= start_date,
                Game.game_date <= end_date,
                Game.home_score.isnot(None),
                Game.away_score.isnot(None),
                Prediction.model_name == model_name
            ).all()
            
            if len(games) < min_games:
                return {
                    'status': 'insufficient_data',
                    'games': len(games),
                    'min_games': min_games
                }
            
            # Group by confidence bins
            bins = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
            bin_stats = defaultdict(lambda: {'correct': 0, 'total': 0})
            
            for game in games:
                prediction = session.query(Prediction).filter_by(
                    game_id=game.game_id,
                    model_name=model_name
                ).first()
                
                if not prediction:
                    continue
                
                # Determine actual winner
                if game.home_score > game.away_score:
                    actual_winner = game.home_team_id
                elif game.away_score > game.home_score:
                    actual_winner = game.away_team_id
                else:
                    continue
                
                # Find confidence bin
                conf = prediction.confidence
                for i in range(len(bins) - 1):
                    if bins[i] <= conf < bins[i+1]:
                        bin_key = f"{bins[i]:.1f}-{bins[i+1]:.1f}"
                        bin_stats[bin_key]['total'] += 1
                        if prediction.predicted_winner == actual_winner:
                            bin_stats[bin_key]['correct'] += 1
                        break
            
            # Calculate calibration
            calibration_data = {}
            for bin_key, stats in bin_stats.items():
                if stats['total'] > 0:
                    accuracy = stats['correct'] / stats['total']
                    calibration_data[bin_key] = {
                        'accuracy': accuracy,
                        'count': stats['total']
                    }
            
            result = {
                'status': 'ok',
                'model_name': model_name,
                'calibration': calibration_data,
                'period_days': days
            }
            
            return result
    
    def get_all_alerts(self) -> List[Dict[str, Any]]:
        """Get all current alerts."""
        return self.alerts
    
    def clear_alerts(self):
        """Clear all alerts."""
        self.alerts = []
    
    def run_health_check(
        self,
        model_name: str,
        accuracy_days: int = 7,
        missing_days: int = 1
    ) -> Dict[str, Any]:
        """
        Run comprehensive health check.
        
        Args:
            model_name: Model name to check
            accuracy_days: Days to check for accuracy
            missing_days: Days ahead to check for missing predictions
            
        Returns:
            Dictionary with all health check results
        """
        logger.info(f"Running health check for model: {model_name}")
        
        results = {
            'model_name': model_name,
            'timestamp': datetime.now().isoformat(),
            'checks': {}
        }
        
        # Accuracy check
        accuracy_result = self.check_prediction_accuracy(model_name, days=accuracy_days)
        results['checks']['accuracy'] = accuracy_result
        
        # Missing predictions check
        missing_result = self.check_missing_predictions(model_name, days=missing_days)
        results['checks']['missing_predictions'] = missing_result
        
        # Calibration check
        calibration_result = self.check_confidence_calibration(model_name)
        results['checks']['calibration'] = calibration_result
        
        # Overall status
        if any(check.get('status') == 'alert' for check in results['checks'].values()):
            results['overall_status'] = 'alert'
        else:
            results['overall_status'] = 'ok'
        
        return results




