"""Forward testing system for today's games."""

import logging
from typing import Dict, Any, List, Optional
from datetime import date, datetime
from pathlib import Path

from src.database.db_manager import DatabaseManager
from src.database.models import Game, Prediction, BettingLine, Bet
from src.prediction.prediction_service import PredictionService
from src.backtesting.strategies import BettingStrategy
from config.settings import get_settings

logger = logging.getLogger(__name__)


class ForwardTester:
    """
    Forward testing system for testing predictions on today's games.
    
    This is NOT backtesting - it's forward testing where we:
    1. Make predictions before games start
    2. Record betting decisions
    3. Wait for games to finish
    4. Resolve bets and calculate results
    """
    
    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        initial_bankroll: float = 10000.0
    ):
        """
        Initialize forward tester.
        
        Args:
            db_manager: Database manager instance
            initial_bankroll: Starting bankroll amount
        """
        self.db_manager = db_manager or DatabaseManager()
        self.prediction_service = PredictionService(self.db_manager)
        self.initial_bankroll = initial_bankroll
        self.settings = get_settings()
    
    def setup_today_test(
        self,
        test_date: date,
        model_name: str,
        strategy: BettingStrategy,
        clf_model_name: Optional[str] = None,
        reg_model_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Setup forward test for today's games.
        
        Steps:
        1. Get today's games
        2. Make predictions
        3. Get odds
        4. Apply strategy to decide bets
        5. Record bets (pending resolution)
        
        Args:
            test_date: Date to test
            model_name: Model name identifier
            strategy: Betting strategy instance
            clf_model_name: Classification model name
            reg_model_name: Regression model name
            
        Returns:
            Dictionary with setup results
        """
        logger.info(f"Setting up forward test for {test_date}")
        
        # Get games
        games = self.prediction_service.get_upcoming_games(
            start_date=test_date,
            end_date=test_date
        )
        
        if not games:
            logger.warning(f"No games found for {test_date}")
            return {
                'status': 'no_games',
                'games_found': 0,
                'bets_placed': 0
            }
        
        logger.info(f"Found {len(games)} games for {test_date}")
        
        # Get current bankroll
        bankroll = self._get_current_bankroll()
        
        bets_placed = []
        
        for game in games:
            try:
                # Make prediction
                prediction = self.prediction_service.predict_game(
                    game_id=game.game_id,
                    model_name=model_name,
                    clf_model_name=clf_model_name,
                    reg_model_name=reg_model_name,
                    regenerate_features=False
                )
                
                # Save prediction
                if prediction:
                    self.prediction_service.save_prediction(prediction, model_name=clf_model_name or model_name)
                
                if not prediction:
                    logger.warning(f"Could not generate prediction for game {game.game_id}")
                    continue
                
                # Get odds
                odds = self._get_odds_for_game(game.game_id)
                if not odds:
                    logger.warning(f"No odds available for game {game.game_id}")
                    continue
                
                # Apply strategy (pass game info for team IDs)
                bet_decision = strategy.should_bet(
                    prediction=prediction,
                    odds=odds,
                    bankroll=bankroll,
                    game=game
                )
                
                if bet_decision:
                    # Record bet
                    bet_id = self._record_bet(
                        game_id=game.game_id,
                        bet_decision=bet_decision,
                        prediction=prediction
                    )
                    bets_placed.append({
                        'game_id': game.game_id,
                        'bet_id': bet_id,
                        'bet_decision': bet_decision
                    })
                    logger.info(f"Placed bet on game {game.game_id}: {bet_decision['bet_type']} on {bet_decision['bet_team']}")
                else:
                    logger.debug(f"No bet placed on game {game.game_id}")
                    
            except Exception as e:
                logger.error(f"Error processing game {game.game_id}: {e}")
                continue
        
        return {
            'status': 'complete',
            'games_found': len(games),
            'bets_placed': len(bets_placed),
            'bets': bets_placed,
            'bankroll': bankroll
        }
    
    def resolve_today_bets(self, test_date: date) -> Dict[str, Any]:
        """
        Resolve all pending bets for a given date.
        
        Args:
            test_date: Date to resolve bets for
            
        Returns:
            Dictionary with resolution results
        """
        logger.info(f"Resolving bets for {test_date}")
        
        # Get all pending bets for date
        with self.db_manager.get_session() as session:
            games = session.query(Game).filter(
                Game.game_date == test_date,
                Game.home_score.isnot(None)  # Game finished
            ).all()
            
            if not games:
                logger.info(f"No finished games found for {test_date}")
                return {
                    'status': 'no_finished_games',
                    'bets_resolved': 0
                }
            
            bets_resolved = 0
            results = []
            
            for game in games:
                # Get pending bets for this game
                bets = session.query(Bet).filter(
                    Bet.game_id == game.game_id,
                    Bet.outcome.is_(None)  # Not resolved yet
                ).all()
                
                for bet in bets:
                    outcome = self._evaluate_bet(bet, game)
                    
                    if outcome:
                        bet.outcome = outcome['outcome']
                        bet.payout = outcome.get('payout')
                        bet.profit = outcome.get('profit')
                        bet.resolved_at = datetime.now()
                        bets_resolved += 1
                        
                        results.append({
                            'bet_id': bet.id,
                            'game_id': game.game_id,
                            'outcome': outcome['outcome'],
                            'profit': outcome.get('profit', 0.0)
                        })
                
                session.commit()
            
            # Calculate summary
            total_profit = sum(r['profit'] for r in results)
            wins = sum(1 for r in results if r['outcome'] == 'win')
            losses = sum(1 for r in results if r['outcome'] == 'loss')
            
            return {
                'status': 'complete',
                'bets_resolved': bets_resolved,
                'wins': wins,
                'losses': losses,
                'total_profit': total_profit,
                'win_rate': wins / len(results) if results else 0.0,
                'results': results
            }
    
    def _get_odds_for_game(self, game_id: str) -> Optional[Dict[str, Any]]:
        """Get odds for a game (consensus or latest)."""
        with self.db_manager.get_session() as session:
            lines = session.query(BettingLine).filter(
                BettingLine.game_id == game_id
            ).order_by(BettingLine.created_at.desc()).all()
            
            if not lines:
                return None
            
            # Use most recent line (or could calculate consensus)
            latest = lines[0]
            
            return {
                'moneyline_home': latest.moneyline_home,
                'moneyline_away': latest.moneyline_away,
                'point_spread_home': latest.point_spread_home,
                'point_spread_away': latest.point_spread_away,
                'total': latest.total if hasattr(latest, 'total') else None
            }
    
    def _record_bet(
        self,
        game_id: str,
        bet_decision: Dict[str, Any],
        prediction: Dict[str, Any]
    ) -> int:
        """Record a bet in the database."""
        with self.db_manager.get_session() as session:
            bet = Bet(
                game_id=game_id,
                bet_type=bet_decision['bet_type'],
                bet_team=bet_decision['bet_team'],
                bet_value=bet_decision.get('bet_value'),
                bet_amount=bet_decision['bet_amount'],
                odds=bet_decision['odds'],
                expected_value=bet_decision['expected_value']
            )
            session.add(bet)
            session.commit()
            return bet.id
    
    def _evaluate_bet(self, bet: Bet, game: Game) -> Optional[Dict[str, Any]]:
        """Evaluate if a bet won or lost."""
        if bet.bet_type == 'moneyline':
            # Moneyline: bet on team to win
            if bet.bet_team == game.winner:
                # Win
                payout = bet.bet_amount * bet.odds
                profit = payout - bet.bet_amount
                return {
                    'outcome': 'win',
                    'payout': payout,
                    'profit': profit
                }
            else:
                # Loss
                return {
                    'outcome': 'loss',
                    'payout': 0.0,
                    'profit': -bet.bet_amount
                }
        
        # TODO: Handle spread and over/under bets
        return None
    
    def _get_current_bankroll(self) -> float:
        """Get current bankroll (sum of all resolved bets)."""
        with self.db_manager.get_session() as session:
            # Start with initial bankroll
            bankroll = self.initial_bankroll
            
            # Add/subtract resolved bets
            resolved_bets = session.query(Bet).filter(
                Bet.outcome.isnot(None)
            ).all()
            
            for bet in resolved_bets:
                if bet.profit:
                    bankroll += bet.profit
            
            return bankroll
    
    def get_test_summary(self, test_date: date) -> Dict[str, Any]:
        """Get summary of test results for a date."""
        with self.db_manager.get_session() as session:
            bets = session.query(Bet).join(Game).filter(
                Game.game_date == test_date
            ).all()
            
            pending = [b for b in bets if b.outcome is None]
            resolved = [b for b in bets if b.outcome is not None]
            
            wins = sum(1 for b in resolved if b.outcome == 'win')
            losses = sum(1 for b in resolved if b.outcome == 'loss')
            total_profit = sum(b.profit or 0.0 for b in resolved)
            
            return {
                'date': test_date,
                'total_bets': len(bets),
                'pending': len(pending),
                'resolved': len(resolved),
                'wins': wins,
                'losses': losses,
                'win_rate': wins / len(resolved) if resolved else 0.0,
                'total_profit': total_profit,
                'current_bankroll': self._get_current_bankroll()
            }

