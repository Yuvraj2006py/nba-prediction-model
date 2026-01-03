"""
Betting Manager for NBA Prediction System.

Handles all betting operations including:
- Placing bets with multiple strategies
- Resolving bets
- Tracking bankroll per strategy
- PNL reporting
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import date, datetime, timedelta
from sqlalchemy import func

from src.database.db_manager import DatabaseManager
from src.database.models import Game, Prediction, BettingLine, Bet, BankrollSnapshot
from src.prediction.prediction_service import PredictionService
from src.backtesting.strategies import (
    BettingStrategy,
    KellyCriterionStrategy,
    ExpectedValueStrategy,
    ConfidenceThresholdStrategy
)

logger = logging.getLogger(__name__)

# Default initial bankroll for each strategy
DEFAULT_INITIAL_BANKROLL = 25.0

# Available strategies
STRATEGIES = {
    'kelly': lambda: KellyCriterionStrategy(kelly_fraction=0.25, min_confidence=0.55),
    'ev': lambda: ExpectedValueStrategy(min_ev=0.05, bet_fraction=0.02, max_bet=500.0),
    'confidence': lambda: ConfidenceThresholdStrategy(confidence_threshold=0.60, bet_amount=100.0)
}


class BettingManager:
    """
    Manages betting operations for the daily workflow.
    
    Supports multiple strategies running in parallel, each with its own
    bankroll and PNL tracking.
    """
    
    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        initial_bankroll: float = DEFAULT_INITIAL_BANKROLL
    ):
        """
        Initialize betting manager.
        
        Args:
            db_manager: Database manager instance
            initial_bankroll: Starting bankroll for each strategy
        """
        self.db_manager = db_manager or DatabaseManager()
        self.prediction_service = PredictionService(self.db_manager)
        self.initial_bankroll = initial_bankroll
    
    def get_strategy(self, strategy_name: str) -> BettingStrategy:
        """Get a betting strategy by name."""
        if strategy_name not in STRATEGIES:
            raise ValueError(f"Unknown strategy: {strategy_name}. Available: {list(STRATEGIES.keys())}")
        return STRATEGIES[strategy_name]()
    
    def get_bankroll(self, strategy_name: str, start_date: Optional[date] = None) -> float:
        """
        Get current bankroll for a strategy.
        
        Calculates from initial bankroll + all resolved bet profits.
        If start_date is provided, only counts bets from that date onwards.
        
        Args:
            strategy_name: Strategy name
            start_date: Optional date to start counting profits from (for fresh starts)
        """
        with self.db_manager.get_session() as session:
            query = session.query(func.sum(Bet.profit)).filter(
                Bet.strategy_name == strategy_name,
                Bet.outcome.isnot(None)
            )
            
            # If start_date is provided, only count bets from that date onwards
            if start_date:
                query = query.join(Game).filter(Game.game_date >= start_date)
            
            total_profit = query.scalar() or 0.0
            
            return self.initial_bankroll + total_profit
    
    def get_odds_for_game(self, game_id: str, sportsbook: str = 'draftkings') -> Optional[Dict[str, Any]]:
        """
        Get betting odds for a game from a specific sportsbook (default: DraftKings).
        Falls back to FanDuel, then any other sportsbook if preferred not found.
        
        Args:
            game_id: Game identifier
            sportsbook: Preferred sportsbook name (default: 'draftkings')
        """
        with self.db_manager.get_session() as session:
            # Priority order: DraftKings -> FanDuel -> Any other
            preferred_sportsbooks = ['draftkings', 'fanduel']
            if sportsbook.lower() not in preferred_sportsbooks:
                preferred_sportsbooks.insert(0, sportsbook.lower())
            
            lines = None
            used_sportsbook = None
            
            # Try preferred sportsbooks in order
            for sb in preferred_sportsbooks:
                lines = session.query(BettingLine).filter(
                    BettingLine.game_id == game_id,
                    BettingLine.sportsbook == sb.lower()
                ).order_by(BettingLine.created_at.desc()).all()
                
                if lines:
                    used_sportsbook = sb
                    break
            
            # If still not found, try any sportsbook
            if not lines:
                logger.debug(f"No {sportsbook} or FanDuel odds found for game {game_id}, trying other sportsbooks...")
                lines = session.query(BettingLine).filter(
                    BettingLine.game_id == game_id
                ).order_by(BettingLine.created_at.desc()).all()
                
                if lines:
                    used_sportsbook = lines[0].sportsbook
                    logger.debug(f"Using {used_sportsbook} odds for game {game_id} (DraftKings/FanDuel not available)")
            
            if not lines:
                logger.warning(f"No odds found for game {game_id}")
                return None
            
            # Use most recent line
            latest = lines[0]
            
            return {
                'moneyline_home': latest.moneyline_home,
                'moneyline_away': latest.moneyline_away,
                'point_spread_home': latest.point_spread_home,
                'point_spread_away': latest.point_spread_away
            }
    
    def place_bets_for_date(
        self,
        target_date: date,
        strategy_names: List[str] = None,
        model_name: str = 'nba_v2_classifier',
        include_finished: bool = False
    ) -> Dict[str, Any]:
        """
        Place bets for all games on a date using specified strategies.
        
        Args:
            target_date: Date to place bets for
            strategy_names: List of strategy names to use (default: all)
            model_name: Model name for predictions
            include_finished: If True, allow betting on finished games (for testing/backtesting)
            
        Returns:
            Dictionary with results per strategy
        """
        if strategy_names is None:
            strategy_names = list(STRATEGIES.keys())
        
        results = {}
        
        with self.db_manager.get_session() as session:
            # Get games for the date
            if include_finished:
                # For testing/backtesting - include all games
                games = session.query(Game).filter(
                    Game.game_date == target_date
                ).all()
            else:
                # Normal operation - only scheduled games
                games = session.query(Game).filter(
                    Game.game_date == target_date,
                    Game.game_status == 'scheduled'
                ).all()
            
            if not games:
                logger.info(f"No games found for {target_date}")
                return {'status': 'no_games', 'strategies': {}}
            
            logger.info(f"Found {len(games)} games for {target_date}")
            
            for strategy_name in strategy_names:
                strategy = self.get_strategy(strategy_name)
                # Get bankroll counting only from today (for fresh starts)
                bankroll = self.get_bankroll(strategy_name, start_date=target_date)
                
                bets_placed = []
                total_wagered = 0.0
                
                for game in games:
                    try:
                        # Check if bet already exists for this game/strategy
                        existing_bet = session.query(Bet).filter(
                            Bet.game_id == game.game_id,
                            Bet.strategy_name == strategy_name
                        ).first()
                        
                        if existing_bet:
                            logger.debug(f"Bet already exists for {game.game_id} with {strategy_name}")
                            continue
                        
                        # Get prediction
                        prediction = session.query(Prediction).filter(
                            Prediction.game_id == game.game_id,
                            Prediction.model_name == model_name
                        ).first()
                        
                        if not prediction:
                            logger.debug(f"No prediction for game {game.game_id}")
                            continue
                        
                        # Convert prediction to dict for strategy
                        pred_dict = {
                            'predicted_winner': prediction.predicted_winner,
                            'confidence': prediction.confidence,
                            'win_probability_home': prediction.win_probability_home,
                            'win_probability_away': prediction.win_probability_away
                        }
                        
                        # Get odds
                        odds = self.get_odds_for_game(game.game_id)
                        if not odds:
                            logger.debug(f"No odds for game {game.game_id}")
                            continue
                        
                        # Apply strategy
                        bet_decision = strategy.should_bet(
                            prediction=pred_dict,
                            odds=odds,
                            bankroll=bankroll - total_wagered,  # Remaining bankroll
                            game=game
                        )
                        
                        if bet_decision:
                            # Record bet
                            bet = Bet(
                                game_id=game.game_id,
                                strategy_name=strategy_name,
                                bet_type=bet_decision['bet_type'],
                                bet_team=bet_decision['bet_team'],
                                bet_value=bet_decision.get('bet_value'),
                                bet_amount=bet_decision['bet_amount'],
                                odds=bet_decision['odds'],
                                expected_value=bet_decision['expected_value'],
                                confidence=bet_decision.get('confidence')
                            )
                            session.add(bet)
                            
                            total_wagered += bet_decision['bet_amount']
                            
                            # Get team name for logging
                            from src.database.models import Team
                            team = session.query(Team).filter(Team.team_id == bet_decision['bet_team']).first()
                            team_name = team.team_name if team else bet_decision['bet_team']
                            
                            # Get original American odds from BettingLine for display
                            # Prioritize DraftKings, then FanDuel, then any other (same as get_odds_for_game)
                            betting_line = None
                            sportsbook_used = None
                            for sb in ['draftkings', 'fanduel']:
                                betting_line = session.query(BettingLine).filter(
                                    BettingLine.game_id == game.game_id,
                                    BettingLine.sportsbook == sb.lower()
                                ).order_by(BettingLine.created_at.desc()).first()
                                if betting_line:
                                    sportsbook_used = sb
                                    break
                            
                            # If still not found, get any sportsbook
                            if not betting_line:
                                betting_line = session.query(BettingLine).filter(
                                    BettingLine.game_id == game.game_id
                                ).order_by(BettingLine.created_at.desc()).first()
                                if betting_line:
                                    sportsbook_used = betting_line.sportsbook
                            
                            american_odds = None
                            if betting_line:
                                if bet_decision['bet_team'] == game.home_team_id:
                                    american_odds = betting_line.moneyline_home
                                else:
                                    american_odds = betting_line.moneyline_away
                            
                            bets_placed.append({
                                'game_id': game.game_id,
                                'team': team_name,
                                'amount': bet_decision['bet_amount'],
                                'odds': bet_decision['odds'],  # Decimal odds for calculations
                                'american_odds': american_odds,  # Original American odds for display
                                'sportsbook': sportsbook_used,  # Track which sportsbook was used
                                'confidence': bet_decision.get('confidence', 0),
                                'ev': bet_decision['expected_value']
                            })
                    
                    except Exception as e:
                        logger.error(f"Error placing bet for {game.game_id}: {e}")
                        continue
                
                session.commit()
                
                # Get existing bets for this strategy/date to include in summary
                existing_bets = self.get_existing_bets_for_date(target_date, strategy_name)
                
                # Calculate total pending bets (new + existing)
                total_pending = total_wagered + sum(b['amount'] for b in existing_bets)
                
                # Combine new and existing bets for display
                all_bets = bets_placed + existing_bets
                total_all_wagered = total_pending
                
                results[strategy_name] = {
                    'bets_placed': len(bets_placed),  # New bets only
                    'bets_existing': len(existing_bets),  # Existing bets
                    'total_wagered': total_wagered,  # New bets wagered
                    'total_all_wagered': total_all_wagered,  # All bets (new + existing)
                    'bankroll_before': bankroll,
                    'bankroll_after': bankroll - total_pending,  # Subtract ALL pending bets
                    'bets': bets_placed,  # New bets only
                    'all_bets': all_bets  # All bets for display
                }
        
        return {'status': 'complete', 'date': str(target_date), 'strategies': results}
    
    def get_existing_bets_for_date(self, target_date: date, strategy_name: str) -> List[Dict[str, Any]]:
        """
        Get existing bets for a date and strategy (for display purposes).
        
        Args:
            target_date: Date to get bets for
            strategy_name: Strategy name
            
        Returns:
            List of bet dictionaries
        """
        existing_bets = []
        
        with self.db_manager.get_session() as session:
            bets = session.query(Bet).join(Game).filter(
                Game.game_date == target_date,
                Bet.strategy_name == strategy_name
            ).all()
            
            for bet in bets:
                # Get team name
                from src.database.models import Team
                team = session.query(Team).filter(Team.team_id == bet.bet_team).first()
                team_name = team.team_name if team else bet.bet_team
                
                # Get original American odds from BettingLine
                # Prioritize DraftKings, then FanDuel, then any other (same as get_odds_for_game)
                betting_line = None
                sportsbook_used = None
                for sb in ['draftkings', 'fanduel']:
                    betting_line = session.query(BettingLine).filter(
                        BettingLine.game_id == bet.game_id,
                        BettingLine.sportsbook == sb.lower()
                    ).order_by(BettingLine.created_at.desc()).first()
                    if betting_line:
                        sportsbook_used = sb
                        break
                
                # If still not found, get any sportsbook
                if not betting_line:
                    betting_line = session.query(BettingLine).filter(
                        BettingLine.game_id == bet.game_id
                    ).order_by(BettingLine.created_at.desc()).first()
                    if betting_line:
                        sportsbook_used = betting_line.sportsbook
                
                # Determine if bet was on home or away team
                game = session.query(Game).filter(Game.game_id == bet.game_id).first()
                american_odds = None
                if game and betting_line:
                    if bet.bet_team == game.home_team_id:
                        american_odds = betting_line.moneyline_home
                    else:
                        american_odds = betting_line.moneyline_away
                
                existing_bets.append({
                    'game_id': bet.game_id,
                    'team': team_name,
                    'amount': bet.bet_amount,
                    'odds': bet.odds,  # Decimal odds for calculations
                    'american_odds': american_odds,  # Original American odds for display
                    'sportsbook': sportsbook_used,  # Track which sportsbook was used
                    'confidence': bet.confidence or 0,
                    'ev': bet.expected_value
                })
        
        return existing_bets
    
    def resolve_bets_for_date(self, target_date: date) -> Dict[str, Any]:
        """
        Resolve all pending bets for a date.
        
        Args:
            target_date: Date to resolve bets for
            
        Returns:
            Dictionary with results per strategy
        """
        results = {}
        
        with self.db_manager.get_session() as session:
            # Get finished games
            finished_games = session.query(Game).filter(
                Game.game_date == target_date,
                Game.home_score.isnot(None),
                Game.away_score.isnot(None)
            ).all()
            
            if not finished_games:
                return {'status': 'no_finished_games', 'strategies': {}}
            
            game_ids = [g.game_id for g in finished_games]
            
            # Get pending bets for these games
            pending_bets = session.query(Bet).filter(
                Bet.game_id.in_(game_ids),
                Bet.outcome.is_(None)
            ).all()
            
            if not pending_bets:
                return {'status': 'no_pending_bets', 'strategies': {}}
            
            # Group by strategy
            strategy_bets = {}
            for bet in pending_bets:
                if bet.strategy_name not in strategy_bets:
                    strategy_bets[bet.strategy_name] = []
                strategy_bets[bet.strategy_name].append(bet)
            
            for strategy_name, bets in strategy_bets.items():
                wins = 0
                losses = 0
                total_profit = 0.0
                resolved_bets = []
                
                for bet in bets:
                    # Get the game
                    game = session.query(Game).filter(Game.game_id == bet.game_id).first()
                    if not game or not game.winner:
                        continue
                    
                    # Resolve the bet
                    if bet.bet_type == 'moneyline':
                        if bet.bet_team == game.winner:
                            # Win
                            payout = bet.bet_amount * bet.odds
                            profit = payout - bet.bet_amount
                            bet.outcome = 'win'
                            bet.payout = payout
                            bet.profit = profit
                            wins += 1
                        else:
                            # Loss
                            bet.outcome = 'loss'
                            bet.payout = 0.0
                            bet.profit = -bet.bet_amount
                            losses += 1
                        
                        bet.resolved_at = datetime.now()
                        total_profit += bet.profit
                        
                        # Get team names for logging
                        from src.database.models import Team
                        bet_team = session.query(Team).filter(Team.team_id == bet.bet_team).first()
                        winner_team = session.query(Team).filter(Team.team_id == game.winner).first()
                        
                        resolved_bets.append({
                            'game_id': bet.game_id,
                            'bet_team': bet_team.team_name if bet_team else bet.bet_team,
                            'winner': winner_team.team_name if winner_team else game.winner,
                            'outcome': bet.outcome,
                            'amount': bet.bet_amount,
                            'profit': bet.profit
                        })
                
                session.commit()
                
                results[strategy_name] = {
                    'resolved': len(resolved_bets),
                    'wins': wins,
                    'losses': losses,
                    'total_profit': total_profit,
                    'win_rate': wins / len(resolved_bets) if resolved_bets else 0.0,
                    'bets': resolved_bets
                }
        
        # Save bankroll snapshots
        self._save_bankroll_snapshots(target_date, results)
        
        return {'status': 'complete', 'date': str(target_date), 'strategies': results}
    
    def _save_bankroll_snapshots(self, target_date: date, results: Dict[str, Any]):
        """Save daily bankroll snapshots for each strategy."""
        with self.db_manager.get_session() as session:
            for strategy_name, data in results.items():
                # Check if snapshot exists
                existing = session.query(BankrollSnapshot).filter(
                    BankrollSnapshot.strategy_name == strategy_name,
                    BankrollSnapshot.snapshot_date == target_date
                ).first()
                
                bankroll = self.get_bankroll(strategy_name)
                
                if existing:
                    # Update existing
                    existing.bankroll = bankroll
                    existing.daily_pnl = data['total_profit']
                    existing.total_bets = data['resolved']
                    existing.wins = data['wins']
                    existing.losses = data['losses']
                else:
                    # Create new
                    snapshot = BankrollSnapshot(
                        strategy_name=strategy_name,
                        snapshot_date=target_date,
                        bankroll=bankroll,
                        daily_pnl=data['total_profit'],
                        total_bets=data['resolved'],
                        wins=data['wins'],
                        losses=data['losses']
                    )
                    session.add(snapshot)
                
                session.commit()
    
    def get_daily_pnl(self, target_date: date) -> Dict[str, Any]:
        """Get PNL summary for a specific date."""
        results = {}
        
        with self.db_manager.get_session() as session:
            for strategy_name in STRATEGIES.keys():
                # Get resolved bets for this date/strategy
                bets = session.query(Bet).join(Game).filter(
                    Game.game_date == target_date,
                    Bet.strategy_name == strategy_name,
                    Bet.outcome.isnot(None)
                ).all()
                
                if not bets:
                    results[strategy_name] = {
                        'bets': 0,
                        'wins': 0,
                        'losses': 0,
                        'pnl': 0.0,
                        'bankroll': self.get_bankroll(strategy_name)
                    }
                    continue
                
                wins = sum(1 for b in bets if b.outcome == 'win')
                losses = sum(1 for b in bets if b.outcome == 'loss')
                pnl = sum(b.profit or 0 for b in bets)
                
                results[strategy_name] = {
                    'bets': len(bets),
                    'wins': wins,
                    'losses': losses,
                    'win_rate': wins / len(bets) if bets else 0.0,
                    'pnl': pnl,
                    'roi': (pnl / sum(b.bet_amount for b in bets)) * 100 if bets else 0.0,
                    'bankroll': self.get_bankroll(strategy_name)
                }
        
        return results
    
    def get_period_pnl(
        self,
        start_date: date,
        end_date: date
    ) -> Dict[str, Any]:
        """
        Get PNL summary for a date range.
        
        Args:
            start_date: Start of period
            end_date: End of period
            
        Returns:
            Dictionary with PNL per strategy
        """
        results = {}
        
        with self.db_manager.get_session() as session:
            for strategy_name in STRATEGIES.keys():
                # Get all resolved bets for this strategy in date range
                bets = session.query(Bet).join(Game).filter(
                    Game.game_date >= start_date,
                    Game.game_date <= end_date,
                    Bet.strategy_name == strategy_name,
                    Bet.outcome.isnot(None)
                ).all()
                
                if not bets:
                    results[strategy_name] = {
                        'total_bets': 0,
                        'wins': 0,
                        'losses': 0,
                        'total_pnl': 0.0,
                        'total_roi': 0.0,
                        'bankroll': self.get_bankroll(strategy_name)
                    }
                    continue
                
                wins = sum(1 for b in bets if b.outcome == 'win')
                losses = sum(1 for b in bets if b.outcome == 'loss')
                total_wagered = sum(b.bet_amount for b in bets)
                total_pnl = sum(b.profit or 0 for b in bets)
                
                results[strategy_name] = {
                    'total_bets': len(bets),
                    'wins': wins,
                    'losses': losses,
                    'win_rate': wins / len(bets) if bets else 0.0,
                    'total_wagered': total_wagered,
                    'total_pnl': total_pnl,
                    'total_roi': (total_pnl / total_wagered) * 100 if total_wagered else 0.0,
                    'bankroll': self.get_bankroll(strategy_name)
                }
        
        return results
    
    def print_daily_summary(self, target_date: date, quiet: bool = False):
        """Print daily PNL summary."""
        if quiet:
            return
        
        pnl = self.get_daily_pnl(target_date)
        
        logger.info("=" * 70)
        logger.info(f"DAILY BETTING SUMMARY - {target_date}")
        logger.info("=" * 70)
        
        for strategy_name, data in pnl.items():
            if data['bets'] == 0:
                logger.info(f"\n{strategy_name.upper()}: No bets resolved")
                continue
            
            pnl_str = f"+${data['pnl']:.2f}" if data['pnl'] >= 0 else f"-${abs(data['pnl']):.2f}"
            roi_str = f"+{data['roi']:.1f}%" if data['roi'] >= 0 else f"{data['roi']:.1f}%"
            
            logger.info(f"\n{strategy_name.upper()}:")
            logger.info(f"  Bets: {data['bets']} ({data['wins']}W / {data['losses']}L)")
            logger.info(f"  Win Rate: {data['win_rate']:.1%}")
            logger.info(f"  Daily PNL: {pnl_str} ({roi_str})")
            logger.info(f"  Bankroll: ${data['bankroll']:,.2f}")
        
        logger.info("=" * 70)
    
    def print_period_summary(
        self,
        start_date: date,
        end_date: date,
        quiet: bool = False
    ):
        """Print PNL summary for a period."""
        if quiet:
            return
        
        pnl = self.get_period_pnl(start_date, end_date)
        
        # Calculate days in period
        days = (end_date - start_date).days + 1
        period_name = f"{start_date} to {end_date}"
        if days <= 7:
            period_name = f"WEEKLY ({start_date} - {end_date})"
        elif days <= 31:
            period_name = f"MONTHLY ({start_date} - {end_date})"
        
        logger.info("=" * 70)
        logger.info(f"{period_name} STRATEGY PERFORMANCE")
        logger.info("=" * 70)
        
        best_strategy = None
        best_roi = float('-inf')
        
        for strategy_name, data in pnl.items():
            pnl_str = f"+${data['total_pnl']:.2f}" if data['total_pnl'] >= 0 else f"-${abs(data['total_pnl']):.2f}"
            roi_str = f"+{data['total_roi']:.1f}%" if data['total_roi'] >= 0 else f"{data['total_roi']:.1f}%"
            
            logger.info(f"\n{strategy_name.upper()}:")
            logger.info(f"  Total Bets: {data['total_bets']}")
            logger.info(f"  Win Rate: {data['win_rate']:.1%}")
            logger.info(f"  Total PNL: {pnl_str}")
            logger.info(f"  Total ROI: {roi_str}")
            logger.info(f"  Final Bankroll: ${data['bankroll']:,.2f}")
            
            if data['total_roi'] > best_roi and data['total_bets'] > 0:
                best_roi = data['total_roi']
                best_strategy = strategy_name
        
        if best_strategy:
            logger.info("=" * 70)
            best_roi_str = f"+{best_roi:.1f}%" if best_roi >= 0 else f"{best_roi:.1f}%"
            logger.info(f"OVERALL WINNER: {best_strategy.upper()} ({best_roi_str} ROI)")
        
        logger.info("=" * 70)

