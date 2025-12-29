"""Betting Features Calculator - Processes betting odds into features."""

import logging
import numpy as np
from typing import Optional, List, Dict
from src.database.db_manager import DatabaseManager
from config.settings import get_settings

logger = logging.getLogger(__name__)


class BettingFeatureCalculator:
    """Calculates features from betting odds."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize betting feature calculator.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.settings = get_settings()
        
        logger.info("BettingFeatureCalculator initialized")
    
    def get_consensus_spread(
        self,
        game_id: str
    ) -> Optional[float]:
        """
        Get consensus point spread (average across sportsbooks).
        
        Args:
            game_id: Game identifier
            
        Returns:
            Consensus spread (from home team perspective) or None
        """
        betting_lines = self.db_manager.get_betting_lines(game_id)
        
        if not betting_lines:
            return None
        
        spreads = []
        for line in betting_lines:
            if line.point_spread_home is not None:
                spreads.append(line.point_spread_home)
        
        if not spreads:
            return None
        
        consensus = np.mean(spreads)
        return round(consensus, 2)
    
    def get_consensus_total(
        self,
        game_id: str
    ) -> Optional[float]:
        """
        Get consensus over/under total.
        
        Args:
            game_id: Game identifier
            
        Returns:
            Consensus total or None
        """
        betting_lines = self.db_manager.get_betting_lines(game_id)
        
        if not betting_lines:
            return None
        
        totals = []
        for line in betting_lines:
            if line.over_under is not None:
                totals.append(line.over_under)
        
        if not totals:
            return None
        
        consensus = np.mean(totals)
        return round(consensus, 2)
    
    def get_moneyline_implied_prob(
        self,
        game_id: str,
        team_id: str
    ) -> Optional[float]:
        """
        Convert moneyline odds to implied win probability.
        
        Args:
            game_id: Game identifier
            team_id: Team identifier
            
        Returns:
            Implied win probability (0-1) or None
        """
        betting_lines = self.db_manager.get_betting_lines(game_id)
        
        if not betting_lines:
            return None
        
        game = self.db_manager.get_game(game_id)
        if not game:
            return None
        
        # Determine if team is home or away
        is_home = game.home_team_id == team_id
        
        moneylines = []
        for line in betting_lines:
            if is_home and line.moneyline_home is not None:
                moneylines.append(line.moneyline_home)
            elif not is_home and line.moneyline_away is not None:
                moneylines.append(line.moneyline_away)
        
        if not moneylines:
            return None
        
        # Convert American odds to probabilities and average
        probs = []
        for ml in moneylines:
            prob = self._american_to_probability(ml)
            if prob:
                probs.append(prob)
        
        if not probs:
            return None
        
        avg_prob = np.mean(probs)
        return round(avg_prob, 4)
    
    def get_spread_implied_prob(
        self,
        game_id: str,
        team_id: str
    ) -> Optional[float]:
        """
        Get implied probability of covering the spread.
        Assumes -110 odds on both sides (standard).
        
        Args:
            game_id: Game identifier
            team_id: Team identifier
            
        Returns:
            Implied probability (0-1) or None
        """
        # Standard spread odds are -110, which implies ~52.4% probability
        # This is a simplified calculation
        return 0.524
    
    def get_total_implied_prob(
        self,
        game_id: str,
        over: bool = True
    ) -> Optional[float]:
        """
        Get implied probability of over/under.
        Assumes -110 odds on both sides (standard).
        
        Args:
            game_id: Game identifier
            over: If True, return over probability; else under
            
        Returns:
            Implied probability (0-1) or None
        """
        # Standard total odds are -110, which implies ~52.4% probability
        return 0.524
    
    def calculate_value(
        self,
        implied_prob: float,
        model_prob: float
    ) -> float:
        """
        Calculate expected value of a bet.
        
        Args:
            implied_prob: Implied probability from odds
            model_prob: Model's predicted probability
            
        Returns:
            Expected value (positive = value bet)
        """
        if implied_prob == 0:
            return 0.0
        
        # Calculate decimal odds from implied probability
        decimal_odds = 1.0 / implied_prob
        
        # Expected value = (model_prob * decimal_odds) - 1
        ev = (model_prob * decimal_odds) - 1.0
        return round(ev, 4)
    
    def get_all_betting_features(
        self,
        game_id: str,
        home_team_id: str,
        away_team_id: str
    ) -> Dict[str, Optional[float]]:
        """
        Get all betting features for a game.
        
        Args:
            game_id: Game identifier
            home_team_id: Home team identifier
            away_team_id: Away team identifier
            
        Returns:
            Dictionary of all betting features
        """
        consensus_spread = self.get_consensus_spread(game_id)
        consensus_total = self.get_consensus_total(game_id)
        home_ml_prob = self.get_moneyline_implied_prob(game_id, home_team_id)
        away_ml_prob = self.get_moneyline_implied_prob(game_id, away_team_id)
        
        return {
            'consensus_spread': consensus_spread,
            'consensus_total': consensus_total,
            'home_moneyline_prob': home_ml_prob,
            'away_moneyline_prob': away_ml_prob,
            'spread_implied_prob': self.get_spread_implied_prob(game_id, home_team_id),
            'over_implied_prob': self.get_total_implied_prob(game_id, over=True),
            'under_implied_prob': self.get_total_implied_prob(game_id, over=False)
        }
    
    def _american_to_probability(self, american_odds: int) -> Optional[float]:
        """
        Convert American odds to probability.
        
        Args:
            american_odds: American odds (e.g., -150, +130)
            
        Returns:
            Probability (0-1) or None if invalid
        """
        try:
            if american_odds > 0:
                # Positive odds: prob = 100 / (odds + 100)
                prob = 100 / (american_odds + 100)
            else:
                # Negative odds: prob = |odds| / (|odds| + 100)
                prob = abs(american_odds) / (abs(american_odds) + 100)
            
            return prob
        except (ValueError, ZeroDivisionError):
            return None

