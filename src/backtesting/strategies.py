"""Betting strategies for forward testing."""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import numpy as np

logger = logging.getLogger(__name__)


class BettingStrategy(ABC):
    """Abstract base class for betting strategies."""
    
    @abstractmethod
    def should_bet(
        self,
        prediction: Dict[str, Any],
        odds: Dict[str, Any],
        bankroll: float,
        game: Any = None
    ) -> Optional[Dict[str, Any]]:
        """
        Determine if a bet should be placed.
        
        Args:
            prediction: Prediction dictionary with 'predicted_winner', 'confidence', etc.
            odds: Odds dictionary with 'moneyline_home', 'moneyline_away', etc.
            bankroll: Current bankroll amount
            
        Returns:
            Bet dictionary with 'bet_type', 'bet_team', 'bet_amount', 'odds', 'expected_value'
            or None if no bet should be placed
        """
        pass


class ConfidenceThresholdStrategy(BettingStrategy):
    """
    Simple strategy: bet when confidence exceeds threshold.
    """
    
    def __init__(
        self,
        confidence_threshold: float = 0.60,
        bet_amount: float = 100.0,
        min_confidence: float = 0.55
    ):
        """
        Initialize confidence threshold strategy.
        
        Args:
            confidence_threshold: Minimum confidence to place bet
            bet_amount: Fixed bet amount
            min_confidence: Absolute minimum confidence (safety check)
        """
        self.confidence_threshold = confidence_threshold
        self.bet_amount = bet_amount
        self.min_confidence = min_confidence
    
    def should_bet(
        self,
        prediction: Dict[str, Any],
        odds: Dict[str, Any],
        bankroll: float,
        game: Any = None
    ) -> Optional[Dict[str, Any]]:
        """Bet if confidence exceeds threshold."""
        confidence = prediction.get('confidence', 0.0)
        predicted_winner = prediction.get('predicted_winner')
        
        if confidence < self.confidence_threshold:
            return None
        
        if confidence < self.min_confidence:
            return None
        
        if not game:
            logger.warning("Game object required for strategy")
            return None
        
        # Get odds for predicted winner
        if predicted_winner == game.home_team_id:
            moneyline = odds.get('moneyline_home')
        else:
            moneyline = odds.get('moneyline_away')
        
        if not moneyline:
            logger.warning("No moneyline odds available")
            return None
        
        # Convert American odds to decimal
        if moneyline > 0:
            decimal_odds = (moneyline / 100) + 1
        else:
            decimal_odds = (100 / abs(moneyline)) + 1
        
        # Calculate expected value
        expected_value = (confidence * (decimal_odds - 1)) - ((1 - confidence) * 1)
        
        return {
            'bet_type': 'moneyline',
            'bet_team': predicted_winner,
            'bet_amount': min(self.bet_amount, bankroll * 0.05),  # Max 5% of bankroll
            'odds': decimal_odds,
            'expected_value': expected_value,
            'confidence': confidence
        }


class ExpectedValueStrategy(BettingStrategy):
    """
    Strategy: bet when expected value is positive.
    """
    
    def __init__(
        self,
        min_ev: float = 0.05,
        bet_fraction: float = 0.02,
        max_bet: float = 500.0
    ):
        """
        Initialize expected value strategy.
        
        Args:
            min_ev: Minimum expected value to place bet
            bet_fraction: Fraction of bankroll to bet
            max_bet: Maximum bet amount
        """
        self.min_ev = min_ev
        self.bet_fraction = bet_fraction
        self.max_bet = max_bet
    
    def should_bet(
        self,
        prediction: Dict[str, Any],
        odds: Dict[str, Any],
        bankroll: float,
        game: Any = None
    ) -> Optional[Dict[str, Any]]:
        """Bet if expected value is positive and above threshold."""
        confidence = prediction.get('confidence', 0.0)
        predicted_winner = prediction.get('predicted_winner')
        
        if not predicted_winner or not game:
            return None
        
        # Get odds for predicted winner
        if predicted_winner == game.home_team_id:
            moneyline = odds.get('moneyline_home')
        else:
            moneyline = odds.get('moneyline_away')
        
        if not moneyline:
            return None
        
        # Convert to decimal
        if moneyline > 0:
            decimal_odds = (moneyline / 100) + 1
        else:
            decimal_odds = (100 / abs(moneyline)) + 1
        
        # Calculate expected value
        expected_value = (confidence * (decimal_odds - 1)) - ((1 - confidence) * 1)
        
        if expected_value < self.min_ev:
            return None
        
        # Calculate bet amount based on EV and bankroll
        bet_amount = min(
            bankroll * self.bet_fraction,
            self.max_bet,
            bankroll * 0.05  # Never bet more than 5% of bankroll
        )
        
        return {
            'bet_type': 'moneyline',
            'bet_team': predicted_winner,
            'bet_amount': bet_amount,
            'odds': decimal_odds,
            'expected_value': expected_value,
            'confidence': confidence
        }


class KellyCriterionStrategy(BettingStrategy):
    """
    Strategy using Kelly Criterion for optimal bet sizing.
    """
    
    def __init__(
        self,
        kelly_fraction: float = 0.25,  # Use 25% of Kelly to be conservative
        min_confidence: float = 0.55
    ):
        """
        Initialize Kelly Criterion strategy.
        
        Args:
            kelly_fraction: Fraction of full Kelly to use (0.25 = quarter Kelly)
            min_confidence: Minimum confidence to place bet
        """
        self.kelly_fraction = kelly_fraction
        self.min_confidence = min_confidence
    
    def should_bet(
        self,
        prediction: Dict[str, Any],
        odds: Dict[str, Any],
        bankroll: float,
        game: Any = None
    ) -> Optional[Dict[str, Any]]:
        """Bet using Kelly Criterion for optimal sizing."""
        confidence = prediction.get('confidence', 0.0)
        predicted_winner = prediction.get('predicted_winner')
        
        if confidence < self.min_confidence or not game:
            return None
        
        # Get odds for predicted winner
        if predicted_winner == game.home_team_id:
            moneyline = odds.get('moneyline_home')
        else:
            moneyline = odds.get('moneyline_away')
        
        if not moneyline:
            return None
        
        # Convert to decimal
        if moneyline > 0:
            decimal_odds = (moneyline / 100) + 1
        else:
            decimal_odds = (100 / abs(moneyline)) + 1
        
        # Kelly Criterion: f = (bp - q) / b
        # where b = odds - 1, p = win probability, q = loss probability
        b = decimal_odds - 1
        p = confidence
        q = 1 - confidence
        
        kelly_percentage = (b * p - q) / b
        
        if kelly_percentage <= 0:
            return None  # Negative or zero Kelly = no bet
        
        # Use fractional Kelly
        bet_fraction = kelly_percentage * self.kelly_fraction
        
        # Cap at 5% of bankroll
        bet_fraction = min(bet_fraction, 0.05)
        
        bet_amount = bankroll * bet_fraction
        
        # Minimum bet check
        if bet_amount < 10.0:
            return None
        
        expected_value = (confidence * (decimal_odds - 1)) - ((1 - confidence) * 1)
        
        return {
            'bet_type': 'moneyline',
            'bet_team': predicted_winner,
            'bet_amount': bet_amount,
            'odds': decimal_odds,
            'expected_value': expected_value,
            'confidence': confidence,
            'kelly_percentage': kelly_percentage
        }

