"""SQLAlchemy models for NBA prediction database."""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Date, DateTime, Text,
    ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()


class Team(Base):
    """Team metadata table."""
    __tablename__ = 'teams'

    team_id = Column(String, primary_key=True, comment="Team identifier (e.g., '1610612737')")
    team_name = Column(String, nullable=False, comment="Full team name")
    team_abbreviation = Column(String, nullable=False, comment="Team abbreviation (e.g., 'ATL')")
    city = Column(String, comment="Team city")
    conference = Column(String, comment="Eastern or Western")
    division = Column(String, comment="Division name")
    created_at = Column(DateTime, default=func.now(), nullable=False)

    # Relationships
    home_games = relationship("Game", foreign_keys="Game.home_team_id", back_populates="home_team")
    away_games = relationship("Game", foreign_keys="Game.away_team_id", back_populates="away_team")
    team_stats = relationship("TeamStats", back_populates="team")
    player_stats = relationship("PlayerStats", back_populates="team")

    def __repr__(self):
        return f"<Team(team_id={self.team_id}, name={self.team_name})>"


class Game(Base):
    """Game results and metadata table."""
    __tablename__ = 'games'

    game_id = Column(String, primary_key=True, comment="Unique game identifier (e.g., '0022300123')")
    season = Column(String, nullable=False, comment="Season year (e.g., '2023-24')")
    season_type = Column(String, nullable=False, comment="Regular Season, Playoffs, etc.")
    game_date = Column(Date, nullable=False, comment="Date of the game")
    home_team_id = Column(String, ForeignKey('teams.team_id'), nullable=False, comment="Home team identifier")
    away_team_id = Column(String, ForeignKey('teams.team_id'), nullable=False, comment="Away team identifier")
    home_score = Column(Integer, nullable=True, comment="Home team final score")
    away_score = Column(Integer, nullable=True, comment="Away team final score")
    winner = Column(String, nullable=True, comment="Winning team ID (home or away)")
    point_differential = Column(Integer, nullable=True, comment="Home score - Away score")
    game_status = Column(String, default='scheduled', nullable=False, comment="scheduled, live, finished, postponed")
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    home_team = relationship("Team", foreign_keys=[home_team_id], back_populates="home_games")
    away_team = relationship("Team", foreign_keys=[away_team_id], back_populates="away_games")
    team_stats = relationship("TeamStats", back_populates="game", cascade="all, delete-orphan")
    player_stats = relationship("PlayerStats", back_populates="game", cascade="all, delete-orphan")
    betting_lines = relationship("BettingLine", back_populates="game", cascade="all, delete-orphan")
    features = relationship("Feature", back_populates="game", cascade="all, delete-orphan")
    predictions = relationship("Prediction", back_populates="game", cascade="all, delete-orphan")
    bets = relationship("Bet", back_populates="game", cascade="all, delete-orphan")

    # Indexes
    __table_args__ = (
        Index('idx_game_date', 'game_date'),
        Index('idx_home_team_date', 'home_team_id', 'game_date'),
        Index('idx_away_team_date', 'away_team_id', 'game_date'),
        Index('idx_season_type', 'season', 'season_type'),
    )

    def __repr__(self):
        return f"<Game(game_id={self.game_id}, date={self.game_date}, {self.away_team_id} @ {self.home_team_id})>"


class TeamStats(Base):
    """Team statistics per game table."""
    __tablename__ = 'team_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String, ForeignKey('games.game_id'), nullable=False, comment="Game identifier")
    team_id = Column(String, ForeignKey('teams.team_id'), nullable=False, comment="Team identifier")
    is_home = Column(Boolean, nullable=False, comment="True if home team")

    # Scoring
    points = Column(Integer, nullable=False, comment="Total points scored")
    field_goals_made = Column(Integer, nullable=False, comment="FGM")
    field_goals_attempted = Column(Integer, nullable=False, comment="FGA")
    field_goal_percentage = Column(Float, nullable=False, comment="FG%")
    three_pointers_made = Column(Integer, nullable=False, comment="3PM")
    three_pointers_attempted = Column(Integer, nullable=False, comment="3PA")
    three_point_percentage = Column(Float, nullable=False, comment="3P%")
    free_throws_made = Column(Integer, nullable=False, comment="FTM")
    free_throws_attempted = Column(Integer, nullable=False, comment="FTA")
    free_throw_percentage = Column(Float, nullable=False, comment="FT%")

    # Rebounding
    rebounds_offensive = Column(Integer, nullable=False, comment="ORB")
    rebounds_defensive = Column(Integer, nullable=False, comment="DRB")
    rebounds_total = Column(Integer, nullable=False, comment="Total rebounds")

    # Other stats
    assists = Column(Integer, nullable=False, comment="Assists")
    steals = Column(Integer, nullable=False, comment="Steals")
    blocks = Column(Integer, nullable=False, comment="Blocks")
    turnovers = Column(Integer, nullable=False, comment="Turnovers")
    personal_fouls = Column(Integer, nullable=False, comment="Personal fouls")

    # Advanced metrics (calculated)
    offensive_rating = Column(Float, nullable=True, comment="Points per 100 possessions")
    defensive_rating = Column(Float, nullable=True, comment="Points allowed per 100 possessions")
    pace = Column(Float, nullable=True, comment="Possessions per game")
    true_shooting_percentage = Column(Float, nullable=True, comment="TS%")
    effective_field_goal_percentage = Column(Float, nullable=True, comment="eFG%")

    created_at = Column(DateTime, default=func.now(), nullable=False)

    # Relationships
    game = relationship("Game", back_populates="team_stats")
    team = relationship("Team", back_populates="team_stats")

    # Indexes
    __table_args__ = (
        UniqueConstraint('game_id', 'team_id', name='uq_game_team'),
        Index('idx_team_game', 'team_id', 'game_id'),
        Index('idx_game_id', 'game_id'),
    )

    def __repr__(self):
        return f"<TeamStats(game_id={self.game_id}, team_id={self.team_id}, points={self.points})>"


class PlayerStats(Base):
    """Individual player statistics per game table."""
    __tablename__ = 'player_stats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String, ForeignKey('games.game_id'), nullable=False, comment="Game identifier")
    player_id = Column(String, nullable=False, comment="Player identifier")
    team_id = Column(String, ForeignKey('teams.team_id'), nullable=False, comment="Team identifier")
    player_name = Column(String, nullable=False, comment="Player full name")
    minutes_played = Column(String, nullable=False, comment="Minutes played (format: MM:SS)")
    points = Column(Integer, nullable=False, comment="Points scored")
    rebounds = Column(Integer, nullable=False, comment="Total rebounds")
    assists = Column(Integer, nullable=False, comment="Assists")
    field_goals_made = Column(Integer, nullable=False, comment="FGM")
    field_goals_attempted = Column(Integer, nullable=False, comment="FGA")
    three_pointers_made = Column(Integer, nullable=False, comment="3PM")
    three_pointers_attempted = Column(Integer, nullable=False, comment="3PA")
    free_throws_made = Column(Integer, nullable=False, comment="FTM")
    free_throws_attempted = Column(Integer, nullable=False, comment="FTA")
    plus_minus = Column(Integer, nullable=True, comment="Plus/minus")
    injury_status = Column(String, default='healthy', nullable=True, comment="healthy, questionable, out, etc.")
    created_at = Column(DateTime, default=func.now(), nullable=False)

    # Relationships
    game = relationship("Game", back_populates="player_stats")
    team = relationship("Team", back_populates="player_stats")

    # Indexes
    __table_args__ = (
        UniqueConstraint('game_id', 'player_id', name='uq_game_player'),
        Index('idx_player_game', 'player_id', 'game_id'),
        Index('idx_team_game_player', 'team_id', 'game_id'),
    )

    def __repr__(self):
        return f"<PlayerStats(game_id={self.game_id}, player={self.player_name}, points={self.points})>"


class BettingLine(Base):
    """Betting odds and lines table."""
    __tablename__ = 'betting_lines'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String, ForeignKey('games.game_id'), nullable=False, comment="Game identifier")
    sportsbook = Column(String, nullable=False, comment="Sportsbook name (e.g., 'draftkings', 'fanduel')")
    point_spread_home = Column(Float, nullable=True, comment="Point spread for home team")
    point_spread_away = Column(Float, nullable=True, comment="Point spread for away team")
    moneyline_home = Column(Integer, nullable=True, comment="Moneyline odds for home team (American format)")
    moneyline_away = Column(Integer, nullable=True, comment="Moneyline odds for away team (American format)")
    over_under = Column(Float, nullable=True, comment="Total points over/under line")
    timestamp = Column(DateTime, default=func.now(), nullable=False, comment="When the line was fetched")
    created_at = Column(DateTime, default=func.now(), nullable=False)

    # Relationships
    game = relationship("Game", back_populates="betting_lines")

    # Indexes
    __table_args__ = (
        Index('idx_game_sportsbook_timestamp', 'game_id', 'sportsbook', 'timestamp'),
        Index('idx_game_timestamp', 'game_id', 'timestamp'),
        Index('idx_timestamp', 'timestamp'),
    )

    def __repr__(self):
        return f"<BettingLine(game_id={self.game_id}, sportsbook={self.sportsbook}, spread={self.point_spread_home})>"


class Feature(Base):
    """Engineered features for modeling table."""
    __tablename__ = 'features'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String, ForeignKey('games.game_id'), nullable=False, comment="Game identifier")
    feature_name = Column(String, nullable=False, comment="Name of the feature")
    feature_value = Column(Float, nullable=True, comment="Feature value")
    feature_category = Column(String, nullable=False, comment="team, matchup, contextual, betting")
    team_id = Column(String, ForeignKey('teams.team_id'), nullable=True, comment="Team ID if feature is team-specific")
    created_at = Column(DateTime, default=func.now(), nullable=False)

    # Relationships
    game = relationship("Game", back_populates="features")
    team = relationship("Team")

    # Indexes
    __table_args__ = (
        UniqueConstraint('game_id', 'feature_name', name='uq_game_feature'),
        Index('idx_game_category', 'game_id', 'feature_category'),
        Index('idx_feature_category', 'feature_category'),
    )

    def __repr__(self):
        return f"<Feature(game_id={self.game_id}, name={self.feature_name}, value={self.feature_value})>"


class Prediction(Base):
    """Model predictions table."""
    __tablename__ = 'predictions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String, ForeignKey('games.game_id'), nullable=False, comment="Game identifier")
    model_name = Column(String, nullable=False, comment="Name of the model used")
    predicted_winner = Column(String, ForeignKey('teams.team_id'), nullable=True, comment="Predicted winning team")
    win_probability_home = Column(Float, nullable=False, comment="Probability home team wins (0-1)")
    win_probability_away = Column(Float, nullable=False, comment="Probability away team wins (0-1)")
    predicted_point_differential = Column(Float, nullable=True, comment="Predicted point differential (home - away)")
    confidence = Column(Float, nullable=False, comment="Model confidence (max of win probabilities)")
    created_at = Column(DateTime, default=func.now(), nullable=False)

    # Relationships
    game = relationship("Game", back_populates="predictions")
    predicted_winner_team = relationship("Team", foreign_keys=[predicted_winner])

    # Indexes
    __table_args__ = (
        UniqueConstraint('game_id', 'model_name', name='uq_game_model'),
        Index('idx_game_id_pred', 'game_id'),
        Index('idx_created_at', 'created_at'),
    )

    def __repr__(self):
        return f"<Prediction(game_id={self.game_id}, model={self.model_name}, winner={self.predicted_winner})>"


class TeamRollingFeatures(Base):
    """
    Model-ready rolling features table - one row per team per game.
    Contains pre-computed rolling averages for fast training and inference.
    """
    __tablename__ = 'team_rolling_features'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String, ForeignKey('games.game_id'), nullable=False)
    team_id = Column(String, ForeignKey('teams.team_id'), nullable=False)
    is_home = Column(Boolean, nullable=False)
    game_date = Column(Date, nullable=False, index=True)
    season = Column(String, nullable=False)
    
    # Last 5 games rolling averages
    l5_points = Column(Float, nullable=True)
    l5_points_allowed = Column(Float, nullable=True)
    l5_fg_pct = Column(Float, nullable=True)
    l5_three_pct = Column(Float, nullable=True)
    l5_ft_pct = Column(Float, nullable=True)
    l5_rebounds = Column(Float, nullable=True)
    l5_assists = Column(Float, nullable=True)
    l5_turnovers = Column(Float, nullable=True)
    l5_steals = Column(Float, nullable=True)
    l5_blocks = Column(Float, nullable=True)
    l5_win_pct = Column(Float, nullable=True)
    
    # Last 10 games rolling averages
    l10_points = Column(Float, nullable=True)
    l10_points_allowed = Column(Float, nullable=True)
    l10_fg_pct = Column(Float, nullable=True)
    l10_three_pct = Column(Float, nullable=True)
    l10_ft_pct = Column(Float, nullable=True)
    l10_rebounds = Column(Float, nullable=True)
    l10_assists = Column(Float, nullable=True)
    l10_turnovers = Column(Float, nullable=True)
    l10_steals = Column(Float, nullable=True)
    l10_blocks = Column(Float, nullable=True)
    l10_win_pct = Column(Float, nullable=True)
    
    # Last 20 games (season average) rolling averages
    l20_points = Column(Float, nullable=True)
    l20_points_allowed = Column(Float, nullable=True)
    l20_fg_pct = Column(Float, nullable=True)
    l20_three_pct = Column(Float, nullable=True)
    l20_win_pct = Column(Float, nullable=True)
    
    # Advanced metrics (pace-adjusted)
    offensive_rating = Column(Float, nullable=True)  # Points per 100 possessions
    defensive_rating = Column(Float, nullable=True)  # Points allowed per 100 possessions
    net_rating = Column(Float, nullable=True)  # Off rating - Def rating
    pace = Column(Float, nullable=True)  # Possessions per game
    efg_pct = Column(Float, nullable=True)  # Effective FG%
    ts_pct = Column(Float, nullable=True)  # True shooting %
    tov_pct = Column(Float, nullable=True)  # Turnover %
    
    # Additional advanced metrics
    offensive_rebound_rate = Column(Float, nullable=True)  # Offensive rebound rate
    defensive_rebound_rate = Column(Float, nullable=True)  # Defensive rebound rate
    assist_rate = Column(Float, nullable=True)  # Assist rate
    steal_rate = Column(Float, nullable=True)  # Steal rate
    block_rate = Column(Float, nullable=True)  # Block rate
    
    # Average stats (last 10 games)
    avg_point_differential = Column(Float, nullable=True)  # Average point differential
    avg_points_for = Column(Float, nullable=True)  # Average points scored
    avg_points_against = Column(Float, nullable=True)  # Average points allowed
    
    # Streaks
    win_streak = Column(Integer, nullable=True)  # Current win streak
    loss_streak = Column(Integer, nullable=True)  # Current loss streak
    
    # Injury features
    players_out = Column(Integer, nullable=True)  # Number of players out
    players_questionable = Column(Integer, nullable=True)  # Number of players questionable
    injury_severity_score = Column(Float, nullable=True)  # Injury severity (0-1)
    
    # Contextual features
    days_rest = Column(Integer, nullable=True)
    is_back_to_back = Column(Boolean, nullable=True)
    games_in_last_7_days = Column(Integer, nullable=True)
    home_win_pct = Column(Float, nullable=True)
    away_win_pct = Column(Float, nullable=True)
    
    # Target variable (stored separately for clarity)
    won_game = Column(Boolean, nullable=True)  # Only set after game completes
    point_differential = Column(Integer, nullable=True)
    
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    game = relationship("Game")
    team = relationship("Team")
    
    # Indexes
    __table_args__ = (
        UniqueConstraint('game_id', 'team_id', name='uq_rolling_game_team'),
        Index('idx_rolling_team_date', 'team_id', 'game_date'),
        Index('idx_rolling_season', 'season'),
        Index('idx_rolling_game_date', 'game_date'),
    )
    
    def __repr__(self):
        return f"<TeamRollingFeatures(game_id={self.game_id}, team_id={self.team_id})>"


class GameMatchupFeatures(Base):
    """
    Game-level matchup features table - one row per game.
    Contains head-to-head, style matchup, and contextual features.
    """
    __tablename__ = 'game_matchup_features'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String, ForeignKey('games.game_id'), nullable=False, unique=True)
    game_date = Column(Date, nullable=False, index=True)
    season = Column(String, nullable=False)
    home_team_id = Column(String, ForeignKey('teams.team_id'), nullable=False)
    away_team_id = Column(String, ForeignKey('teams.team_id'), nullable=False)
    
    # Head-to-head features
    h2h_home_wins = Column(Integer, nullable=True)  # Home team wins in last 5 H2H
    h2h_away_wins = Column(Integer, nullable=True)  # Away team wins in last 5 H2H
    h2h_total_games = Column(Integer, nullable=True)  # Total H2H games
    h2h_avg_point_differential = Column(Float, nullable=True)  # H2H point differential
    h2h_home_avg_score = Column(Float, nullable=True)  # Home team avg score in H2H
    h2h_away_avg_score = Column(Float, nullable=True)  # Away team avg score in H2H
    
    # Style matchup features
    pace_differential = Column(Float, nullable=True)  # Pace difference
    ts_differential = Column(Float, nullable=True)  # True shooting % difference
    efg_differential = Column(Float, nullable=True)  # Effective FG% difference
    
    # Recent form comparison
    home_win_pct_recent = Column(Float, nullable=True)  # Home team recent win %
    away_win_pct_recent = Column(Float, nullable=True)  # Away team recent win %
    win_pct_differential = Column(Float, nullable=True)  # Win % difference
    
    # Contextual features
    same_conference = Column(Boolean, nullable=True)  # Same conference
    same_division = Column(Boolean, nullable=True)  # Same division
    is_playoffs = Column(Boolean, nullable=True)  # Is playoff game
    is_home_advantage = Column(Integer, nullable=True, default=1)  # Home advantage (always 1)
    
    # Rest days
    home_rest_days = Column(Integer, nullable=True)
    away_rest_days = Column(Integer, nullable=True)
    rest_days_differential = Column(Integer, nullable=True)
    
    # Back-to-back
    home_is_b2b = Column(Boolean, nullable=True)
    away_is_b2b = Column(Boolean, nullable=True)
    
    # Days until next game
    home_days_until_next = Column(Integer, nullable=True)
    away_days_until_next = Column(Integer, nullable=True)
    
    created_at = Column(DateTime, default=func.now(), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relationships
    game = relationship("Game")
    home_team = relationship("Team", foreign_keys=[home_team_id])
    away_team = relationship("Team", foreign_keys=[away_team_id])
    
    # Indexes
    __table_args__ = (
        Index('idx_matchup_game_date', 'game_date'),
        Index('idx_matchup_season', 'season'),
        Index('idx_matchup_teams', 'home_team_id', 'away_team_id'),
    )
    
    def __repr__(self):
        return f"<GameMatchupFeatures(game_id={self.game_id})>"


class Bet(Base):
    """Betting decisions and outcomes table."""
    __tablename__ = 'bets'

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(String, ForeignKey('games.game_id'), nullable=False, comment="Game identifier")
    bet_type = Column(String, nullable=False, comment="spread, moneyline, over_under")
    bet_team = Column(String, ForeignKey('teams.team_id'), nullable=True, comment="Team bet on (for spread/moneyline)")
    bet_value = Column(Float, nullable=True, comment="Spread value or over/under value")
    bet_amount = Column(Float, nullable=False, comment="Amount wagered")
    odds = Column(Float, nullable=False, comment="Odds (decimal format)")
    expected_value = Column(Float, nullable=False, comment="Expected value of the bet")
    outcome = Column(String, nullable=True, comment="win, loss, push")
    payout = Column(Float, nullable=True, comment="Payout amount (if won)")
    profit = Column(Float, nullable=True, comment="Profit/loss from bet")
    placed_at = Column(DateTime, default=func.now(), nullable=False)
    resolved_at = Column(DateTime, nullable=True)

    # Relationships
    game = relationship("Game", back_populates="bets")
    team = relationship("Team", foreign_keys=[bet_team])

    # Indexes
    __table_args__ = (
        Index('idx_game_id_bet', 'game_id'),
        Index('idx_placed_at', 'placed_at'),
        Index('idx_outcome', 'outcome'),
    )

    def __repr__(self):
        return f"<Bet(game_id={self.game_id}, type={self.bet_type}, amount={self.bet_amount}, outcome={self.outcome})>"

