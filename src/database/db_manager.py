"""Database manager for NBA prediction model."""

import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from contextlib import contextmanager

from config.settings import get_settings
from src.database.models import (
    Base, Team, Game, TeamStats, PlayerStats, BettingLine,
    Feature, Prediction, Bet
)

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections and operations."""

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize database manager.
        
        Args:
            database_url: Optional database URL. If not provided, uses settings.
        """
        settings = get_settings()
        
        if database_url:
            self.database_url = database_url
        elif settings.DATABASE_TYPE == 'postgresql':
            # PostgreSQL connection string
            db_user = get_settings().DATABASE_USER if hasattr(get_settings(), 'DATABASE_USER') else 'nba_user'
            db_password = get_settings().DATABASE_PASSWORD if hasattr(get_settings(), 'DATABASE_PASSWORD') else 'nba_password'
            db_host = get_settings().DATABASE_HOST if hasattr(get_settings(), 'DATABASE_HOST') else 'localhost'
            db_port = get_settings().DATABASE_PORT if hasattr(get_settings(), 'DATABASE_PORT') else '5432'
            db_name = get_settings().DATABASE_NAME if hasattr(get_settings(), 'DATABASE_NAME') else 'nba_predictions'
            
            self.database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        else:
            # SQLite fallback
            self.database_url = f"sqlite:///{settings.DATABASE_PATH}"
        
        self.engine = create_engine(
            self.database_url,
            echo=False,  # Set to True for SQL debugging
            pool_pre_ping=True,  # Verify connections before using
            pool_size=10,
            max_overflow=20
        )
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False  # Keep objects accessible after commit
        )
        
        logger.info(f"Database manager initialized with URL: {self._mask_password(self.database_url)}")

    def _mask_password(self, url: str) -> str:
        """Mask password in database URL for logging."""
        if '@' in url:
            parts = url.split('@')
            if '://' in parts[0]:
                protocol_user = parts[0].split('://')
                if ':' in protocol_user[1]:
                    user_pass = protocol_user[1].split(':')
                    return f"{protocol_user[0]}://{user_pass[0]}:****@{parts[1]}"
        return url

    def create_tables(self):
        """Create all database tables."""
        try:
            Base.metadata.create_all(self.engine)
            logger.info("Database tables created successfully")
        except SQLAlchemyError as e:
            logger.error(f"Error creating tables: {e}")
            raise

    def drop_tables(self):
        """Drop all database tables (use with caution!)."""
        try:
            Base.metadata.drop_all(self.engine)
            logger.warning("All database tables dropped")
        except SQLAlchemyError as e:
            logger.error(f"Error dropping tables: {e}")
            raise

    @contextmanager
    def get_session(self):
        """Context manager for database sessions."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()

    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
            return True
        except SQLAlchemyError as e:
            logger.error(f"Database connection failed: {e}")
            return False

    # Team operations
    def insert_team(self, team_data: Dict[str, Any]) -> Team:
        """Insert or update a team."""
        with self.get_session() as session:
            team = session.query(Team).filter_by(team_id=team_data['team_id']).first()
            if team:
                # Update existing team
                for key, value in team_data.items():
                    setattr(team, key, value)
                team = team
            else:
                # Create new team
                team = Team(**team_data)
                session.add(team)
            return team

    def get_team(self, team_id: str) -> Optional[Team]:
        """Get team by ID."""
        with self.get_session() as session:
            return session.query(Team).filter_by(team_id=team_id).first()

    def get_all_teams(self) -> List[Team]:
        """Get all teams."""
        with self.get_session() as session:
            return session.query(Team).all()

    # Game operations
    def insert_game(self, game_data: Dict[str, Any]) -> Game:
        """Insert or update a game."""
        with self.get_session() as session:
            game = session.query(Game).filter_by(game_id=game_data['game_id']).first()
            if game:
                # Update existing game
                for key, value in game_data.items():
                    setattr(game, key, value)
                game.updated_at = datetime.now()
            else:
                # Create new game
                game = Game(**game_data)
                session.add(game)
            return game

    def get_game(self, game_id: str) -> Optional[Game]:
        """Get game by ID."""
        with self.get_session() as session:
            return session.query(Game).filter_by(game_id=game_id).first()

    def get_games(
        self,
        season: Optional[str] = None,
        season_type: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        team_id: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Game]:
        """Query games with filters."""
        with self.get_session() as session:
            query = session.query(Game)
            
            if season:
                query = query.filter(Game.season == season)
            if season_type:
                query = query.filter(Game.season_type == season_type)
            if start_date:
                query = query.filter(Game.game_date >= start_date)
            if end_date:
                query = query.filter(Game.game_date <= end_date)
            if team_id:
                query = query.filter(
                    (Game.home_team_id == team_id) | (Game.away_team_id == team_id)
                )
            
            query = query.order_by(Game.game_date.desc())
            
            if limit:
                query = query.limit(limit)
            
            return query.all()

    # Team stats operations
    def insert_team_stats(self, stats_data: Dict[str, Any]) -> TeamStats:
        """Insert or update team stats for a game."""
        with self.get_session() as session:
            stats = session.query(TeamStats).filter_by(
                game_id=stats_data['game_id'],
                team_id=stats_data['team_id']
            ).first()
            
            if stats:
                # Update existing stats
                for key, value in stats_data.items():
                    setattr(stats, key, value)
            else:
                # Create new stats
                stats = TeamStats(**stats_data)
                session.add(stats)
            
            return stats

    def get_team_stats(self, game_id: str, team_id: str) -> Optional[TeamStats]:
        """Get team stats for a specific game."""
        with self.get_session() as session:
            return session.query(TeamStats).filter_by(
                game_id=game_id,
                team_id=team_id
            ).first()

    def get_team_stats_history(
        self,
        team_id: str,
        games_back: int = 10,
        end_date: Optional[date] = None
    ) -> List[TeamStats]:
        """Get recent team stats history."""
        with self.get_session() as session:
            query = session.query(TeamStats).join(Game).filter(
                TeamStats.team_id == team_id
            )
            
            if end_date:
                query = query.filter(Game.game_date <= end_date)
            
            query = query.order_by(Game.game_date.desc()).limit(games_back)
            
            return query.all()

    # Player stats operations
    def insert_player_stats(self, stats_data: Dict[str, Any]) -> PlayerStats:
        """Insert or update player stats for a game."""
        with self.get_session() as session:
            stats = session.query(PlayerStats).filter_by(
                game_id=stats_data['game_id'],
                player_id=stats_data['player_id']
            ).first()
            
            if stats:
                # Update existing stats
                for key, value in stats_data.items():
                    setattr(stats, key, value)
            else:
                # Create new stats
                stats = PlayerStats(**stats_data)
                session.add(stats)
            
            return stats

    def get_player_stats(self, game_id: str, player_id: str) -> Optional[PlayerStats]:
        """Get player stats for a specific game."""
        with self.get_session() as session:
            return session.query(PlayerStats).filter_by(
                game_id=game_id,
                player_id=player_id
            ).first()

    # Betting lines operations
    def insert_betting_line(self, line_data: Dict[str, Any]) -> BettingLine:
        """Insert a betting line."""
        with self.get_session() as session:
            line = BettingLine(**line_data)
            session.add(line)
            return line

    def get_betting_lines(
        self,
        game_id: str,
        sportsbook: Optional[str] = None
    ) -> List[BettingLine]:
        """Get betting lines for a game."""
        with self.get_session() as session:
            query = session.query(BettingLine).filter_by(game_id=game_id)
            
            if sportsbook:
                query = query.filter_by(sportsbook=sportsbook)
            
            query = query.order_by(BettingLine.timestamp.desc())
            
            return query.all()

    def get_latest_betting_line(
        self,
        game_id: str,
        sportsbook: Optional[str] = None
    ) -> Optional[BettingLine]:
        """Get the latest betting line for a game."""
        lines = self.get_betting_lines(game_id, sportsbook)
        return lines[0] if lines else None

    # Features operations
    def insert_feature(self, feature_data: Dict[str, Any]) -> Feature:
        """Insert or update a feature."""
        with self.get_session() as session:
            feature = session.query(Feature).filter_by(
                game_id=feature_data['game_id'],
                feature_name=feature_data['feature_name']
            ).first()
            
            if feature:
                # Update existing feature
                for key, value in feature_data.items():
                    setattr(feature, key, value)
            else:
                # Create new feature
                feature = Feature(**feature_data)
                session.add(feature)
            
            return feature

    def get_features(
        self,
        game_id: str,
        category: Optional[str] = None,
        team_id: Optional[str] = None
    ) -> List[Feature]:
        """Get features for a game."""
        with self.get_session() as session:
            query = session.query(Feature).filter_by(game_id=game_id)
            
            if category:
                query = query.filter_by(feature_category=category)
            if team_id:
                query = query.filter_by(team_id=team_id)
            
            return query.all()

    def get_feature_vector(self, game_id: str) -> Dict[str, float]:
        """Get all features for a game as a dictionary."""
        features = self.get_features(game_id)
        return {f.feature_name: f.feature_value for f in features if f.feature_value is not None}

    # Prediction operations
    def insert_prediction(self, prediction_data: Dict[str, Any]) -> Prediction:
        """Insert or update a prediction."""
        with self.get_session() as session:
            prediction = session.query(Prediction).filter_by(
                game_id=prediction_data['game_id'],
                model_name=prediction_data['model_name']
            ).first()
            
            if prediction:
                # Update existing prediction
                for key, value in prediction_data.items():
                    setattr(prediction, key, value)
            else:
                # Create new prediction
                prediction = Prediction(**prediction_data)
                session.add(prediction)
            
            return prediction

    def get_predictions(
        self,
        game_id: Optional[str] = None,
        model_name: Optional[str] = None
    ) -> List[Prediction]:
        """Get predictions."""
        with self.get_session() as session:
            query = session.query(Prediction)
            
            if game_id:
                query = query.filter_by(game_id=game_id)
            if model_name:
                query = query.filter_by(model_name=model_name)
            
            query = query.order_by(Prediction.created_at.desc())
            
            return query.all()

    # Bet operations
    def insert_bet(self, bet_data: Dict[str, Any]) -> Bet:
        """Insert a bet."""
        with self.get_session() as session:
            bet = Bet(**bet_data)
            session.add(bet)
            return bet

    def get_bets(
        self,
        game_id: Optional[str] = None,
        outcome: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Bet]:
        """Get bets with filters."""
        with self.get_session() as session:
            query = session.query(Bet)
            
            if game_id:
                query = query.filter_by(game_id=game_id)
            if outcome:
                query = query.filter_by(outcome=outcome)
            if start_date:
                query = query.filter(Bet.placed_at >= start_date)
            if end_date:
                query = query.filter(Bet.placed_at <= end_date)
            
            query = query.order_by(Bet.placed_at.desc())
            
            return query.all()

    def update_bet_outcome(
        self,
        bet_id: int,
        outcome: str,
        payout: Optional[float] = None,
        profit: Optional[float] = None
    ) -> Bet:
        """Update bet outcome after game completion."""
        with self.get_session() as session:
            bet = session.query(Bet).filter_by(id=bet_id).first()
            if bet:
                bet.outcome = outcome
                bet.resolved_at = datetime.now()
                if payout is not None:
                    bet.payout = payout
                if profit is not None:
                    bet.profit = profit
            return bet

