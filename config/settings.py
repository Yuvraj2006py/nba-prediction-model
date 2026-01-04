"""Central configuration settings for the NBA prediction model."""

import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings:
    """Application settings loaded from environment variables."""
    
    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent
    DATA_DIR = PROJECT_ROOT / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    PROCESSED_DATA_DIR = DATA_DIR / "processed"
    MODELS_DIR = DATA_DIR / "models"
    LOGS_DIR = PROJECT_ROOT / "logs"
    
    # API Configuration
    NBA_API_KEY: Optional[str] = os.getenv("NBA_API_KEY")
    NBA_API_BASE_URL: str = os.getenv("NBA_API_BASE_URL", "https://stats.nba.com/stats")
    
    BETTING_API_KEY: Optional[str] = os.getenv("BETTING_API_KEY")
    BETTING_API_BASE_URL: str = os.getenv("BETTING_API_BASE_URL", "https://api.the-odds-api.com/v4")
    
    # Database Configuration
    DATABASE_TYPE: str = os.getenv("DATABASE_TYPE", "postgresql")
    DATABASE_HOST: str = os.getenv("DATABASE_HOST", "localhost")
    DATABASE_PORT: str = os.getenv("DATABASE_PORT", "5432")
    DATABASE_NAME: str = os.getenv("DATABASE_NAME", "nba_predictions")
    DATABASE_USER: str = os.getenv("DATABASE_USER", "nba_user")
    DATABASE_PASSWORD: str = os.getenv("DATABASE_PASSWORD", "nba_password")
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", str(DATA_DIR / "nba_predictions.db"))  # For SQLite fallback
    
    # Model Configuration
    MODEL_SAVE_PATH: str = os.getenv("MODEL_SAVE_PATH", str(MODELS_DIR))
    FEATURE_CACHE_PATH: str = os.getenv("FEATURE_CACHE_PATH", str(PROCESSED_DATA_DIR / "features"))
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE_PATH: str = os.getenv("LOG_FILE_PATH", str(LOGS_DIR / "nba_predictions.log"))
    
    # Betting Strategy Configuration
    INITIAL_BANKROLL: float = float(os.getenv("INITIAL_BANKROLL", "10000"))
    MAX_BET_PERCENTAGE: float = float(os.getenv("MAX_BET_PERCENTAGE", "0.05"))
    MIN_CONFIDENCE_THRESHOLD: float = float(os.getenv("MIN_CONFIDENCE_THRESHOLD", "0.55"))
    KELLY_FRACTION: float = float(os.getenv("KELLY_FRACTION", "0.25"))
    MIN_BET_SIZE: float = float(os.getenv("MIN_BET_SIZE", "10.0"))
    
    # Data Collection Configuration
    RATE_LIMIT_DELAY: float = float(os.getenv("RATE_LIMIT_DELAY", "1.0"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY: float = float(os.getenv("RETRY_DELAY", "5.0"))
    
    # Basketball Reference Scraping
    BBALL_REF_BASE_URL: str = os.getenv("BBALL_REF_BASE_URL", "https://www.basketball-reference.com")
    SCRAPING_DELAY: float = float(os.getenv("SCRAPING_DELAY", "2.0"))
    
    # Model Training Configuration
    TEST_SIZE: float = float(os.getenv("TEST_SIZE", "0.2"))
    VALIDATION_SIZE: float = float(os.getenv("VALIDATION_SIZE", "0.1"))
    RANDOM_STATE: int = int(os.getenv("RANDOM_STATE", "42"))
    CV_FOLDS: int = int(os.getenv("CV_FOLDS", "5"))
    
    # Feature Engineering Configuration
    DEFAULT_GAMES_BACK: int = int(os.getenv("DEFAULT_GAMES_BACK", "10"))
    FEATURE_CACHE_ENABLED: bool = os.getenv("FEATURE_CACHE_ENABLED", "True").lower() == "true"
    
    # Exponential Decay Weighting for Rolling Statistics
    # λ (lambda) controls how fast old games lose influence
    # Formula: weight_i = e^(-λ * games_ago_i)
    #   - games_ago_i = 0 for most recent game, 1 for previous, etc.
    #   - Higher λ = more emphasis on recent games (faster decay)
    #   - Lower λ = more balanced weighting (slower decay)
    # 
    # Weight distribution at λ = 0.1 (default):
    #   - 5-game window: game 1 = 24.2%, game 5 = 16.2%
    #   - 10-game window: games 1-5 = 60.1%, games 6-10 = 39.9%
    #   - 20-game window: games 1-5 = 60.1%, games 6-10 = 24.8%, games 11-20 = 15.1%
    #
    # Recommended values:
    #   - 0.05: Gentle decay (20 games ago still has ~37% weight of most recent)
    #   - 0.10: Moderate decay [DEFAULT]
    #   - 0.15: Aggressive decay (20 games ago has ~5% weight)
    #   - 0.20: Very aggressive (20 games ago has ~2% weight)
    #   - 0.00: No decay (simple average, backward compatible)
    ROLLING_STATS_DECAY_RATE: float = float(os.getenv("ROLLING_STATS_DECAY_RATE", "0.1"))
    
    # Backtesting Configuration
    COMMISSION_RATE: float = float(os.getenv("COMMISSION_RATE", "0.0"))  # Betting commission
    TRANSACTION_COST: float = float(os.getenv("TRANSACTION_COST", "0.0"))  # Per-bet cost
    
    # RapidAPI Configuration (for injury data)
    RAPIDAPI_NBA_INJURIES_KEY: Optional[str] = os.getenv("RAPIDAPI_NBA_INJURIES_KEY")
    RAPIDAPI_NBA_INJURIES_HOST: str = os.getenv(
        "RAPIDAPI_NBA_INJURIES_HOST", 
        "nba-injuries-reports.p.rapidapi.com"
    )
    
    # Player Importance Configuration
    PLAYER_IMPORTANCE_GAMES_BACK: int = int(os.getenv("PLAYER_IMPORTANCE_GAMES_BACK", "20"))
    TOP_PLAYERS_COUNT: int = int(os.getenv("TOP_PLAYERS_COUNT", "5"))  # Number of "key players"
    
    # Injury Severity Weights
    INJURY_WEIGHT_OUT: float = float(os.getenv("INJURY_WEIGHT_OUT", "1.0"))
    INJURY_WEIGHT_QUESTIONABLE: float = float(os.getenv("INJURY_WEIGHT_QUESTIONABLE", "0.5"))
    INJURY_WEIGHT_PROBABLE: float = float(os.getenv("INJURY_WEIGHT_PROBABLE", "0.25"))
    INJURY_WEIGHT_HEALTHY: float = float(os.getenv("INJURY_WEIGHT_HEALTHY", "0.0"))
    
    @classmethod
    def create_directories(cls):
        """Create necessary directories if they don't exist."""
        directories = [
            cls.DATA_DIR,
            cls.RAW_DATA_DIR,
            cls.PROCESSED_DATA_DIR,
            cls.MODELS_DIR,
            cls.LOGS_DIR,
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def validate(cls) -> bool:
        """Validate that required settings are present."""
        # API keys are optional for initial setup
        # Database path should be valid
        db_path = Path(cls.DATABASE_PATH)
        if not db_path.parent.exists():
            db_path.parent.mkdir(parents=True, exist_ok=True)
        return True


# Global settings instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Get or create the global settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
        _settings.create_directories()
        _settings.validate()
    return _settings

