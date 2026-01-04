"""Team name mapping utility for converting API team names to database team IDs."""

import logging
from typing import Optional, Dict
from src.database.db_manager import DatabaseManager
from src.database.models import Team

logger = logging.getLogger(__name__)


class TeamMapper:
    """
    Maps team names from various sources (betting API, NBA API, etc.)
    to database team IDs.
    """
    
    # Common team name variations from betting APIs
    TEAM_NAME_VARIATIONS = {
        # Full names
        'Atlanta Hawks': '1610612737',
        'Boston Celtics': '1610612738',
        'Brooklyn Nets': '1610612751',
        'Charlotte Hornets': '1610612766',
        'Chicago Bulls': '1610612741',
        'Cleveland Cavaliers': '1610612739',
        'Dallas Mavericks': '1610612742',
        'Denver Nuggets': '1610612743',
        'Detroit Pistons': '1610612765',
        'Golden State Warriors': '1610612744',
        'Houston Rockets': '1610612745',
        'Indiana Pacers': '1610612754',
        'LA Clippers': '1610612746',
        'Los Angeles Clippers': '1610612746',
        'Los Angeles Lakers': '1610612747',
        'Memphis Grizzlies': '1610612763',
        'Miami Heat': '1610612748',
        'Milwaukee Bucks': '1610612749',
        'Minnesota Timberwolves': '1610612750',
        'New Orleans Pelicans': '1610612740',
        'New York Knicks': '1610612752',
        'Oklahoma City Thunder': '1610612760',
        'Orlando Magic': '1610612753',
        'Philadelphia 76ers': '1610612755',
        'Phoenix Suns': '1610612756',
        'Portland Trail Blazers': '1610612757',
        'Sacramento Kings': '1610612758',
        'San Antonio Spurs': '1610612759',
        'Toronto Raptors': '1610612761',
        'Utah Jazz': '1610612762',
        'Washington Wizards': '1610612764',
        
        # Common variations
        'Atlanta': '1610612737',
        'Boston': '1610612738',
        'Brooklyn': '1610612751',
        'Charlotte': '1610612766',
        'Chicago': '1610612741',
        'Cleveland': '1610612739',
        'Dallas': '1610612742',
        'Denver': '1610612743',
        'Detroit': '1610612765',
        'Golden State': '1610612744',
        'Houston': '1610612745',
        'Indiana': '1610612754',
        'LA Clippers': '1610612746',
        'L.A. Clippers': '1610612746',
        'L.A. Lakers': '1610612747',
        'Lakers': '1610612747',
        'Clippers': '1610612746',
        'Memphis': '1610612763',
        'Miami': '1610612748',
        'Milwaukee': '1610612749',
        'Minnesota': '1610612750',
        'New Orleans': '1610612740',
        'New York': '1610612752',
        'Oklahoma City': '1610612760',
        'Orlando': '1610612753',
        'Philadelphia': '1610612755',
        'Phoenix': '1610612756',
        'Portland': '1610612757',
        'Sacramento': '1610612758',
        'San Antonio': '1610612759',
        'Toronto': '1610612761',
        'Utah': '1610612762',
        'Washington': '1610612764',
    }
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize team mapper."""
        self.db_manager = db_manager or DatabaseManager()
        self._team_cache: Dict[str, str] = {}
        self._load_team_mappings()
    
    def _load_team_mappings(self):
        """Load team mappings from database."""
        try:
            with self.db_manager.get_session() as session:
                teams = session.query(Team).all()
                for team in teams:
                    # Map by full name
                    self._team_cache[team.team_name.lower()] = team.team_id
                    # Map by abbreviation
                    self._team_cache[team.team_abbreviation.lower()] = team.team_id
                    # Map by city + name
                    if team.city:
                        self._team_cache[f"{team.city} {team.team_name}".lower()] = team.team_id
        except Exception as e:
            logger.warning(f"Error loading team mappings: {e}")
    
    def map_team_name_to_id(self, team_name: str) -> Optional[str]:
        """
        Map team name to team ID.
        
        Args:
            team_name: Team name from API (various formats)
            
        Returns:
            Team ID or None if not found
        """
        if not team_name:
            return None
        
        # Try exact match (case insensitive)
        team_name_lower = team_name.lower().strip()
        
        # Check cache first
        if team_name_lower in self._team_cache:
            return self._team_cache[team_name_lower]
        
        # Check variations
        if team_name in self.TEAM_NAME_VARIATIONS:
            team_id = self.TEAM_NAME_VARIATIONS[team_name]
            self._team_cache[team_name_lower] = team_id
            return team_id
        
        # Try fuzzy matching (remove common suffixes)
        team_name_clean = team_name_lower
        for suffix in [' nba', ' basketball', ' team']:
            if team_name_clean.endswith(suffix):
                team_name_clean = team_name_clean[:-len(suffix)].strip()
        
        if team_name_clean in self._team_cache:
            return self._team_cache[team_name_clean]
        
        # Try partial matching
        for cached_name, team_id in self._team_cache.items():
            if team_name_lower in cached_name or cached_name in team_name_lower:
                return team_id
        
        logger.warning(f"Could not map team name '{team_name}' to team ID")
        return None




