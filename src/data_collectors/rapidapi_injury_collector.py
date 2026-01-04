"""
RapidAPI NBA Injuries Collector.

Collects real-time injury data from RapidAPI NBA Injuries Reports.
This enables pre-game injury tracking for more accurate predictions.
"""

import http.client
import json
import logging
from datetime import date, timedelta
from typing import List, Dict, Optional
from config.settings import get_settings
from src.database.db_manager import DatabaseManager
from src.database.models import PlayerStats, Team, Game

logger = logging.getLogger(__name__)


class RapidAPIInjuryCollector:
    """
    Collects injury data from RapidAPI NBA Injuries Reports.
    
    API Response Format:
    [
        {
            "date": "2026-01-04",
            "team": "Detroit Pistons",
            "player": "Jalen Duren",
            "status": "Out",
            "reason": "Injury/Illness - Right Ankle; Sprain",
            "reportTime": "02PM"
        },
        ...
    ]
    """
    
    # Team name mapping (API names to database names)
    TEAM_NAME_MAPPINGS = {
        # Add any name discrepancies here
        'LA Clippers': 'Los Angeles Clippers',
        'LA Lakers': 'Los Angeles Lakers',
    }
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize RapidAPI injury collector.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.settings = get_settings()
        self.db_manager = db_manager or DatabaseManager()
        self.api_key = self.settings.RAPIDAPI_NBA_INJURIES_KEY
        self.host = self.settings.RAPIDAPI_NBA_INJURIES_HOST
        self.headers = {
            'x-rapidapi-key': self.api_key or '',
            'x-rapidapi-host': self.host
        }
        
        if not self.api_key:
            logger.warning("RAPIDAPI_NBA_INJURIES_KEY not set in environment")
    
    def _normalize_team_name(self, team_name: str) -> str:
        """Normalize team name to match database format."""
        return self.TEAM_NAME_MAPPINGS.get(team_name, team_name)
    
    def _normalize_injury_status(self, status: str) -> str:
        """
        Normalize injury status to our format.
        
        Args:
            status: Status from API (e.g., "Out", "Questionable", "Probable")
            
        Returns:
            Normalized status: 'out', 'questionable', 'probable', or 'healthy'
        """
        status_lower = status.lower().strip()
        
        if 'out' in status_lower:
            return 'out'
        elif 'questionable' in status_lower or 'doubtful' in status_lower:
            return 'questionable'
        elif 'probable' in status_lower or 'day-to-day' in status_lower:
            return 'probable'
        else:
            return 'healthy'
    
    def get_injuries_for_date(self, injury_date: date) -> List[Dict]:
        """
        Get injury list for a specific date.
        
        Args:
            injury_date: Date to fetch injuries for
            
        Returns:
            List of injury records, each with:
            - date: str (YYYY-MM-DD)
            - team: str (team name)
            - player: str (player full name)
            - status: str (Out, Questionable, Probable, etc.)
            - reason: str (injury description)
            - reportTime: str (time of report)
        """
        if not self.api_key:
            logger.error("Cannot fetch injuries: API key not configured")
            return []
        
        try:
            conn = http.client.HTTPSConnection(self.host)
            endpoint = f"/injuries/nba/{injury_date.strftime('%Y-%m-%d')}"
            
            logger.info(f"Fetching injuries for {injury_date} from RapidAPI")
            conn.request("GET", endpoint, headers=self.headers)
            
            res = conn.getresponse()
            status = res.status
            
            if status == 200:
                data = res.read()
                response_json = json.loads(data.decode("utf-8"))
                logger.info(f"Successfully fetched {len(response_json)} injury records")
                return response_json
            elif status == 429:
                logger.warning(f"Rate limit exceeded for {injury_date}. Daily quota may be reached.")
                return []
            elif status == 404:
                logger.info(f"No injury data found for {injury_date}")
                return []
            else:
                logger.error(f"API returned status {status} for {injury_date}")
                data = res.read()
                logger.error(f"Response: {data.decode('utf-8')[:200]}")
                return []
                
        except Exception as e:
            logger.error(f"Error fetching injuries for {injury_date}: {e}")
            return []
        finally:
            try:
                conn.close()
            except:
                pass
    
    def get_today_injuries(self) -> List[Dict]:
        """Get today's injury list."""
        return self.get_injuries_for_date(date.today())
    
    def get_injuries_by_team(
        self, 
        injury_date: date,
        team_name: Optional[str] = None
    ) -> Dict[str, List[Dict]]:
        """
        Get injuries grouped by team.
        
        Args:
            injury_date: Date to fetch injuries for
            team_name: Optional team name to filter by
            
        Returns:
            Dictionary mapping team names to list of injury records
        """
        injuries = self.get_injuries_for_date(injury_date)
        
        grouped: Dict[str, List[Dict]] = {}
        for injury in injuries:
            team = self._normalize_team_name(injury.get('team', 'Unknown'))
            if team_name and team != team_name:
                continue
            if team not in grouped:
                grouped[team] = []
            grouped[team].append(injury)
        
        return grouped
    
    def update_player_injury_status_for_games(
        self, 
        game_date: date,
        games: Optional[List[Game]] = None
    ) -> Dict[str, int]:
        """
        Update PlayerStats.injury_status for upcoming games based on injury reports.
        
        Note: This updates existing PlayerStats records. For future games,
        PlayerStats may not exist yet - this is handled by creating placeholder
        records or updating after game data is collected.
        
        Args:
            game_date: Date of games to update
            games: Optional list of Game objects. If None, fetches games for date.
            
        Returns:
            Dictionary with update statistics:
            - total_injuries: Total injury records processed
            - players_updated: Number of players updated in database
            - players_not_found: Number of players in injury report but not in database
            - teams_matched: Number of teams matched to database
        """
        if not games:
            games = self.db_manager.get_games(game_date=game_date)
        
        if not games:
            logger.info(f"No games found for {game_date}")
            return {'total_injuries': 0, 'players_updated': 0, 'players_not_found': 0, 'teams_matched': 0}
        
        # Fetch injury reports
        injuries = self.get_injuries_for_date(game_date)
        if not injuries:
            logger.info(f"No injury reports found for {game_date}")
            return {'total_injuries': 0, 'players_updated': 0, 'players_not_found': 0, 'teams_matched': 0}
        
        # Map team names to team_ids
        with self.db_manager.get_session() as session:
            teams = {t.team_name: t.team_id for t in session.query(Team).all()}
        
        stats = {
            'total_injuries': len(injuries),
            'players_updated': 0,
            'players_not_found': 0,
            'teams_matched': 0
        }
        
        # Get team IDs that are playing today
        playing_team_ids = set()
        for game in games:
            playing_team_ids.add(game.home_team_id)
            playing_team_ids.add(game.away_team_id)
        
        # Process each injury
        matched_teams = set()
        with self.db_manager.get_session() as session:
            for injury in injuries:
                team_name = self._normalize_team_name(injury.get('team', ''))
                player_name = injury.get('player', '')
                status = injury.get('status', '')
                
                # Map status to our format
                injury_status = self._normalize_injury_status(status)
                
                # Find team_id
                team_id = teams.get(team_name)
                if not team_id:
                    logger.debug(f"Team not found: {team_name}")
                    continue
                
                # Check if this team is playing today
                if team_id not in playing_team_ids:
                    continue
                
                matched_teams.add(team_id)
                
                # Find games for this team on this date
                team_games = [
                    g for g in games 
                    if g.home_team_id == team_id or g.away_team_id == team_id
                ]
                
                for game in team_games:
                    # Try to find player in PlayerStats for this game
                    # Use case-insensitive partial matching
                    player_stat = session.query(PlayerStats).filter_by(
                        game_id=game.game_id,
                        team_id=team_id
                    ).filter(
                        PlayerStats.player_name.ilike(f"%{player_name}%")
                    ).first()
                    
                    if player_stat:
                        old_status = player_stat.injury_status
                        player_stat.injury_status = injury_status
                        stats['players_updated'] += 1
                        logger.debug(
                            f"Updated {player_name} ({team_name}) "
                            f"from {old_status} to {injury_status}"
                        )
                    else:
                        # Player not in database yet
                        stats['players_not_found'] += 1
                        logger.debug(
                            f"Player {player_name} ({team_name}) "
                            f"not found in game {game.game_id}"
                        )
            
            session.commit()
        
        stats['teams_matched'] = len(matched_teams)
        
        logger.info(
            f"Injury update complete: {stats['players_updated']} updated, "
            f"{stats['players_not_found']} not found, "
            f"{stats['teams_matched']} teams matched"
        )
        
        return stats
    
    def get_injury_summary(self, game_date: date) -> Dict[str, Dict]:
        """
        Get a summary of injuries for each team playing on a date.
        
        Args:
            game_date: Date to get summary for
            
        Returns:
            Dictionary mapping team names to injury summary:
            {
                'Detroit Pistons': {
                    'out_count': 3,
                    'questionable_count': 1,
                    'probable_count': 0,
                    'players_out': ['Jalen Duren', 'Caris LeVert'],
                    'players_questionable': ['Isaiah Stewart']
                }
            }
        """
        injuries = self.get_injuries_for_date(game_date)
        
        summary: Dict[str, Dict] = {}
        
        for injury in injuries:
            team = self._normalize_team_name(injury.get('team', 'Unknown'))
            player = injury.get('player', 'Unknown')
            status = self._normalize_injury_status(injury.get('status', ''))
            
            if team not in summary:
                summary[team] = {
                    'out_count': 0,
                    'questionable_count': 0,
                    'probable_count': 0,
                    'players_out': [],
                    'players_questionable': [],
                    'players_probable': []
                }
            
            if status == 'out':
                summary[team]['out_count'] += 1
                summary[team]['players_out'].append(player)
            elif status == 'questionable':
                summary[team]['questionable_count'] += 1
                summary[team]['players_questionable'].append(player)
            elif status == 'probable':
                summary[team]['probable_count'] += 1
                summary[team]['players_probable'].append(player)
        
        return summary

