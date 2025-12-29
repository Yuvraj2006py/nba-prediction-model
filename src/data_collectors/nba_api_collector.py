"""NBA API Collector - Proof of Concept for data collection."""

import time
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from nba_api.stats.endpoints import (
    TeamGameLog,
    BoxScoreTraditionalV3,
    BoxScoreSummaryV3,
    CommonTeamRoster
)
from nba_api.stats.static import teams

from config.settings import get_settings
from src.database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)


class NBAPICollector:
    """Collects NBA data from the NBA API."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize NBA API collector.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.settings = get_settings()
        self.db_manager = db_manager or DatabaseManager()
        self.rate_limit_delay = self.settings.RATE_LIMIT_DELAY
        self.max_retries = self.settings.MAX_RETRIES
        self.retry_delay = self.settings.RETRY_DELAY
        
        logger.info("NBA API Collector initialized")
    
    def _rate_limit(self):
        """Apply rate limiting delay."""
        time.sleep(self.rate_limit_delay)
    
    def _retry_api_call(self, func, *args, **kwargs):
        """Retry API call with exponential backoff."""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"API call failed (attempt {attempt + 1}/{self.max_retries}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API call failed after {self.max_retries} attempts: {e}")
                    raise
        return None
    
    def collect_all_teams(self) -> List[Dict[str, Any]]:
        """
        Collect all NBA teams (one-time operation).
        
        Returns:
            List of team dictionaries
        """
        logger.info("Collecting all NBA teams...")
        
        try:
            nba_teams = teams.get_teams()
            teams_data = []
            
            for team in nba_teams:
                team_data = {
                    'team_id': str(team['id']),
                    'team_name': team['full_name'],
                    'team_abbreviation': team['abbreviation'],
                    'city': team['city'],
                    'conference': None,  # Will need to get from another endpoint
                    'division': None     # Will need to get from another endpoint
                }
                
                # Store in database
                self.db_manager.insert_team(team_data)
                teams_data.append(team_data)
            
            logger.info(f"Collected {len(teams_data)} teams")
            return teams_data
            
        except Exception as e:
            logger.error(f"Error collecting teams: {e}")
            raise
    
    def get_games_for_team_season(self, team_id: str, season: str) -> List[Dict[str, Any]]:
        """
        Get all games for a team in a season.
        
        Args:
            team_id: NBA team ID
            season: Season string (e.g., '2024-25')
            
        Returns:
            List of game dictionaries
        """
        logger.debug(f"Getting games for team {team_id} in season {season}")
        
        try:
            self._rate_limit()
            
            team_gamelog = self._retry_api_call(
                TeamGameLog,
                team_id=team_id,
                season=season
            )
            
            if not team_gamelog:
                return []
            
            data = team_gamelog.get_dict()
            games = []
            
            if data['resultSets'] and len(data['resultSets']) > 0:
                game_log = data['resultSets'][0]
                headers = game_log.get('headers', [])
                
                # Find column indices using headers
                game_id_idx = headers.index('Game_ID') if 'Game_ID' in headers else 1
                game_date_idx = headers.index('GAME_DATE') if 'GAME_DATE' in headers else 2
                matchup_idx = headers.index('MATCHUP') if 'MATCHUP' in headers else 3
                win_loss_idx = headers.index('WL') if 'WL' in headers else 4
                
                for game_row in game_log['rowSet']:
                    # Get game ID - it should be a string like "0022400123"
                    game_id = str(game_row[game_id_idx]) if len(game_row) > game_id_idx else None
                    
                    # Skip if game_id is not a valid format (should be 10 digits)
                    if not game_id or len(game_id) < 10 or not game_id.isdigit():
                        continue
                    
                    # Parse date - format is "APR 13, 2025"
                    game_date = None
                    if len(game_row) > game_date_idx and game_row[game_date_idx]:
                        try:
                            date_str = str(game_row[game_date_idx])
                            if isinstance(date_str, str) and len(date_str) > 5:
                                game_date = datetime.strptime(date_str, '%b %d, %Y').date()
                        except (ValueError, TypeError) as e:
                            logger.debug(f"Could not parse date {game_row[game_date_idx]}: {e}")
                    
                    matchup = game_row[matchup_idx] if len(game_row) > matchup_idx else None
                    win_loss = game_row[win_loss_idx] if len(game_row) > win_loss_idx else None
                    
                    game_data = {
                        'game_id': game_id,
                        'game_date': game_date,
                        'team_id': str(game_row[0]),  # Team_ID is first column
                        'season': season,
                        'season_type': 'Regular Season',
                        'matchup': matchup,  # e.g., "LAL vs. GSW" or "LAL @ GSW"
                        'win_loss': win_loss
                    }
                    games.append(game_data)
            
            return games
            
        except Exception as e:
            logger.error(f"Error getting games for team {team_id} season {season}: {e}")
            return []
    
    def get_games_for_date(self, game_date: date, season: str = '2024-25') -> List[Dict[str, Any]]:
        """
        Get all games for a specific date by checking all teams.
        Note: This is less efficient but works when scoreboard endpoint isn't available.
        
        Args:
            game_date: Date to get games for
            season: Season string (e.g., '2024-25')
            
        Returns:
            List of game dictionaries
        """
        logger.info(f"Getting games for date: {game_date}")
        
        try:
            # Get all teams
            nba_teams = teams.get_teams()
            all_games = {}
            
            # Check first team to get games (we'll deduplicate)
            if nba_teams:
                team_id = str(nba_teams[0]['id'])
                games = self.get_games_for_team_season(team_id, season)
                
                # Filter by date and collect unique games
                for game in games:
                    if game.get('game_date') == game_date:
                        game_id = game.get('game_id')
                        if game_id and game_id not in all_games:
                            # Parse matchup to get home/away teams
                            matchup = game.get('matchup', '')
                            # Format is usually "TEAM @ TEAM" or "TEAM vs. TEAM"
                            all_games[game_id] = {
                                'game_id': game_id,
                                'game_date': game_date,
                                'season': season,
                                'season_type': game.get('season_type', 'Regular Season'),
                                'matchup': matchup
                            }
            
            games_list = list(all_games.values())
            logger.info(f"Found {len(games_list)} games for {game_date}")
            return games_list
            
        except Exception as e:
            logger.error(f"Error getting games for date {game_date}: {e}")
            return []
    
    def get_game_details(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed game information including scores.
        
        Args:
            game_id: NBA game ID
            
        Returns:
            Game details dictionary or None
        """
        logger.debug(f"Getting game details for {game_id}")
        
        try:
            self._rate_limit()
            
            # Get game summary using BoxScoreSummaryV3 (newer API)
            game_summary = self._retry_api_call(
                BoxScoreSummaryV3,
                game_id=game_id
            )
            
            if not game_summary:
                return None
            
            summary_data = game_summary.get_dict()
            if 'boxScoreSummary' not in summary_data:
                return None
            
            bss = summary_data['boxScoreSummary']
            
            # Extract basic game info
            home_team_id = str(bss.get('homeTeamId', ''))
            away_team_id = str(bss.get('awayTeamId', ''))
            game_status = bss.get('gameStatus', 1)  # 1=scheduled, 2=live, 3=finished
            
            # Get game date from gameEt or gameTimeUTC
            game_date = None
            if bss.get('gameEt'):
                try:
                    date_str = str(bss['gameEt'])
                    if 'T' in date_str:
                        game_date = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    pass
            
            # Get scores from team objects
            home_score = None
            away_score = None
            
            if bss.get('homeTeam') and 'score' in bss['homeTeam']:
                home_score = bss['homeTeam'].get('score')
            if bss.get('awayTeam') and 'score' in bss['awayTeam']:
                away_score = bss['awayTeam'].get('score')
            
            # Determine season from game ID (first 2 digits after 00 = season start year)
            # e.g., 0022401199 -> 2024-25 season
            season = None
            if len(game_id) >= 4:
                season_start = int(game_id[2:4])
                if season_start >= 50:  # 1950-1999
                    season = f"19{season_start}-{season_start + 1}"
                else:  # 2000-2049
                    season = f"20{season_start}-{season_start + 1}"
            
            # Determine winner
            winner = None
            point_differential = None
            if home_score is not None and away_score is not None:
                point_differential = home_score - away_score
                winner = home_team_id if home_score > away_score else away_team_id
            
            # For POC, we'll get basic stats from BoxScoreSummaryV3
            # Detailed stats can be added later using BoxScoreTraditionalV3
            home_stats = None
            away_stats = None
            
            # Try to get stats from team objects if available
            # For now, return basic game info - detailed stats collection can be added later
            
            # Build game details
            game_status_text = 'scheduled'
            if game_status == 2:
                game_status_text = 'live'
            elif game_status == 3:
                game_status_text = 'finished'
            
            game_details = {
                'game_id': game_id,
                'season': season,
                'season_type': 'Regular Season',  # Default, can be determined later
                'game_date': game_date,
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'home_score': home_score,
                'away_score': away_score,
                'game_status': game_status_text,
                'winner': winner,
                'point_differential': point_differential,
                'home_stats': home_stats,  # Will be None for POC, can add later
                'away_stats': away_stats   # Will be None for POC, can add later
            }
            
            return game_details
            
        except Exception as e:
            logger.error(f"Error getting game details for {game_id}: {e}")
            return None
    
    def collect_team_stats(self, game_id: str) -> List[Dict[str, Any]]:
        """
        Collect team statistics for a game using BoxScoreTraditionalV3.
        
        Args:
            game_id: NBA game ID
            
        Returns:
            List of team stat dictionaries (one for home, one for away)
        """
        logger.debug(f"Collecting team stats for game {game_id}")
        
        try:
            self._rate_limit()
            
            # Get box score using BoxScoreTraditionalV3
            boxscore = self._retry_api_call(
                BoxScoreTraditionalV3,
                game_id=game_id
            )
            
            if not boxscore:
                logger.warning(f"No boxscore data for game {game_id}")
                return []
            
            boxscore_data = boxscore.get_dict()
            if 'boxScoreTraditional' not in boxscore_data:
                logger.warning(f"No boxScoreTraditional data for game {game_id}")
                return []
            
            bst = boxscore_data['boxScoreTraditional']
            home_team_id = str(bst.get('homeTeamId', ''))
            away_team_id = str(bst.get('awayTeamId', ''))
            
            if not home_team_id or not away_team_id:
                logger.warning(f"Missing team IDs for game {game_id}")
                return []
            
            team_stats_list = []
            
            # Extract home team stats
            if 'homeTeam' in bst and 'statistics' in bst['homeTeam']:
                home_stats = self._extract_team_stats_from_api(
                    bst['homeTeam']['statistics'],
                    game_id,
                    home_team_id,
                    is_home=True
                )
                if home_stats:
                    team_stats_list.append(home_stats)
            
            # Extract away team stats
            if 'awayTeam' in bst and 'statistics' in bst['awayTeam']:
                away_stats = self._extract_team_stats_from_api(
                    bst['awayTeam']['statistics'],
                    game_id,
                    away_team_id,
                    is_home=False
                )
                if away_stats:
                    team_stats_list.append(away_stats)
            
            logger.debug(f"Collected {len(team_stats_list)} team stats for game {game_id}")
            return team_stats_list
            
        except Exception as e:
            logger.error(f"Error collecting team stats for game {game_id}: {e}")
            return []
    
    def _extract_team_stats_from_api(
        self,
        stats_dict: Dict[str, Any],
        game_id: str,
        team_id: str,
        is_home: bool
    ) -> Optional[Dict[str, Any]]:
        """
        Extract team statistics from API response dictionary.
        
        Args:
            stats_dict: Statistics dictionary from API
            game_id: Game ID
            team_id: Team ID
            is_home: Whether this is the home team
            
        Returns:
            Team stats dictionary or None
        """
        try:
            # Helper function to safely get integer values
            def get_int(key: str, default: int = 0) -> int:
                value = stats_dict.get(key, default)
                try:
                    return int(float(value)) if value is not None else default
                except (ValueError, TypeError):
                    return default
            
            # Helper function to safely get float values
            def get_float(key: str, default: float = 0.0) -> float:
                value = stats_dict.get(key, default)
                try:
                    return float(value) if value is not None else default
                except (ValueError, TypeError):
                    return default
            
            # Extract basic stats
            points = get_int('points', 0)
            fgm = get_int('fieldGoalsMade', 0)
            fga = get_int('fieldGoalsAttempted', 0)
            fg_pct = get_float('fieldGoalsPercentage', 0.0)
            if fga > 0 and fg_pct == 0.0:
                fg_pct = (fgm / fga) * 100.0
            
            three_pm = get_int('threePointersMade', 0)
            three_pa = get_int('threePointersAttempted', 0)
            three_pct = get_float('threePointersPercentage', 0.0)
            if three_pa > 0 and three_pct == 0.0:
                three_pct = (three_pm / three_pa) * 100.0
            
            ftm = get_int('freeThrowsMade', 0)
            fta = get_int('freeThrowsAttempted', 0)
            ft_pct = get_float('freeThrowsPercentage', 0.0)
            if fta > 0 and ft_pct == 0.0:
                ft_pct = (ftm / fta) * 100.0
            
            orb = get_int('offensiveRebounds', 0)
            drb = get_int('defensiveRebounds', 0)
            trb = get_int('rebounds', 0)
            if trb == 0:
                trb = orb + drb
            
            assists = get_int('assists', 0)
            steals = get_int('steals', 0)
            blocks = get_int('blocks', 0)
            turnovers = get_int('turnovers', 0)
            fouls = get_int('fouls', 0)
            
            # Calculate advanced metrics (can be None if not enough data)
            offensive_rating = None
            defensive_rating = None
            pace = None
            ts_pct = None
            efg_pct = None
            
            # True shooting percentage: TS% = PTS / (2 * (FGA + 0.44 * FTA))
            if fga > 0 or fta > 0:
                ts_denominator = 2 * (fga + 0.44 * fta)
                if ts_denominator > 0:
                    ts_pct = (points / ts_denominator) * 100.0
            
            # Effective field goal percentage: eFG% = (FGM + 0.5 * 3PM) / FGA
            if fga > 0:
                efg_pct = ((fgm + 0.5 * three_pm) / fga) * 100.0
            
            team_stats = {
                'game_id': game_id,
                'team_id': team_id,
                'is_home': is_home,
                'points': points,
                'field_goals_made': fgm,
                'field_goals_attempted': fga,
                'field_goal_percentage': fg_pct,
                'three_pointers_made': three_pm,
                'three_pointers_attempted': three_pa,
                'three_point_percentage': three_pct,
                'free_throws_made': ftm,
                'free_throws_attempted': fta,
                'free_throw_percentage': ft_pct,
                'rebounds_offensive': orb,
                'rebounds_defensive': drb,
                'rebounds_total': trb,
                'assists': assists,
                'steals': steals,
                'blocks': blocks,
                'turnovers': turnovers,
                'personal_fouls': fouls,
                'offensive_rating': offensive_rating,
                'defensive_rating': defensive_rating,
                'pace': pace,
                'true_shooting_percentage': ts_pct,
                'effective_field_goal_percentage': efg_pct
            }
            
            return team_stats
            
        except Exception as e:
            logger.error(f"Error extracting team stats: {e}")
            return None
    
    def collect_player_stats(self, game_id: str) -> List[Dict[str, Any]]:
        """
        Collect player statistics for a game using BoxScoreTraditionalV3.
        
        Args:
            game_id: NBA game ID
            
        Returns:
            List of player stat dictionaries
        """
        logger.debug(f"Collecting player stats for game {game_id}")
        
        try:
            self._rate_limit()
            
            # Get box score using BoxScoreTraditionalV3
            boxscore = self._retry_api_call(
                BoxScoreTraditionalV3,
                game_id=game_id
            )
            
            if not boxscore:
                logger.warning(f"No boxscore data for game {game_id}")
                return []
            
            boxscore_data = boxscore.get_dict()
            if 'boxScoreTraditional' not in boxscore_data:
                logger.warning(f"No boxScoreTraditional data for game {game_id}")
                return []
            
            bst = boxscore_data['boxScoreTraditional']
            home_team_id = str(bst.get('homeTeamId', ''))
            away_team_id = str(bst.get('awayTeamId', ''))
            
            if not home_team_id or not away_team_id:
                logger.warning(f"Missing team IDs for game {game_id}")
                return []
            
            player_stats_list = []
            
            # Extract home team player stats
            if 'homeTeam' in bst and 'players' in bst['homeTeam']:
                for player in bst['homeTeam']['players']:
                    player_stat = self._extract_player_stats_from_api(
                        player,
                        game_id,
                        home_team_id
                    )
                    if player_stat:
                        player_stats_list.append(player_stat)
            
            # Extract away team player stats
            if 'awayTeam' in bst and 'players' in bst['awayTeam']:
                for player in bst['awayTeam']['players']:
                    player_stat = self._extract_player_stats_from_api(
                        player,
                        game_id,
                        away_team_id
                    )
                    if player_stat:
                        player_stats_list.append(player_stat)
            
            logger.debug(f"Collected {len(player_stats_list)} player stats for game {game_id}")
            return player_stats_list
            
        except Exception as e:
            logger.error(f"Error collecting player stats for game {game_id}: {e}")
            return []
    
    def _extract_player_stats_from_api(
        self,
        player_dict: Dict[str, Any],
        game_id: str,
        team_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract player statistics from API response dictionary.
        
        Args:
            player_dict: Player dictionary from API
            game_id: Game ID
            team_id: Team ID
            
        Returns:
            Player stats dictionary or None
        """
        try:
            # Helper function to safely get values
            def get_int(key: str, default: int = 0) -> int:
                value = player_dict.get(key, default)
                try:
                    return int(float(value)) if value is not None else default
                except (ValueError, TypeError):
                    return default
            
            def get_str(key: str, default: str = '') -> str:
                value = player_dict.get(key, default)
                return str(value) if value is not None else default
            
            # Extract player info - API uses personId, firstName, familyName
            player_id = str(player_dict.get('personId', ''))
            first_name = get_str('firstName', '')
            family_name = get_str('familyName', '')
            player_name = f"{first_name} {family_name}".strip() if first_name or family_name else get_str('nameI', 'Unknown Player')
            
            # Statistics are nested in 'statistics' key
            stats = player_dict.get('statistics', {})
            if not stats:
                # If no statistics, player didn't play
                return None
            
            minutes = get_str('minutes', '0:00') if 'minutes' in player_dict else stats.get('minutes', '0:00')
            
            # Extract stats from statistics object
            points = get_int('points', 0) if 'points' in player_dict else int(stats.get('points', 0) or 0)
            rebounds = get_int('rebounds', 0) if 'rebounds' in player_dict else int(stats.get('reboundsTotal', 0) or 0)
            assists = get_int('assists', 0) if 'assists' in player_dict else int(stats.get('assists', 0) or 0)
            fgm = get_int('fieldGoalsMade', 0) if 'fieldGoalsMade' in player_dict else int(stats.get('fieldGoalsMade', 0) or 0)
            fga = get_int('fieldGoalsAttempted', 0) if 'fieldGoalsAttempted' in player_dict else int(stats.get('fieldGoalsAttempted', 0) or 0)
            three_pm = get_int('threePointersMade', 0) if 'threePointersMade' in player_dict else int(stats.get('threePointersMade', 0) or 0)
            three_pa = get_int('threePointersAttempted', 0) if 'threePointersAttempted' in player_dict else int(stats.get('threePointersAttempted', 0) or 0)
            ftm = get_int('freeThrowsMade', 0) if 'freeThrowsMade' in player_dict else int(stats.get('freeThrowsMade', 0) or 0)
            fta = get_int('freeThrowsAttempted', 0) if 'freeThrowsAttempted' in player_dict else int(stats.get('freeThrowsAttempted', 0) or 0)
            plus_minus = None
            if 'plusMinusPoints' in stats:
                try:
                    plus_minus = int(float(stats['plusMinusPoints']))
                except (ValueError, TypeError):
                    pass
            
            # Determine injury status based on minutes played
            injury_status = 'healthy'
            if minutes == '0:00' or minutes == '00:00':
                injury_status = 'out'
            elif minutes and ':' in minutes:
                try:
                    mins, secs = minutes.split(':')
                    total_seconds = int(mins) * 60 + int(secs)
                    if total_seconds < 300:  # Less than 5 minutes
                        injury_status = 'questionable'
                except (ValueError, IndexError):
                    pass
            
            # Skip if player didn't play (no meaningful stats)
            if points == 0 and rebounds == 0 and assists == 0 and fga == 0:
                return None
            
            player_stats = {
                'game_id': game_id,
                'player_id': player_id,
                'team_id': team_id,
                'player_name': player_name,
                'minutes_played': minutes,
                'points': points,
                'rebounds': rebounds,
                'assists': assists,
                'field_goals_made': fgm,
                'field_goals_attempted': fga,
                'three_pointers_made': three_pm,
                'three_pointers_attempted': three_pa,
                'free_throws_made': ftm,
                'free_throws_attempted': fta,
                'plus_minus': plus_minus,
                'injury_status': injury_status
            }
            
            return player_stats
            
        except Exception as e:
            logger.error(f"Error extracting player stats: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None

