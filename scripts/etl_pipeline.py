#!/usr/bin/env python
"""
Comprehensive ETL Pipeline for NBA Season Data.

Features:
- Fast batch processing with rate limiting
- Detailed game results (scores, status, winner)
- Complete team statistics (box scores)
- Complete player statistics
- Data validation and cleaning
- Incremental updates (skip already processed games)
- Progress tracking with tqdm
- Error handling and retry logic
- Optimized for speed with minimal API calls

Usage:
    python scripts/etl_pipeline.py --season 2025-26
    python scripts/etl_pipeline.py --season 2025-26 --full-refresh
    python scripts/etl_pipeline.py --season 2024-25 --stats-only
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
import argparse
import time
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional, Set, Tuple
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Setup logging before other imports
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Reduce noise from other loggers
logging.getLogger('src.database.db_manager').setLevel(logging.WARNING)
logging.getLogger('src.data_collectors').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

from tqdm import tqdm
from nba_api.stats.endpoints import TeamGameLog, BoxScoreTraditionalV3, BoxScoreSummaryV3
from nba_api.stats.static import teams as nba_teams_static

from src.database.db_manager import DatabaseManager
from src.database.models import Game, Team, TeamStats, PlayerStats


class ETLPipeline:
    """
    Fast, comprehensive ETL pipeline for NBA season data.
    
    Optimizations:
    - Single API call per game for all stats (team + player)
    - Batch database commits
    - Skip already processed games
    - Intelligent rate limiting
    - Progress tracking
    """
    
    def __init__(self, season: str = '2025-26', db_manager: Optional[DatabaseManager] = None):
        """
        Initialize ETL Pipeline.
        
        Args:
            season: NBA season (e.g., '2025-26')
            db_manager: Optional database manager
        """
        self.season = season
        self.db_manager = db_manager or DatabaseManager()
        
        # Rate limiting configuration
        self.rate_limit_delay = 0.6  # 600ms between API calls (safe for NBA API)
        self.batch_delay = 2.0  # Extra delay every batch
        self.batch_size = 10  # Games per batch
        self.max_retries = 3
        self.retry_delay = 2.0
        
        # Thread safety for rate limiting
        self._api_lock = threading.Lock()
        self._last_api_call = 0
        
        # Stats tracking
        self.stats = {
            'games_found': 0,
            'games_new': 0,
            'games_updated': 0,
            'games_skipped': 0,
            'team_stats_added': 0,
            'player_stats_added': 0,
            'errors': 0,
            'api_calls': 0,
            'start_time': None,
            'end_time': None
        }
        
        logger.info(f"ETL Pipeline initialized for season {season}")
    
    def _rate_limit(self):
        """Apply rate limiting between API calls."""
        with self._api_lock:
            elapsed = time.time() - self._last_api_call
            if elapsed < self.rate_limit_delay:
                time.sleep(self.rate_limit_delay - elapsed)
            self._last_api_call = time.time()
            self.stats['api_calls'] += 1
    
    def _retry_api_call(self, func, *args, **kwargs):
        """Retry API call with exponential backoff."""
        for attempt in range(self.max_retries):
            try:
                self._rate_limit()
                return func(*args, **kwargs)
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (1.5 ** attempt)
                    logger.warning(f"API error (attempt {attempt + 1}): {e}. Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"API call failed after {self.max_retries} attempts: {e}")
                    raise
        return None
    
    def run(self, full_refresh: bool = False, stats_only: bool = False) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.
        
        Args:
            full_refresh: If True, re-fetch all data even if already exists
            stats_only: If True, only collect stats for existing games
            
        Returns:
            Statistics dictionary
        """
        self.stats['start_time'] = datetime.now()
        
        print("\n" + "=" * 70)
        print(f"NBA ETL PIPELINE - Season {self.season}")
        print("=" * 70)
        
        try:
            # Step 1: Ensure teams exist
            self._ensure_teams()
            
            # Step 2: Extract all games for the season
            if not stats_only:
                self._extract_all_games(full_refresh)
            
            # Step 3: Collect detailed stats for games
            self._collect_game_stats(full_refresh)
            
            # Step 4: Validate data
            self._validate_data()
            
        except KeyboardInterrupt:
            print("\n\n[!] Pipeline interrupted by user")
            self.stats['errors'] += 1
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            self.stats['errors'] += 1
            import traceback
            traceback.print_exc()
        
        self.stats['end_time'] = datetime.now()
        self._print_summary()
        
        return self.stats
    
    def _ensure_teams(self):
        """Ensure all NBA teams exist in the database."""
        print("\n[STEP 1] Ensuring teams exist in database...")
        
        all_teams = nba_teams_static.get_teams()
        
        with self.db_manager.get_session() as session:
            existing_teams = {t.team_id for t in session.query(Team).all()}
            
            new_teams = 0
            for team in all_teams:
                team_id = str(team['id'])
                if team_id not in existing_teams:
                    team_obj = Team(
                        team_id=team_id,
                        team_name=team['full_name'],
                        team_abbreviation=team['abbreviation'],
                        city=team['city']
                    )
                    session.add(team_obj)
                    new_teams += 1
            
            session.commit()
        
        print(f"  [OK] {len(all_teams)} teams total, {new_teams} new teams added")
    
    def _extract_all_games(self, full_refresh: bool = False):
        """
        Extract all games for the season by querying each team's game log.
        
        This is more efficient than checking each date because:
        1. One API call per team = 30 calls max
        2. Gets all games at once instead of day-by-day
        """
        print(f"\n[STEP 2] Extracting all games for season {self.season}...")
        
        all_teams = nba_teams_static.get_teams()
        all_games: Dict[str, Dict[str, Any]] = {}  # game_id -> game_data
        
        # Get existing games to skip
        with self.db_manager.get_session() as session:
            if not full_refresh:
                existing_games = {g.game_id for g in session.query(Game).filter(
                    Game.season == self.season
                ).all()}
                print(f"  Found {len(existing_games)} existing games in database")
            else:
                existing_games = set()
                print("  Full refresh mode - will update all games")
        
        # Query each team's game log
        print(f"  Querying game logs for {len(all_teams)} teams...")
        
        for team in tqdm(all_teams, desc="  Fetching team game logs"):
            team_id = str(team['id'])
            try:
                games = self._get_team_game_log(team_id)
                for game in games:
                    game_id = game.get('game_id')
                    if game_id and game_id not in all_games:
                        all_games[game_id] = game
            except Exception as e:
                logger.warning(f"Error getting games for team {team['abbreviation']}: {e}")
                self.stats['errors'] += 1
        
        self.stats['games_found'] = len(all_games)
        print(f"  [OK] Found {len(all_games)} unique games for season {self.season}")
        
        # Store games in database
        if all_games:
            self._store_games(all_games, existing_games, full_refresh)
    
    def _get_team_game_log(self, team_id: str) -> List[Dict[str, Any]]:
        """Get all games for a team in the current season."""
        try:
            gamelog = self._retry_api_call(
                TeamGameLog,
                team_id=team_id,
                season=self.season
            )
            
            if not gamelog:
                return []
            
            data = gamelog.get_dict()
            games = []
            
            if data.get('resultSets') and len(data['resultSets']) > 0:
                result_set = data['resultSets'][0]
                headers = result_set.get('headers', [])
                
                # Find column indices
                idx = {h: i for i, h in enumerate(headers)}
                
                for row in result_set.get('rowSet', []):
                    try:
                        game_id = str(row[idx.get('Game_ID', 1)])
                        
                        # Validate game ID format
                        if not game_id or len(game_id) < 10 or not game_id.isdigit():
                            continue
                        
                        # Parse date
                        game_date = None
                        date_str = row[idx.get('GAME_DATE', 2)]
                        if date_str:
                            try:
                                game_date = datetime.strptime(str(date_str), '%b %d, %Y').date()
                            except ValueError:
                                try:
                                    game_date = datetime.strptime(str(date_str), '%Y-%m-%d').date()
                                except ValueError:
                                    pass
                        
                        # Parse matchup to determine home/away
                        matchup = str(row[idx.get('MATCHUP', 3)] or '')
                        is_home = 'vs.' in matchup
                        
                        # Extract opponent from matchup
                        opponent_abbr = None
                        if '@' in matchup:
                            parts = matchup.split('@')
                            if len(parts) == 2:
                                opponent_abbr = parts[1].strip()
                        elif 'vs.' in matchup:
                            parts = matchup.split('vs.')
                            if len(parts) == 2:
                                opponent_abbr = parts[1].strip()
                        
                        # Get score from WL and PTS columns
                        win_loss = row[idx.get('WL', 4)]
                        points = row[idx.get('PTS', -1)] if 'PTS' in idx else None
                        
                        games.append({
                            'game_id': game_id,
                            'game_date': game_date,
                            'season': self.season,
                            'season_type': 'Regular Season',
                            'team_id': team_id,
                            'is_home': is_home,
                            'opponent_abbr': opponent_abbr,
                            'win_loss': win_loss,
                            'points': points,
                            'matchup': matchup
                        })
                        
                    except (IndexError, ValueError) as e:
                        continue
            
            return games
            
        except Exception as e:
            logger.debug(f"Error getting game log for team {team_id}: {e}")
            return []
    
    def _store_games(self, all_games: Dict[str, Dict[str, Any]], existing_games: Set[str], full_refresh: bool):
        """Store games in database with batch commits."""
        print(f"  Storing games in database...")
        
        # Group games by game_id to merge home/away data
        game_data_merged: Dict[str, Dict[str, Any]] = {}
        
        for game_id, game_info in all_games.items():
            if game_id not in game_data_merged:
                game_data_merged[game_id] = {
                    'game_id': game_id,
                    'game_date': game_info.get('game_date'),
                    'season': game_info.get('season'),
                    'season_type': game_info.get('season_type'),
                }
            
            # Determine home/away teams
            if game_info.get('is_home'):
                game_data_merged[game_id]['home_team_id'] = game_info.get('team_id')
            else:
                game_data_merged[game_id]['away_team_id'] = game_info.get('team_id')
        
        # Now we need to resolve games where we have incomplete home/away data
        # We'll get this from the detailed game info later
        
        with self.db_manager.get_session() as session:
            batch_count = 0
            
            for game_id, game_data in tqdm(game_data_merged.items(), desc="  Saving games"):
                try:
                    # Skip if exists and not full refresh
                    if game_id in existing_games and not full_refresh:
                        self.stats['games_skipped'] += 1
                        continue
                    
                    # Check if game exists
                    existing = session.query(Game).filter_by(game_id=game_id).first()
                    
                    if existing:
                        # Update existing game
                        if game_data.get('game_date'):
                            existing.game_date = game_data['game_date']
                        if game_data.get('home_team_id'):
                            existing.home_team_id = game_data['home_team_id']
                        if game_data.get('away_team_id'):
                            existing.away_team_id = game_data['away_team_id']
                        self.stats['games_updated'] += 1
                    else:
                        # Need both home and away team IDs
                        if not game_data.get('home_team_id') or not game_data.get('away_team_id'):
                            # Will be filled in during stats collection
                            pass
                        
                        # Create new game (will fill in missing data later)
                        new_game = Game(
                            game_id=game_id,
                            season=game_data.get('season', self.season),
                            season_type=game_data.get('season_type', 'Regular Season'),
                            game_date=game_data.get('game_date') or date.today(),
                            home_team_id=game_data.get('home_team_id', '0'),
                            away_team_id=game_data.get('away_team_id', '0'),
                            game_status='scheduled'
                        )
                        session.add(new_game)
                        self.stats['games_new'] += 1
                    
                    batch_count += 1
                    
                    # Commit in batches
                    if batch_count >= self.batch_size:
                        session.commit()
                        batch_count = 0
                        
                except Exception as e:
                    logger.debug(f"Error storing game {game_id}: {e}")
                    self.stats['errors'] += 1
            
            # Final commit
            session.commit()
        
        print(f"  [OK] {self.stats['games_new']} new games, {self.stats['games_updated']} updated, {self.stats['games_skipped']} skipped")
    
    def _collect_game_stats(self, full_refresh: bool = False):
        """Collect detailed stats for all games."""
        print(f"\n[STEP 3] Collecting game statistics...")
        
        # Get games that need stats
        with self.db_manager.get_session() as session:
            games = session.query(Game).filter(
                Game.season == self.season
            ).order_by(Game.game_date).all()
            
            # Convert to list of dicts to avoid detached instance issues
            games_to_process = []
            for game in games:
                # Check if game needs stats
                if not full_refresh:
                    has_stats = session.query(TeamStats).filter_by(game_id=game.game_id).first() is not None
                    if has_stats and game.game_status == 'finished':
                        continue
                
                games_to_process.append({
                    'game_id': game.game_id,
                    'game_date': game.game_date,
                    'home_team_id': game.home_team_id,
                    'away_team_id': game.away_team_id,
                    'game_status': game.game_status
                })
        
        if not games_to_process:
            print("  [OK] All games already have stats")
            return
        
        print(f"  Processing {len(games_to_process)} games...")
        
        # Process games in batches
        for i, game_info in enumerate(tqdm(games_to_process, desc="  Collecting stats")):
            try:
                self._process_single_game(game_info)
                
                # Extra delay every batch to avoid rate limiting
                if (i + 1) % self.batch_size == 0:
                    time.sleep(self.batch_delay)
                    
            except Exception as e:
                logger.debug(f"Error processing game {game_info['game_id']}: {e}")
                self.stats['errors'] += 1
        
        print(f"  [OK] {self.stats['team_stats_added']} team stats, {self.stats['player_stats_added']} player stats collected")
    
    def _process_single_game(self, game_info: Dict[str, Any]):
        """Process a single game - get details and stats in minimal API calls."""
        game_id = game_info['game_id']
        
        try:
            # Get box score (includes team and player stats)
            boxscore = self._retry_api_call(BoxScoreTraditionalV3, game_id=game_id)
            
            if not boxscore:
                return
            
            data = boxscore.get_dict()
            if 'boxScoreTraditional' not in data:
                return
            
            bst = data['boxScoreTraditional']
            home_team_id = str(bst.get('homeTeamId', ''))
            away_team_id = str(bst.get('awayTeamId', ''))
            
            if not home_team_id or not away_team_id:
                return
            
            # Extract scores
            home_score = None
            away_score = None
            if bst.get('homeTeam') and 'statistics' in bst['homeTeam']:
                home_score = bst['homeTeam']['statistics'].get('points')
            if bst.get('awayTeam') and 'statistics' in bst['awayTeam']:
                away_score = bst['awayTeam']['statistics'].get('points')
            
            # Determine game status and winner
            game_status = 'finished' if home_score is not None and away_score is not None else 'scheduled'
            winner = None
            point_diff = None
            if home_score is not None and away_score is not None:
                point_diff = home_score - away_score
                winner = home_team_id if home_score > away_score else away_team_id
            
            # Update game in database
            with self.db_manager.get_session() as session:
                game = session.query(Game).filter_by(game_id=game_id).first()
                
                if game:
                    # Update game with complete info
                    game.home_team_id = home_team_id
                    game.away_team_id = away_team_id
                    game.home_score = home_score
                    game.away_score = away_score
                    game.winner = winner
                    game.point_differential = point_diff
                    game.game_status = game_status
                
                # Extract and store team stats
                for team_key, team_id, is_home in [('homeTeam', home_team_id, True), ('awayTeam', away_team_id, False)]:
                    if team_key in bst and 'statistics' in bst[team_key]:
                        team_stats = self._extract_team_stats(
                            bst[team_key]['statistics'],
                            game_id,
                            team_id,
                            is_home
                        )
                        if team_stats:
                            # Check if stats already exist
                            existing = session.query(TeamStats).filter_by(
                                game_id=game_id,
                                team_id=team_id
                            ).first()
                            
                            if existing:
                                for key, value in team_stats.items():
                                    if key not in ('game_id', 'team_id'):
                                        setattr(existing, key, value)
                            else:
                                session.add(TeamStats(**team_stats))
                                self.stats['team_stats_added'] += 1
                
                # Extract and store player stats
                for team_key, team_id in [('homeTeam', home_team_id), ('awayTeam', away_team_id)]:
                    if team_key in bst and 'players' in bst[team_key]:
                        for player_data in bst[team_key]['players']:
                            player_stats = self._extract_player_stats(
                                player_data,
                                game_id,
                                team_id
                            )
                            if player_stats:
                                # Check if player stats already exist
                                existing = session.query(PlayerStats).filter_by(
                                    game_id=game_id,
                                    player_id=player_stats['player_id']
                                ).first()
                                
                                if not existing:
                                    session.add(PlayerStats(**player_stats))
                                    self.stats['player_stats_added'] += 1
                
                session.commit()
                
        except Exception as e:
            logger.debug(f"Error processing game {game_id}: {e}")
            self.stats['errors'] += 1
    
    def _extract_team_stats(self, stats: Dict[str, Any], game_id: str, team_id: str, is_home: bool) -> Optional[Dict[str, Any]]:
        """Extract team statistics from API response."""
        try:
            def get_int(key: str, default: int = 0) -> int:
                val = stats.get(key, default)
                try:
                    return int(float(val)) if val is not None else default
                except (ValueError, TypeError):
                    return default
            
            def get_float(key: str, default: float = 0.0) -> float:
                val = stats.get(key, default)
                try:
                    return float(val) if val is not None else default
                except (ValueError, TypeError):
                    return default
            
            points = get_int('points')
            fgm = get_int('fieldGoalsMade')
            fga = get_int('fieldGoalsAttempted')
            fg_pct = get_float('fieldGoalsPercentage') * 100 if get_float('fieldGoalsPercentage') < 1 else get_float('fieldGoalsPercentage')
            
            three_pm = get_int('threePointersMade')
            three_pa = get_int('threePointersAttempted')
            three_pct = get_float('threePointersPercentage') * 100 if get_float('threePointersPercentage') < 1 else get_float('threePointersPercentage')
            
            ftm = get_int('freeThrowsMade')
            fta = get_int('freeThrowsAttempted')
            ft_pct = get_float('freeThrowsPercentage') * 100 if get_float('freeThrowsPercentage') < 1 else get_float('freeThrowsPercentage')
            
            orb = get_int('reboundsOffensive')
            drb = get_int('reboundsDefensive')
            trb = get_int('reboundsTotal') or (orb + drb)
            
            # Calculate advanced metrics
            ts_pct = None
            efg_pct = None
            if fga > 0 or fta > 0:
                ts_denom = 2 * (fga + 0.44 * fta)
                if ts_denom > 0:
                    ts_pct = (points / ts_denom) * 100
            if fga > 0:
                efg_pct = ((fgm + 0.5 * three_pm) / fga) * 100
            
            return {
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
                'assists': get_int('assists'),
                'steals': get_int('steals'),
                'blocks': get_int('blocks'),
                'turnovers': get_int('turnovers'),
                'personal_fouls': get_int('foulsPersonal'),
                'true_shooting_percentage': ts_pct,
                'effective_field_goal_percentage': efg_pct
            }
        except Exception as e:
            logger.debug(f"Error extracting team stats: {e}")
            return None
    
    def _extract_player_stats(self, player: Dict[str, Any], game_id: str, team_id: str) -> Optional[Dict[str, Any]]:
        """Extract player statistics from API response."""
        try:
            stats = player.get('statistics', {})
            if not stats:
                return None
            
            player_id = str(player.get('personId', ''))
            first_name = player.get('firstName', '')
            family_name = player.get('familyName', '')
            player_name = f"{first_name} {family_name}".strip()
            
            if not player_name:
                player_name = player.get('nameI', 'Unknown')
            
            def get_int(key: str, default: int = 0) -> int:
                val = stats.get(key, default)
                try:
                    return int(float(val)) if val is not None else default
                except (ValueError, TypeError):
                    return default
            
            points = get_int('points')
            rebounds = get_int('reboundsTotal')
            assists = get_int('assists')
            fga = get_int('fieldGoalsAttempted')
            
            # Skip players who didn't play
            if points == 0 and rebounds == 0 and assists == 0 and fga == 0:
                return None
            
            minutes = stats.get('minutes', '0:00') or '0:00'
            
            plus_minus = None
            if 'plusMinusPoints' in stats:
                try:
                    plus_minus = int(float(stats['plusMinusPoints']))
                except (ValueError, TypeError):
                    pass
            
            return {
                'game_id': game_id,
                'player_id': player_id,
                'team_id': team_id,
                'player_name': player_name,
                'minutes_played': str(minutes),
                'points': points,
                'rebounds': rebounds,
                'assists': assists,
                'field_goals_made': get_int('fieldGoalsMade'),
                'field_goals_attempted': fga,
                'three_pointers_made': get_int('threePointersMade'),
                'three_pointers_attempted': get_int('threePointersAttempted'),
                'free_throws_made': get_int('freeThrowsMade'),
                'free_throws_attempted': get_int('freeThrowsAttempted'),
                'plus_minus': plus_minus,
                'injury_status': 'healthy'
            }
        except Exception as e:
            logger.debug(f"Error extracting player stats: {e}")
            return None
    
    def _validate_data(self):
        """Validate the collected data."""
        print(f"\n[STEP 4] Validating data...")
        
        with self.db_manager.get_session() as session:
            # Count games with complete data
            total_games = session.query(Game).filter(Game.season == self.season).count()
            finished_games = session.query(Game).filter(
                Game.season == self.season,
                Game.game_status == 'finished'
            ).count()
            
            # Count stats records
            team_stats_count = session.query(TeamStats).join(Game).filter(
                Game.season == self.season
            ).count()
            player_stats_count = session.query(PlayerStats).join(Game).filter(
                Game.season == self.season
            ).count()
            
            # Check for games missing stats
            games_with_stats = session.query(TeamStats.game_id).distinct().join(Game).filter(
                Game.season == self.season
            ).count()
            
        print(f"  [OK] Total games: {total_games}")
        print(f"  [OK] Finished games: {finished_games}")
        print(f"  [OK] Games with team stats: {games_with_stats}")
        print(f"  [OK] Team stats records: {team_stats_count}")
        print(f"  [OK] Player stats records: {player_stats_count}")
        
        if finished_games > games_with_stats:
            print(f"  [WARN] {finished_games - games_with_stats} finished games missing stats")
    
    def _print_summary(self):
        """Print final summary."""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds() if self.stats['start_time'] and self.stats['end_time'] else 0
        
        print("\n" + "=" * 70)
        print("ETL PIPELINE COMPLETE")
        print("=" * 70)
        print(f"  Season: {self.season}")
        print(f"  Duration: {duration:.1f} seconds")
        print(f"  API Calls: {self.stats['api_calls']}")
        print(f"  Games Found: {self.stats['games_found']}")
        print(f"  Games New: {self.stats['games_new']}")
        print(f"  Games Updated: {self.stats['games_updated']}")
        print(f"  Games Skipped: {self.stats['games_skipped']}")
        print(f"  Team Stats Added: {self.stats['team_stats_added']}")
        print(f"  Player Stats Added: {self.stats['player_stats_added']}")
        print(f"  Errors: {self.stats['errors']}")
        print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='NBA ETL Pipeline - Extract, Transform, Load season data'
    )
    parser.add_argument(
        '--season',
        type=str,
        default='2025-26',
        help='NBA season (e.g., 2025-26)'
    )
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Re-fetch all data even if already exists'
    )
    parser.add_argument(
        '--stats-only',
        action='store_true',
        help='Only collect stats for existing games (skip game discovery)'
    )
    
    args = parser.parse_args()
    
    # Initialize database
    db_manager = DatabaseManager()
    db_manager.create_tables()
    
    # Run pipeline
    pipeline = ETLPipeline(season=args.season, db_manager=db_manager)
    stats = pipeline.run(
        full_refresh=args.full_refresh,
        stats_only=args.stats_only
    )
    
    return 0 if stats['errors'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())

