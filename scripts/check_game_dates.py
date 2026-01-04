#!/usr/bin/env python
"""Check game dates to understand why features are None."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamRollingFeatures
from datetime import date

db = DatabaseManager()
today = date.today()

with db.get_session() as session:
    finished = session.query(Game).filter(Game.game_status == 'finished').order_by(Game.game_date).all()
    print(f'Finished games: {len(finished)}')
    print('\nGame dates:')
    for g in finished[:10]:
        print(f'  {g.game_date}: {g.home_team_id} vs {g.away_team_id}')
    
    upcoming = session.query(Game).filter(Game.game_date == today).all()
    print(f'\nToday ({today}) games: {len(upcoming)}')
    
    if upcoming:
        print(f'\nChecking features for first today game: {upcoming[0].game_id}')
        home_feat = session.query(TeamRollingFeatures).filter_by(
            game_id=upcoming[0].game_id,
            team_id=upcoming[0].home_team_id
        ).first()
        
        if home_feat:
            print(f'  l5_points: {home_feat.l5_points}')
            print(f'  l10_points: {home_feat.l10_points}')
            print(f'  l5_win_pct: {home_feat.l5_win_pct}')
            
            # Check how many past games this team has
            team_id = upcoming[0].home_team_id
            past_games = session.query(Game).filter(
                Game.game_date < today,
                Game.game_status == 'finished',
                ((Game.home_team_id == team_id) | (Game.away_team_id == team_id))
            ).all()
            print(f'\n  Past games for {team_id}: {len(past_games)}')
            for pg in past_games[:5]:
                print(f'    {pg.game_date}')
        
        # Check which teams played in finished games vs today
        finished_teams = set()
        for g in finished:
            finished_teams.add(g.home_team_id)
            finished_teams.add(g.away_team_id)
        
        today_teams = set()
        for g in upcoming:
            today_teams.add(g.home_team_id)
            today_teams.add(g.away_team_id)
        
        missing = today_teams - finished_teams
        print(f'\nTeams playing today with no past games: {len(missing)}')
        if missing:
            print(f'  Teams: {list(missing)[:5]}')

