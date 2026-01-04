"""Migration script to add new columns to TeamRollingFeatures table."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from sqlalchemy import text

def main():
    print("=" * 70)
    print("DATABASE SCHEMA MIGRATION")
    print("=" * 70)
    
    db = DatabaseManager()
    
    # New columns to add to team_rolling_features
    new_columns = [
        ('offensive_rebound_rate', 'REAL'),
        ('defensive_rebound_rate', 'REAL'),
        ('assist_rate', 'REAL'),
        ('steal_rate', 'REAL'),
        ('block_rate', 'REAL'),
        ('avg_point_differential', 'REAL'),
        ('avg_points_for', 'REAL'),
        ('avg_points_against', 'REAL'),
        ('win_streak', 'INTEGER'),
        ('loss_streak', 'INTEGER'),
        ('players_out', 'INTEGER'),
        ('players_questionable', 'INTEGER'),
        ('injury_severity_score', 'REAL'),
    ]
    
    with db.get_session() as session:
        # Check which columns already exist
        result = session.execute(text("PRAGMA table_info(team_rolling_features)"))
        existing_columns = {row[1] for row in result}
        
        print(f"\nExisting columns: {len(existing_columns)}")
        
        # Add missing columns
        added = 0
        for col_name, col_type in new_columns:
            if col_name not in existing_columns:
                try:
                    session.execute(text(f"ALTER TABLE team_rolling_features ADD COLUMN {col_name} {col_type}"))
                    print(f"  [ADDED] {col_name}")
                    added += 1
                except Exception as e:
                    print(f"  [ERROR] {col_name}: {e}")
            else:
                print(f"  [EXISTS] {col_name}")
        
        session.commit()
        
        # Check if game_matchup_features table exists
        result = session.execute(text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='game_matchup_features'
        """))
        table_exists = result.fetchone() is not None
        
        if not table_exists:
            print("\n[INFO] game_matchup_features table will be created automatically")
            print("       Run transform_features.py to create it")
        else:
            print("\n[OK] game_matchup_features table exists")
    
    print(f"\n{'=' * 70}")
    print(f"Migration complete: {added} columns added")
    print(f"{'=' * 70}\n")

if __name__ == '__main__':
    main()



