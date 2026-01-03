"""
Database migration script to add betting-related columns and tables.

This script:
1. Adds strategy_name and confidence columns to the bets table
2. Creates the bankroll_snapshots table
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import sqlite3
from datetime import datetime

def migrate():
    """Run database migrations."""
    db_path = project_root / 'data' / 'nba_predictions.db'
    
    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return False
    
    print(f"Migrating database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Check if strategy_name column exists in bets table
        cursor.execute("PRAGMA table_info(bets)")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Add strategy_name column if not exists
        if 'strategy_name' not in columns:
            print("Adding strategy_name column to bets table...")
            cursor.execute("ALTER TABLE bets ADD COLUMN strategy_name TEXT DEFAULT 'kelly'")
            print("  Added strategy_name column")
        else:
            print("  strategy_name column already exists")
        
        # Add confidence column if not exists
        if 'confidence' not in columns:
            print("Adding confidence column to bets table...")
            cursor.execute("ALTER TABLE bets ADD COLUMN confidence REAL")
            print("  Added confidence column")
        else:
            print("  confidence column already exists")
        
        # Create bankroll_snapshots table if not exists
        print("Creating bankroll_snapshots table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bankroll_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_name TEXT NOT NULL,
                snapshot_date DATE NOT NULL,
                bankroll REAL NOT NULL,
                daily_pnl REAL NOT NULL DEFAULT 0.0,
                total_bets INTEGER NOT NULL DEFAULT 0,
                wins INTEGER NOT NULL DEFAULT 0,
                losses INTEGER NOT NULL DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(strategy_name, snapshot_date)
            )
        """)
        print("  Created bankroll_snapshots table")
        
        # Create indexes
        print("Creating indexes...")
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_strategy_name ON bets(strategy_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_strategy_date ON bets(strategy_name, placed_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_strategy ON bankroll_snapshots(strategy_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_snapshot_date ON bankroll_snapshots(snapshot_date)")
            print("  Created indexes")
        except sqlite3.OperationalError as e:
            print(f"  Index creation (may already exist): {e}")
        
        conn.commit()
        print("\nMigration complete!")
        return True
        
    except Exception as e:
        print(f"Error during migration: {e}")
        conn.rollback()
        return False
        
    finally:
        conn.close()


if __name__ == '__main__':
    success = migrate()
    sys.exit(0 if success else 1)

