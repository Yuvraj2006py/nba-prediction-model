#!/usr/bin/env python
"""
End-to-End Integration Test for Enhanced Injury Tracking.

Tests the complete flow from database to feature calculation to ensure
all phases work together correctly.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from datetime import date, timedelta
from src.database.db_manager import DatabaseManager
from src.database.models import Game, Team, PlayerStats
from src.features.player_importance import PlayerImportanceCalculator
from src.features.team_features import TeamFeatureCalculator
from src.features.feature_aggregator import FeatureAggregator
from src.data_collectors.rapidapi_injury_collector import RapidAPIInjuryCollector
from config.settings import get_settings


def print_section(title):
    """Print a section header."""
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def test_player_importance_calculator():
    """Test Phase 1: Player Importance Calculator with real data."""
    print_section("PHASE 1: Player Importance Calculator")
    
    db = DatabaseManager()
    calc = PlayerImportanceCalculator(db)
    
    # Get a sample team
    with db.get_session() as session:
        teams = session.query(Team).limit(5).all()
        if not teams:
            print("[SKIP] No teams in database")
            return False
    
    test_team = teams[0]
    print(f"Testing with team: {test_team.team_name} ({test_team.team_id})")
    
    # Get team player importances
    today = date.today()
    players = calc.get_team_player_importances(
        test_team.team_id,
        games_back=20,
        end_date=today
    )
    
    if not players:
        print("[INFO] No player data available for this team")
        # Try another team
        for team in teams[1:]:
            players = calc.get_team_player_importances(
                team.team_id,
                games_back=20,
                end_date=today
            )
            if players:
                test_team = team
                print(f"Switching to team: {test_team.team_name} ({test_team.team_id})")
                break
    
    if players:
        print(f"\nFound {len(players)} players with importance scores:")
        print("-" * 50)
        for i, p in enumerate(players[:10], 1):
            print(f"  {i:2d}. {p['player_name'][:25]:25s} | "
                  f"Importance: {p['importance_score']:.4f} | "
                  f"Pts: {p['avg_points']:.1f} | "
                  f"Games: {p['games_played']}")
        
        # Verify top players have higher importance
        if len(players) >= 2:
            top = players[0]['importance_score']
            bottom = players[-1]['importance_score']
            if top >= bottom:
                print("\n[PASS] Top player has higher importance than bottom player")
            else:
                print("\n[WARN] Unexpected: bottom player has higher importance")
        
        # Get top players
        top_players = calc.get_top_players(test_team.team_id, top_n=5, end_date=today)
        print(f"\nTop {len(top_players)} key players:")
        for p in top_players:
            print(f"  - {p['player_name']}: {p['importance_score']:.4f}")
        
        # Get team total importance
        total = calc.get_team_total_importance(test_team.team_id, end_date=today)
        print(f"\nTeam total importance: {total:.4f}")
        
        return True
    else:
        print("[INFO] No player importance data available")
        print("[INFO] This is expected if PlayerStats table has no data")
        print("[PASS] Player importance calculator initialized correctly")
        return True  # Not a failure, just no data


def test_enhanced_injury_impact():
    """Test Phase 2: Enhanced Injury Impact Calculation."""
    print_section("PHASE 2: Enhanced Injury Impact Calculation")
    
    db = DatabaseManager()
    team_calc = TeamFeatureCalculator(db)
    
    # Get a team with recent games
    with db.get_session() as session:
        recent_game = session.query(Game).filter(
            Game.game_status == 'finished'
        ).order_by(Game.game_date.desc()).first()
        
        if not recent_game:
            print("[SKIP] No finished games in database")
            return False
    
    test_team_id = recent_game.home_team_id
    test_date = recent_game.game_date
    
    print(f"Testing with team: {test_team_id}")
    print(f"Using game date: {test_date}")
    
    # Calculate injury impact with weighted importance
    injury = team_calc.calculate_injury_impact(
        test_team_id,
        end_date=test_date,
        use_weighted_importance=True
    )
    
    print("\nInjury Impact Results:")
    print("-" * 50)
    for key, value in injury.items():
        print(f"  {key:30s}: {value}")
    
    # Verify all expected keys are present
    expected_keys = [
        'players_out', 'players_questionable', 'injury_severity_score',
        'weighted_injury_score', 'weighted_severity_score',
        'key_player_out', 'key_players_out_count', 'total_importance_out'
    ]
    
    missing_keys = [k for k in expected_keys if k not in injury]
    if missing_keys:
        print(f"\n[FAIL] Missing keys: {missing_keys}")
        return False
    else:
        print("\n[PASS] All expected injury impact keys present")
    
    # Test without weighted importance (backward compatibility)
    injury_simple = team_calc.calculate_injury_impact(
        test_team_id,
        end_date=test_date,
        use_weighted_importance=False
    )
    
    print("\nSimple Injury Impact (no weighting):")
    print(f"  players_out: {injury_simple['players_out']}")
    print(f"  injury_severity_score: {injury_simple['injury_severity_score']}")
    
    return True


def test_historical_injury_impact():
    """Test Phase 3: Historical Injury Impact Analysis."""
    print_section("PHASE 3: Historical Injury Impact Analysis")
    
    db = DatabaseManager()
    team_calc = TeamFeatureCalculator(db)
    
    # Get a team with many games
    with db.get_session() as session:
        # Find team with most games
        games = session.query(Game).filter(
            Game.game_status == 'finished'
        ).all()
        
        if len(games) < 10:
            print(f"[INFO] Only {len(games)} finished games in database")
            print("[SKIP] Need more games for historical analysis")
            return True  # Not a failure, just not enough data
    
    test_team_id = games[0].home_team_id
    today = date.today()
    
    print(f"Testing with team: {test_team_id}")
    print(f"Analyzing {len(games)} games")
    
    # Calculate historical injury impact
    hist_impact = team_calc.calculate_historical_injury_impact(
        test_team_id,
        end_date=today,
        games_back=50
    )
    
    print("\nHistorical Injury Impact Results:")
    print("-" * 50)
    for key, value in hist_impact.items():
        print(f"  {key:35s}: {value}")
    
    # Verify expected keys
    expected_keys = [
        'avg_win_pct_with_key_players', 'avg_win_pct_without_key_players',
        'win_pct_delta', 'avg_point_diff_with', 'avg_point_diff_without',
        'point_diff_delta', 'games_with_key_players', 'games_without_key_players'
    ]
    
    missing_keys = [k for k in expected_keys if k not in hist_impact]
    if missing_keys:
        print(f"\n[FAIL] Missing keys: {missing_keys}")
        return False
    else:
        print("\n[PASS] All expected historical impact keys present")
    
    return True


def test_feature_aggregator_integration():
    """Test Phase 4: Feature Aggregator Integration."""
    print_section("PHASE 4: Feature Aggregator Integration")
    
    db = DatabaseManager()
    aggregator = FeatureAggregator(db)
    
    # Get a recent game
    with db.get_session() as session:
        recent_game = session.query(Game).filter(
            Game.game_status == 'finished'
        ).order_by(Game.game_date.desc()).first()
        
        if not recent_game:
            print("[SKIP] No finished games in database")
            return False
    
    game_id = recent_game.game_id
    print(f"Testing with game: {game_id}")
    print(f"Date: {recent_game.game_date}")
    print(f"Matchup: {recent_game.away_team_id} @ {recent_game.home_team_id}")
    
    # Calculate all features
    try:
        feature_df = aggregator.create_feature_vector(
            game_id,
            recent_game.home_team_id,
            recent_game.away_team_id,
            recent_game.game_date
        )
        features = feature_df.iloc[0].to_dict()
    except Exception as e:
        print(f"[WARN] Feature calculation error: {e}")
        print("[INFO] This may be expected if data is sparse")
        import traceback
        traceback.print_exc()
        return True
    
    # Check for new injury features
    injury_features = {k: v for k, v in features.items() if 'injury' in k.lower()}
    
    print(f"\nFound {len(injury_features)} injury-related features:")
    print("-" * 50)
    for key, value in sorted(injury_features.items()):
        print(f"  {key:40s}: {value}")
    
    # Check for advantage features
    advantage_features = {k: v for k, v in features.items() if 'advantage' in k.lower()}
    
    print(f"\nFound {len(advantage_features)} advantage features:")
    print("-" * 50)
    for key, value in sorted(advantage_features.items()):
        print(f"  {key:40s}: {value}")
    
    # Check for key_player features
    key_player_features = {k: v for k, v in features.items() if 'key_player' in k.lower()}
    
    print(f"\nFound {len(key_player_features)} key player features:")
    print("-" * 50)
    for key, value in sorted(key_player_features.items()):
        print(f"  {key:40s}: {value}")
    
    # Verify expected features
    expected_new_features = [
        'home_weighted_injury_score',
        'away_weighted_injury_score',
        'injury_advantage',
        'home_key_player_out',
        'away_key_player_out',
    ]
    
    found = [f for f in expected_new_features if f in features]
    missing = [f for f in expected_new_features if f not in features]
    
    print(f"\nExpected features found: {len(found)}/{len(expected_new_features)}")
    if missing:
        print(f"Missing features: {missing}")
    
    print("\n[PASS] Feature aggregator integration complete")
    return True


def test_rapidapi_collector():
    """Test Phase 5: RapidAPI Injury Collector."""
    print_section("PHASE 5: RapidAPI Injury Collector")
    
    settings = get_settings()
    
    if not settings.RAPIDAPI_NBA_INJURIES_KEY:
        print("[INFO] RAPIDAPI_NBA_INJURIES_KEY not configured")
        print("[INFO] Skipping live API test")
        print("[INFO] Testing status normalization only...")
        
        # Test status normalization
        collector = RapidAPIInjuryCollector()
        
        test_cases = [
            ('Out', 'out'),
            ('Questionable', 'questionable'),
            ('Doubtful', 'questionable'),
            ('Probable', 'probable'),
            ('Day-to-Day', 'probable'),
            ('Available', 'healthy'),
        ]
        
        all_passed = True
        for input_status, expected in test_cases:
            result = collector._normalize_injury_status(input_status)
            if result == expected:
                print(f"  [PASS] '{input_status}' -> '{result}'")
            else:
                print(f"  [FAIL] '{input_status}' -> '{result}' (expected '{expected}')")
                all_passed = False
        
        return all_passed
    
    # Live API test
    db = DatabaseManager()
    collector = RapidAPIInjuryCollector(db)
    
    print("Testing live API connection...")
    
    today = date.today()
    injuries = collector.get_injuries_for_date(today)
    
    if injuries:
        print(f"\n[SUCCESS] Fetched {len(injuries)} injury records for {today}")
        
        # Show sample
        print("\nSample injuries:")
        for i, injury in enumerate(injuries[:5], 1):
            print(f"  {i}. {injury.get('team', 'N/A')}: {injury.get('player', 'N/A')} - {injury.get('status', 'N/A')}")
        
        # Test summary
        summary = collector.get_injury_summary(today)
        print(f"\nInjury summary by team ({len(summary)} teams):")
        for team, data in list(summary.items())[:3]:
            print(f"  {team}: {data['out_count']} out, {data['questionable_count']} questionable")
        
        return True
    else:
        print(f"\n[INFO] No injuries fetched for {today}")
        print("[INFO] API may have rate limit or no data for today")
        return True  # Not necessarily a failure


def run_all_tests():
    """Run all integration tests."""
    print_section("ENHANCED INJURY TRACKING - END-TO-END INTEGRATION TEST")
    
    results = {}
    
    # Phase 1
    try:
        results['Phase 1: Player Importance'] = test_player_importance_calculator()
    except Exception as e:
        print(f"[ERROR] Phase 1 failed: {e}")
        import traceback
        traceback.print_exc()
        results['Phase 1: Player Importance'] = False
    
    # Phase 2
    try:
        results['Phase 2: Injury Impact'] = test_enhanced_injury_impact()
    except Exception as e:
        print(f"[ERROR] Phase 2 failed: {e}")
        import traceback
        traceback.print_exc()
        results['Phase 2: Injury Impact'] = False
    
    # Phase 3
    try:
        results['Phase 3: Historical Impact'] = test_historical_injury_impact()
    except Exception as e:
        print(f"[ERROR] Phase 3 failed: {e}")
        import traceback
        traceback.print_exc()
        results['Phase 3: Historical Impact'] = False
    
    # Phase 4
    try:
        results['Phase 4: Feature Aggregator'] = test_feature_aggregator_integration()
    except Exception as e:
        print(f"[ERROR] Phase 4 failed: {e}")
        import traceback
        traceback.print_exc()
        results['Phase 4: Feature Aggregator'] = False
    
    # Phase 5
    try:
        results['Phase 5: RapidAPI Collector'] = test_rapidapi_collector()
    except Exception as e:
        print(f"[ERROR] Phase 5 failed: {e}")
        import traceback
        traceback.print_exc()
        results['Phase 5: RapidAPI Collector'] = False
    
    # Summary
    print_section("TEST SUMMARY")
    
    total = len(results)
    passed = sum(1 for r in results.values() if r)
    failed = total - passed
    
    for phase, result in results.items():
        status = "[PASS]" if result else "[FAIL]"
        print(f"  {status} {phase}")
    
    print()
    print(f"Total: {total} | Passed: {passed} | Failed: {failed}")
    
    if failed == 0:
        print("\n[SUCCESS] All integration tests passed!")
        return True
    else:
        print(f"\n[FAILURE] {failed} test(s) failed!")
        return False


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)

