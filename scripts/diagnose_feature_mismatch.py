"""
DIAGNOSTIC SCRIPT: Feature Mismatch Analysis

This script establishes ground truth for:
1. What features the model was trained on (from DataLoader)
2. What features would be generated at inference time
3. Exact comparison of schemas (names, count, order)

Run this to understand the root cause of prediction failures.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import json
import logging
from typing import Dict, List, Set, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_training_feature_schema() -> Tuple[List[str], Dict]:
    """
    Determine the actual feature schema used during training.
    Returns the feature names and metadata about which system generated them.
    """
    from src.training.data_loader import DataLoader
    from src.database.db_manager import DatabaseManager
    from src.database.models import TeamRollingFeatures, GameMatchupFeatures, Feature, Game
    
    db_manager = DatabaseManager()
    data_loader = DataLoader(db_manager)
    
    # Count which feature system has data
    with db_manager.get_session() as session:
        rolling_count = session.query(TeamRollingFeatures).count()
        matchup_count = session.query(GameMatchupFeatures).count()
        legacy_feature_count = session.query(Feature).count()
        game_count = session.query(Game).filter(Game.game_status == 'finished').count()
    
    logger.info("=" * 70)
    logger.info("FEATURE SYSTEM AVAILABILITY")
    logger.info("=" * 70)
    logger.info(f"TeamRollingFeatures rows: {rolling_count}")
    logger.info(f"GameMatchupFeatures rows: {matchup_count}")
    logger.info(f"Legacy Feature rows: {legacy_feature_count}")
    logger.info(f"Finished games: {game_count}")
    
    # Load actual training data to get feature names
    logger.info("\n" + "=" * 70)
    logger.info("LOADING TRAINING DATA TO EXTRACT FEATURE SCHEMA")
    logger.info("=" * 70)
    
    try:
        data = data_loader.load_all_data(
            train_seasons=['2022-23', '2023-24'],
            val_seasons=['2024-25'],
            test_seasons=['2025-26'],
            min_features=40
        )
        
        training_features = list(data['X_train'].columns)
        
        metadata = {
            'feature_count': len(training_features),
            'train_samples': len(data['X_train']),
            'val_samples': len(data['X_val']),
            'test_samples': len(data['X_test']),
            'rolling_features_available': rolling_count > 0,
            'matchup_features_available': matchup_count > 0,
            'legacy_features_available': legacy_feature_count > 0,
        }
        
        return training_features, metadata
        
    except Exception as e:
        logger.error(f"Failed to load training data: {e}")
        return [], {'error': str(e)}


def get_schema_from_models() -> Tuple[List[str], int]:
    """
    Extract feature schema directly from database model definitions.
    This is what SHOULD be generated, regardless of what's in DB.
    """
    from src.database.models import TeamRollingFeatures, GameMatchupFeatures
    
    # TeamRollingFeatures columns
    rolling_exclude = {
        'id', 'game_id', 'team_id', 'is_home', 'game_date', 'season',
        'created_at', 'updated_at', 'won_game', 'point_differential'
    }
    
    rolling_columns = [
        col.name for col in TeamRollingFeatures.__table__.columns
        if col.name not in rolling_exclude
    ]
    
    # Name mapping (as per DataLoader)
    name_mapping = {
        'efg_pct': 'effective_fg_pct',
        'ts_pct': 'true_shooting_pct',
        'tov_pct': 'turnover_rate',
    }
    
    # Generate home/away features
    team_features = []
    for prefix in ['home_', 'away_']:
        for col in rolling_columns:
            mapped_name = name_mapping.get(col, col)
            team_features.append(f'{prefix}{mapped_name}')
    
    # GameMatchupFeatures columns
    matchup_exclude = {
        'id', 'game_id', 'game_date', 'season', 'home_team_id', 'away_team_id',
        'created_at', 'updated_at'
    }
    
    matchup_columns = [
        col.name for col in GameMatchupFeatures.__table__.columns
        if col.name not in matchup_exclude
    ]
    
    matchup_mapping = {
        'home_win_pct_recent': 'home_win_pct',
        'away_win_pct_recent': 'away_win_pct',
    }
    
    matchup_features = []
    for col in matchup_columns:
        mapped_name = matchup_mapping.get(col, col)
        matchup_features.append(mapped_name)
    
    all_schema_features = team_features + matchup_features
    
    logger.info("\n" + "=" * 70)
    logger.info("SCHEMA-DERIVED FEATURES (from model definitions)")
    logger.info("=" * 70)
    logger.info(f"TeamRollingFeatures columns: {len(rolling_columns)}")
    logger.info(f"Team features (home + away): {len(team_features)}")
    logger.info(f"GameMatchupFeatures columns: {len(matchup_columns)}")
    logger.info(f"Matchup features: {len(matchup_features)}")
    logger.info(f"Total schema-derived features: {len(all_schema_features)}")
    
    return all_schema_features, len(rolling_columns)


def get_saved_model_expectations() -> Dict[str, Dict]:
    """
    Read all saved model metadata to understand what each model expects.
    """
    models_dir = Path('data/models')
    model_info = {}
    
    for json_file in models_dir.glob('*.json'):
        if 'training_summary' in json_file.name:
            continue
        
        with open(json_file, 'r') as f:
            metadata = json.load(f)
        
        model_name = json_file.stem
        model_info[model_name] = {
            'feature_count': metadata.get('feature_count', metadata.get('n_features')),
            'n_samples': metadata.get('n_samples'),
            'task_type': metadata.get('task_type'),
            'feature_names': metadata.get('feature_names', []),  # Usually missing!
        }
    
    logger.info("\n" + "=" * 70)
    logger.info("SAVED MODEL EXPECTATIONS")
    logger.info("=" * 70)
    for name, info in model_info.items():
        has_names = 'YES' if info['feature_names'] else 'NO'
        logger.info(f"{name}: {info['feature_count']} features, feature_names saved: {has_names}")
    
    return model_info


def compare_schemas(train_features: List[str], schema_features: List[str]) -> Dict:
    """
    Rigorous comparison of feature schemas.
    """
    train_set = set(train_features)
    schema_set = set(schema_features)
    
    train_only = train_set - schema_set
    schema_only = schema_set - train_set
    common = train_set & schema_set
    
    # Order comparison (only if same features)
    order_matches = train_features == sorted(train_features)  # DataLoader sorts alphabetically
    
    comparison = {
        'train_feature_count': len(train_features),
        'schema_feature_count': len(schema_features),
        'common_features': len(common),
        'train_only_features': sorted(train_only),
        'schema_only_features': sorted(schema_only),
        'order_is_alphabetical': order_matches,
        'schemas_identical': train_set == schema_set,
    }
    
    logger.info("\n" + "=" * 70)
    logger.info("SCHEMA COMPARISON")
    logger.info("=" * 70)
    logger.info(f"Training features: {len(train_features)}")
    logger.info(f"Schema-derived features: {len(schema_features)}")
    logger.info(f"Common features: {len(common)}")
    logger.info(f"Training-only features: {len(train_only)}")
    logger.info(f"Schema-only features: {len(schema_only)}")
    logger.info(f"Schemas identical: {comparison['schemas_identical']}")
    
    if train_only:
        logger.warning(f"\nFeatures in training but not in schema:")
        for f in sorted(train_only)[:20]:
            logger.warning(f"  - {f}")
        if len(train_only) > 20:
            logger.warning(f"  ... and {len(train_only) - 20} more")
    
    if schema_only:
        logger.warning(f"\nFeatures in schema but not in training:")
        for f in sorted(schema_only)[:20]:
            logger.warning(f"  - {f}")
        if len(schema_only) > 20:
            logger.warning(f"  ... and {len(schema_only) - 20} more")
    
    return comparison


def check_feature_system_selection_logic():
    """
    Analyze the DataLoader to identify potential non-determinism.
    """
    logger.info("\n" + "=" * 70)
    logger.info("FEATURE SYSTEM SELECTION LOGIC ANALYSIS")
    logger.info("=" * 70)
    
    issues = []
    
    # Check 1: Silent fallback
    issues.append({
        'issue': 'Silent fallback between feature systems',
        'location': 'src/training/data_loader.py:154-193',
        'description': 'DataLoader tries TeamRollingFeatures first, then falls back to Feature table without explicit logging of which system was used per game.',
        'severity': 'CRITICAL',
        'fix': 'Remove fallback logic. Require explicit feature system selection.'
    })
    
    # Check 2: No feature system identifier in metadata
    issues.append({
        'issue': 'Feature system not recorded in model metadata',
        'location': 'Model JSON files',
        'description': 'Model metadata does not record which feature system was used, making post-hoc analysis impossible.',
        'severity': 'HIGH',
        'fix': 'Persist feature_system_name, feature_names list, and feature_version in model metadata.'
    })
    
    # Check 3: Feature name mapping
    issues.append({
        'issue': 'Feature name mapping creates ambiguity',
        'location': 'src/training/data_loader.py:388-393',
        'description': 'efg_pct -> effective_fg_pct, ts_pct -> true_shooting_pct mappings exist. If applied inconsistently, creates mismatch.',
        'severity': 'MEDIUM',
        'fix': 'Standardize on single naming convention. No runtime mapping.'
    })
    
    for issue in issues:
        logger.warning(f"\n[{issue['severity']}] {issue['issue']}")
        logger.warning(f"  Location: {issue['location']}")
        logger.warning(f"  Description: {issue['description']}")
        logger.warning(f"  Fix: {issue['fix']}")
    
    return issues


def main():
    logger.info("=" * 70)
    logger.info("NBA PREDICTION MODEL: FEATURE MISMATCH DIAGNOSTIC")
    logger.info("=" * 70)
    
    # Step 1: Get schema from model definitions
    schema_features, rolling_col_count = get_schema_from_models()
    
    # Step 2: Get saved model expectations
    model_info = get_saved_model_expectations()
    
    # Step 3: Get actual training feature schema
    train_features, train_metadata = get_training_feature_schema()
    
    if not train_features:
        logger.error("Could not extract training features. Check database connection.")
        return
    
    # Step 4: Compare schemas
    comparison = compare_schemas(train_features, schema_features)
    
    # Step 5: Analyze selection logic
    issues = check_feature_system_selection_logic()
    
    # Step 6: Summary
    logger.info("\n" + "=" * 70)
    logger.info("DIAGNOSTIC SUMMARY")
    logger.info("=" * 70)
    
    # Check for model/training mismatch
    for model_name, info in model_info.items():
        expected = info['feature_count']
        actual = len(train_features)
        if expected and expected != actual:
            logger.error(f"MISMATCH: Model '{model_name}' expects {expected} features, current training produces {actual}")
    
    # Final verdict
    if comparison['schemas_identical']:
        logger.info("✓ Training and schema-derived features are IDENTICAL")
    else:
        logger.error("✗ Training and schema-derived features are DIFFERENT")
        logger.error(f"  - {len(comparison['train_only_features'])} features only in training")
        logger.error(f"  - {len(comparison['schema_only_features'])} features only in schema")
    
    # Save results
    results = {
        'training_features': train_features,
        'training_metadata': train_metadata,
        'schema_features': schema_features,
        'comparison': comparison,
        'model_expectations': model_info,
        'issues': issues
    }
    
    output_file = Path('data/feature_diagnostic_results.json')
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logger.info(f"\nDetailed results saved to: {output_file}")
    
    return results


if __name__ == '__main__':
    main()

