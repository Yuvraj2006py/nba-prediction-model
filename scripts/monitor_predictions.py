"""Script to monitor prediction performance and send alerts."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import argparse
import logging
import json
from src.monitoring.prediction_monitor import PredictionMonitor
from config.settings import get_settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Monitor prediction performance and send alerts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check specific model
  python scripts/monitor_predictions.py --model nba_classifier
  
  # Check all models
  python scripts/monitor_predictions.py --all-models
  
  # Export results to JSON
  python scripts/monitor_predictions.py --model nba_classifier --export results.json
        """
    )
    
    parser.add_argument(
        '--model',
        type=str,
        help='Model name to monitor'
    )
    parser.add_argument(
        '--all-models',
        action='store_true',
        help='Monitor all models found in models directory'
    )
    parser.add_argument(
        '--accuracy-days',
        type=int,
        default=7,
        help='Days back to check accuracy (default: 7)'
    )
    parser.add_argument(
        '--missing-days',
        type=int,
        default=1,
        help='Days ahead to check for missing predictions (default: 1)'
    )
    parser.add_argument(
        '--export',
        type=str,
        help='Export results to JSON file'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Only show alerts, not all checks'
    )
    
    args = parser.parse_args()
    
    monitor = PredictionMonitor()
    
    # Determine which models to check
    models_to_check = []
    
    if args.all_models:
        # Find all models in models directory
        settings = get_settings()
        models_dir = Path(settings.MODELS_DIR)
        model_files = list(models_dir.glob("*.pkl"))
        models_to_check = [f.stem for f in model_files]
        logger.info(f"Found {len(models_to_check)} models to check")
    elif args.model:
        models_to_check = [args.model]
    else:
        parser.error("Must specify --model or --all-models")
    
    if not models_to_check:
        logger.warning("No models found to monitor")
        return 0
    
    # Run health checks
    all_results = {}
    
    for model_name in models_to_check:
        logger.info(f"\n{'=' * 70}")
        logger.info(f"Monitoring: {model_name}")
        logger.info(f"{'=' * 70}")
        
        results = monitor.run_health_check(
            model_name,
            accuracy_days=args.accuracy_days,
            missing_days=args.missing_days
        )
        
        all_results[model_name] = results
        
        # Print results
        if not args.quiet or results['overall_status'] == 'alert':
            # Accuracy
            if 'accuracy' in results['checks']:
                acc = results['checks']['accuracy']
                if acc.get('status') != 'insufficient_data':
                    logger.info(f"\nAccuracy ({args.accuracy_days} days):")
                    logger.info(f"  {acc.get('correct_predictions', 0)}/{acc.get('total_games', 0)} correct")
                    logger.info(f"  Accuracy: {acc.get('accuracy', 0):.1%}")
                    if acc.get('status') == 'alert':
                        logger.warning(f"  ALERT: {acc.get('alert')}")
            
            # Missing predictions
            if 'missing_predictions' in results['checks']:
                missing = results['checks']['missing_predictions']
                logger.info(f"\nMissing Predictions (next {args.missing_days} days):")
                logger.info(f"  {missing.get('missing', 0)}/{missing.get('total_upcoming', 0)} games missing predictions")
                if missing.get('status') == 'alert':
                    logger.warning(f"  ALERT: {missing.get('alert')}")
                    if missing.get('missing_game_ids'):
                        logger.warning(f"  Missing game IDs: {', '.join(missing['missing_game_ids'][:10])}")
                        if len(missing['missing_game_ids']) > 10:
                            logger.warning(f"  ... and {len(missing['missing_game_ids']) - 10} more")
            
            # Calibration
            if 'calibration' in results['checks']:
                cal = results['checks']['calibration']
                if cal.get('status') != 'insufficient_data' and cal.get('calibration'):
                    logger.info(f"\nConfidence Calibration:")
                    for bin_key, stats in cal.get('calibration', {}).items():
                        logger.info(f"  {bin_key}: {stats['accuracy']:.1%} accuracy ({stats['count']} games)")
        
        # Overall status
        if results['overall_status'] == 'alert':
            logger.warning(f"\n{'=' * 70}")
            logger.warning(f"ALERT: {model_name} has issues")
            logger.warning(f"{'=' * 70}")
        else:
            logger.info(f"\nStatus: OK")
    
    # Print summary
    logger.info(f"\n{'=' * 70}")
    logger.info("Summary")
    logger.info(f"{'=' * 70}")
    
    alert_count = sum(1 for r in all_results.values() if r['overall_status'] == 'alert')
    logger.info(f"Models checked: {len(all_results)}")
    logger.info(f"Models with alerts: {alert_count}")
    
    # Export if requested
    if args.export:
        with open(args.export, 'w') as f:
            json.dump(all_results, f, indent=2, default=str)
        logger.info(f"\nResults exported to: {args.export}")
    
    # Get all alerts
    alerts = monitor.get_all_alerts()
    if alerts:
        logger.info(f"\n{'=' * 70}")
        logger.info(f"All Alerts ({len(alerts)})")
        logger.info(f"{'=' * 70}")
        for alert in alerts:
            logger.warning(f"{alert['type']}: {alert.get('message', alert)}")
    
    return 0 if alert_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())



