"""
PUBG Weibo Analyzer - CLI Entry Point

Usage:
    python main.py run --days 365           # 전체 파이프라인 실행
    python main.py fetch --days 365         # 데이터 수집만
    python main.py classify                 # 콜라보 분류만
    python main.py aggregate --days 365     # 지표 집계만
    python main.py export --out ./output    # CSV 내보내기만
    python main.py status                   # 파이프라인 상태 확인
"""
import sys
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def cmd_run(args):
    """Run full pipeline."""
    from pipeline.fetch import run_fetch
    from pipeline.classify import run_classify
    from pipeline.aggregate import run_aggregate
    from pipeline.export import run_export

    logger.info("=" * 60)
    logger.info("PUBG Weibo Analyzer - Full Pipeline")
    logger.info("=" * 60)

    # Step 1: Fetch
    logger.info("\n[Step 1/4] Fetching posts and comments...")
    fetch_stats = run_fetch(days=args.days, full=args.full)

    # Step 2: Classify
    logger.info("\n[Step 2/4] Classifying collabs...")
    classify_stats = run_classify(no_gpt=args.no_gpt)

    # Step 3: Aggregate
    logger.info("\n[Step 3/4] Aggregating metrics...")
    agg_stats = run_aggregate(days=args.days)

    # Step 4: Export
    logger.info("\n[Step 4/4] Exporting reports...")
    export_stats = run_export(out=args.out, upload=args.upload)

    logger.info("\n" + "=" * 60)
    logger.info("Pipeline complete!")
    logger.info(f"  Fetch:     {fetch_stats}")
    logger.info(f"  Classify:  {classify_stats}")
    logger.info(f"  Aggregate: {agg_stats}")
    logger.info(f"  Export:    {export_stats}")
    logger.info("=" * 60)


def cmd_fetch(args):
    """Fetch data only."""
    from pipeline.fetch import run_fetch
    stats = run_fetch(
        days=args.days,
        full=args.full,
        fetch_comments=not args.no_comments,
    )
    print(f"Fetch complete: {stats}")


def cmd_classify(args):
    """Classify only."""
    from pipeline.classify import run_classify
    stats = run_classify(no_gpt=args.no_gpt, reclassify=args.reclassify)
    print(f"Classification complete: {stats}")


def cmd_aggregate(args):
    """Aggregate only."""
    from pipeline.aggregate import run_aggregate
    stats = run_aggregate(days=args.days)
    print(f"Aggregation complete: {stats}")


def cmd_export(args):
    """Export only."""
    from pipeline.export import run_export
    stats = run_export(out=args.out, full=args.full, upload=args.upload)
    print(f"Export complete: {stats}")


def cmd_status(args):
    """Show pipeline status."""
    from db.models import init_db, get_conn, get_status_summary
    init_db()

    with get_conn() as conn:
        summary = get_status_summary(conn)

    print("\n" + "=" * 50)
    print("  PUBG Weibo Analyzer - Pipeline Status")
    print("=" * 50)
    print(f"  Total posts:       {summary['total_posts']:>8,}")
    print(f"  Classified:        {summary['classified']:>8,}")
    print(f"  Unclassified:      {summary['unclassified']:>8,}")
    print(f"  Collabs detected:  {summary['collabs']:>8,}")
    print(f"  Total comments:    {summary['total_comments']:>8,}")
    print(f"  Last fetched:      {summary['last_fetched_at'] or 'Never'}")
    print("=" * 50 + "\n")


def main():
    parser = argparse.ArgumentParser(
        prog="PUBG Weibo Analyzer",
        description="和平精英 웨이보 콜라보 분석 파이프라인",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # run
    p_run = subparsers.add_parser("run", help="Run full pipeline")
    p_run.add_argument("--days", type=int, default=365, help="Days to analyze")
    p_run.add_argument("--full", action="store_true", help="Full re-fetch")
    p_run.add_argument("--no-gpt", action="store_true", help="Skip GPT classification")
    p_run.add_argument("--out", default="./output", help="Output path")
    p_run.add_argument("--upload", action="store_true", help="Upload to cloud")
    p_run.set_defaults(func=cmd_run)

    # fetch
    p_fetch = subparsers.add_parser("fetch", help="Fetch posts and comments")
    p_fetch.add_argument("--days", type=int, default=365, help="Days to fetch")
    p_fetch.add_argument("--full", action="store_true", help="Full re-fetch")
    p_fetch.add_argument("--no-comments", action="store_true", help="Skip comments")
    p_fetch.set_defaults(func=cmd_fetch)

    # classify
    p_classify = subparsers.add_parser("classify", help="Classify collabs")
    p_classify.add_argument("--no-gpt", action="store_true", help="Rules only")
    p_classify.add_argument("--reclassify", action="store_true", help="Re-classify all")
    p_classify.set_defaults(func=cmd_classify)

    # aggregate
    p_agg = subparsers.add_parser("aggregate", help="Aggregate metrics")
    p_agg.add_argument("--days", type=int, default=365, help="Days to aggregate")
    p_agg.set_defaults(func=cmd_aggregate)

    # export
    p_export = subparsers.add_parser("export", help="Export CSV reports")
    p_export.add_argument("--out", default="./output/collab_report.csv", help="Output path")
    p_export.add_argument("--full", action="store_true", help="Export all CSVs")
    p_export.add_argument("--upload", action="store_true", help="Upload to cloud")
    p_export.set_defaults(func=cmd_export)

    # status
    p_status = subparsers.add_parser("status", help="Show pipeline status")
    p_status.set_defaults(func=cmd_status)

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
