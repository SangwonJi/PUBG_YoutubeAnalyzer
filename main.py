#!/usr/bin/env python3
"""
PUBG MOBILE Collab Pipeline CLI

A data pipeline for collecting, classifying, and analyzing
collaboration content from the PUBG MOBILE YouTube channel.

Usage:
    python main.py fetch --days 365
    python main.py classify
    python main.py aggregate --days 365
    python main.py export --out ./output/collab_report.csv
    python main.py run --days 365  # Run full pipeline
"""

import sys
from pathlib import Path
from datetime import datetime

import click

from config import get_config, validate_config
from db.models import Database


@click.group()
@click.option("--db-path", default=None, help="Path to SQLite database")
@click.pass_context
def cli(ctx, db_path):
    """PUBG MOBILE Collab Analysis Pipeline"""
    ctx.ensure_object(dict)
    
    config = get_config()
    db_path = Path(db_path) if db_path else config.database.db_path
    
    # Ensure data directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    ctx.obj["db"] = Database(db_path)
    ctx.obj["config"] = config


@cli.command()
@click.option("--days", default=365, help="Number of days to fetch (default: 365)")
@click.option("--channel", default="@PUBGMOBILE", help="YouTube channel handle")
@click.option("--full", is_flag=True, help="Full fetch (ignore incremental)")
@click.option("--comments/--no-comments", default=True, help="Also fetch comments")
@click.pass_context
def fetch(ctx, days, channel, full, comments):
    """Fetch videos and comments from YouTube."""
    from pipeline.fetch import fetch_videos, fetch_comments
    
    db = ctx.obj["db"]
    
    # Validate YouTube API key
    validation = validate_config()
    if not validation["youtube_api"]:
        click.echo("Error: YOUTUBE_API_KEY not set in .env")
        sys.exit(1)
    
    click.echo(f"Fetching videos from {channel} (last {days} days)...")
    click.echo(f"Mode: {'Full fetch' if full else 'Incremental'}")
    click.echo()
    
    # Fetch videos
    video_stats = fetch_videos(
        db=db,
        days=days,
        channel_handle=channel,
        incremental=not full
    )
    
    click.echo()
    click.echo(f"Videos: {video_stats['videos_fetched']} fetched "
               f"({video_stats['videos_new']} new, {video_stats['videos_updated']} updated)")
    
    if video_stats["errors"]:
        click.echo(f"Errors: {len(video_stats['errors'])}")
    
    # Fetch comments
    if comments:
        click.echo()
        click.echo("Fetching comments...")
        
        comment_stats = fetch_comments(
            db=db,
            max_comments_per_video=200
        )
        
        click.echo(f"Comments: {comment_stats['comments_fetched']} fetched "
                   f"from {comment_stats['videos_processed']} videos")
    
    click.echo()
    click.echo("Fetch complete!")


@cli.command()
@click.option("--gpt/--no-gpt", default=True, help="Use GPT for ambiguous cases")
@click.option("--reclassify", is_flag=True, help="Reclassify all videos")
@click.option("--threshold", default=0.5, help="Confidence threshold for GPT usage")
@click.pass_context
def classify(ctx, gpt, reclassify, threshold):
    """Classify videos as collab or non-collab."""
    from pipeline.classify import classify_collabs, normalize_partners
    
    db = ctx.obj["db"]
    
    # Validate GPT API key if using GPT
    if gpt:
        validation = validate_config()
        if not validation["gpt_api"]:
            click.echo("Warning: GPT_API_KEY not set. Using rule-based only.")
            gpt = False
    
    click.echo("Classifying videos...")
    click.echo(f"GPT: {'enabled' if gpt else 'disabled'}")
    click.echo(f"Mode: {'Reclassify all' if reclassify else 'Unclassified only'}")
    click.echo()
    
    stats = classify_collabs(
        db=db,
        use_gpt=gpt,
        gpt_threshold=threshold,
        reclassify_all=reclassify
    )
    
    click.echo()
    click.echo(f"Processed: {stats['total_processed']} videos")
    click.echo(f"Collabs found: {stats['collabs_found']}")
    click.echo(f"Non-collabs: {stats['non_collabs']}")
    click.echo(f"Rule-based: {stats['rule_classified']}, GPT: {stats['gpt_classified']}")
    
    # Normalize partner names
    click.echo()
    click.echo("Normalizing partner names...")
    normalize_stats = normalize_partners(db)
    click.echo(f"Normalized: {normalize_stats['partners_normalized']} partner name variations")
    
    click.echo()
    click.echo("Classification complete!")


@cli.command()
@click.option("--days", default=365, help="Number of days to aggregate")
@click.pass_context
def aggregate(ctx, days):
    """Aggregate collab metrics by partner."""
    from pipeline.aggregate import aggregate_collabs, get_partner_rankings
    
    db = ctx.obj["db"]
    
    click.echo(f"Aggregating collab metrics (last {days} days)...")
    click.echo()
    
    stats = aggregate_collabs(db=db, days=days)
    
    click.echo()
    click.echo(f"Partners processed: {stats['partners_processed']}")
    click.echo(f"Total views: {stats['total_views']:,}")
    
    # Show top 10
    click.echo()
    click.echo("Top 10 Partners by Views:")
    click.echo("-" * 60)
    
    rankings = get_partner_rankings(db, metric="total_views", limit=10)
    for r in rankings:
        click.echo(f"{r['rank']:2}. {r['partner_name']:<25} "
                   f"Views: {r['total_views']:>12,}  "
                   f"Videos: {r['video_count']}")
    
    click.echo()
    click.echo("Aggregation complete!")


@cli.command()
@click.option("--out", "-o", default="./output/collab_report.csv", 
              help="Output file path")
@click.option("--days", default=365, help="Number of days to include")
@click.option("--full", is_flag=True, help="Generate full report with all CSVs")
@click.option("--sentiment", is_flag=True, help="Include sentiment analysis (requires GPT)")
@click.option("--upload", is_flag=True, help="Upload to cloud storage")
@click.pass_context
def export(ctx, out, days, full, sentiment, upload):
    """Export collab report to CSV."""
    from pipeline.export import (
        export_to_csv, export_report, upload_to_cloud
    )
    
    db = ctx.obj["db"]
    config = ctx.obj["config"]
    
    click.echo(f"Exporting collab report...")
    click.echo()
    
    if full:
        # Full report
        output_dir = Path(out).parent if not Path(out).is_dir() else Path(out)
        results = export_report(
            db=db,
            output_dir=output_dir,
            days=days,
            include_sentiment=sentiment
        )
        
        click.echo()
        click.echo("Generated files:")
        for name, path in results.items():
            if path:
                click.echo(f"  - {name}: {path}")
        
        # Upload if requested
        if upload:
            files = [Path(p) for p in results.values() if p and Path(p).exists()]
            upload_results = upload_to_cloud(files)
            
            if upload_results["uploaded"]:
                click.echo()
                click.echo("Uploaded files:")
                for item in upload_results["uploaded"]:
                    click.echo(f"  - {item['path']}")
    else:
        # Single report
        output_path = export_to_csv(
            db=db,
            output_path=out,
            days=days
        )
        
        click.echo(f"Exported to: {output_path}")
        
        if upload:
            results = upload_to_cloud([output_path])
            if results["uploaded"]:
                click.echo(f"Uploaded: {results['uploaded'][0]['url'] or 'success'}")
    
    click.echo()
    click.echo("Export complete!")


@cli.command()
@click.option("--days", default=365, help="Number of days to process")
@click.option("--channel", default="@PUBGMOBILE", help="YouTube channel handle")
@click.option("--gpt/--no-gpt", default=True, help="Use GPT for classification")
@click.option("--out", "-o", default="./output", help="Output directory")
@click.pass_context
def run(ctx, days, channel, gpt, out):
    """Run the full pipeline (fetch → classify → aggregate → export)."""
    from pipeline.fetch import fetch_videos, fetch_comments
    from pipeline.classify import classify_collabs, normalize_partners
    from pipeline.aggregate import aggregate_collabs
    from pipeline.export import export_report
    
    db = ctx.obj["db"]
    
    # Validate API keys
    validation = validate_config()
    if not validation["youtube_api"]:
        click.echo("Error: YOUTUBE_API_KEY not set in .env")
        sys.exit(1)
    
    click.echo("=" * 60)
    click.echo("PUBG MOBILE Collab Pipeline - Full Run")
    click.echo("=" * 60)
    click.echo(f"Channel: {channel}")
    click.echo(f"Period: Last {days} days")
    click.echo(f"GPT: {'enabled' if gpt else 'disabled'}")
    click.echo("=" * 60)
    click.echo()
    
    # Step 1: Fetch
    click.echo("STEP 1: Fetching videos...")
    click.echo("-" * 40)
    video_stats = fetch_videos(db=db, days=days, channel_handle=channel)
    click.echo(f"→ {video_stats['videos_fetched']} videos fetched")
    click.echo()
    
    click.echo("Fetching comments...")
    comment_stats = fetch_comments(db=db)
    click.echo(f"→ {comment_stats['comments_fetched']} comments fetched")
    click.echo()
    
    # Step 2: Classify
    click.echo("STEP 2: Classifying collabs...")
    click.echo("-" * 40)
    
    if gpt and not validation["gpt_api"]:
        click.echo("Warning: GPT_API_KEY not set. Using rule-based only.")
        gpt = False
    
    classify_stats = classify_collabs(db=db, use_gpt=gpt)
    click.echo(f"→ {classify_stats['collabs_found']} collabs found")
    
    normalize_partners(db)
    click.echo()
    
    # Step 3: Aggregate
    click.echo("STEP 3: Aggregating metrics...")
    click.echo("-" * 40)
    agg_stats = aggregate_collabs(db=db, days=days)
    click.echo(f"→ {agg_stats['partners_processed']} partners processed")
    click.echo(f"→ {agg_stats['total_views']:,} total views")
    click.echo()
    
    # Step 4: Export
    click.echo("STEP 4: Exporting reports...")
    click.echo("-" * 40)
    output_dir = Path(out)
    results = export_report(db=db, output_dir=output_dir, days=days)
    click.echo()
    
    # Summary
    click.echo("=" * 60)
    click.echo("Pipeline Complete!")
    click.echo("=" * 60)
    click.echo(f"Videos processed: {video_stats['videos_fetched']}")
    click.echo(f"Collabs identified: {classify_stats['collabs_found']}")
    click.echo(f"Partners analyzed: {agg_stats['partners_processed']}")
    click.echo(f"Output directory: {output_dir.absolute()}")
    click.echo()


@cli.command()
@click.pass_context
def status(ctx):
    """Show pipeline status and database statistics."""
    db = ctx.obj["db"]
    config = ctx.obj["config"]
    
    click.echo("PUBG MOBILE Collab Pipeline Status")
    click.echo("=" * 50)
    click.echo()
    
    # API Configuration
    click.echo("API Configuration:")
    validation = validate_config()
    click.echo(f"  YouTube API: {'[OK] Configured' if validation['youtube_api'] else '[X] Not set'}")
    click.echo(f"  GPT API: {'[OK] Configured' if validation['gpt_api'] else '[X] Not set'}")
    click.echo(f"  Cloud API: {'[OK] Configured' if validation['cloud_api'] else '[X] Not set'}")
    click.echo()
    
    # Database
    click.echo(f"Database: {config.database.db_path}")
    click.echo(f"  Videos: {db.get_video_count()}")
    click.echo(f"  Comments: {db.get_comment_count()}")
    
    last_date = db.get_last_video_date()
    if last_date:
        click.echo(f"  Latest video: {last_date.strftime('%Y-%m-%d')}")
    click.echo()
    
    # Collab stats
    collab_videos = db.get_collab_videos()
    click.echo(f"Collabs identified: {len(collab_videos)}")
    
    aggs = db.get_all_collab_aggs()
    if aggs:
        click.echo(f"Partners tracked: {len(aggs)}")
        total_views = sum(a.total_views for a in aggs)
        click.echo(f"Total collab views: {total_views:,}")
    click.echo()


@cli.command()
@click.option("--target", type=click.Choice(["csv", "db", "both"]), default="csv")
@click.pass_context
def upload(ctx, target):
    """Upload files to cloud storage."""
    from pipeline.export import upload_to_cloud
    
    config = ctx.obj["config"]
    
    validation = validate_config()
    if not validation["cloud_api"]:
        click.echo("Error: Cloud API not configured.")
        click.echo("Set CLOUD_API_KEY and CLOUD_UPLOAD_URL in .env")
        sys.exit(1)
    
    files = []
    
    if target in ("csv", "both"):
        # Find latest CSV files
        output_dir = config.output_dir
        csv_files = list(output_dir.glob("*.csv"))
        if csv_files:
            files.extend(csv_files)
            click.echo(f"Found {len(csv_files)} CSV files")
    
    if target in ("db", "both"):
        db_path = config.database.db_path
        if db_path.exists():
            files.append(db_path)
            click.echo(f"Found database: {db_path}")
    
    if not files:
        click.echo("No files found to upload.")
        sys.exit(1)
    
    click.echo()
    results = upload_to_cloud(files)
    
    if results["uploaded"]:
        click.echo()
        click.echo(f"Successfully uploaded {len(results['uploaded'])} files")
    
    if results["failed"]:
        click.echo()
        click.echo(f"Failed to upload {len(results['failed'])} files:")
        for item in results["failed"]:
            click.echo(f"  - {item['path']}: {item['error']}")


if __name__ == "__main__":
    cli()
