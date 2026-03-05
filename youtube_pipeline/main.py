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
@click.option("--all", "fetch_all", is_flag=True, help="Fetch ALL videos (ignore days limit)")
@click.option("--channel", default="pubgm", 
              type=click.Choice(["pubgm", "freefire", "all"]),
              help="Channel to fetch: pubgm, freefire, or all")
@click.option("--full", is_flag=True, help="Full fetch (ignore incremental)")
@click.option("--comments/--no-comments", default=True, help="Also fetch comments")
@click.pass_context
def fetch(ctx, days, fetch_all, channel, full, comments):
    """Fetch videos and comments from YouTube."""
    from pipeline.fetch import fetch_videos, fetch_comments
    
    db = ctx.obj["db"]
    config = ctx.obj["config"]
    
    # Validate YouTube API key
    validation = validate_config()
    if not validation["youtube_api"]:
        click.echo("Error: YOUTUBE_API_KEY not set in .env")
        sys.exit(1)
    
    # Determine which channels to fetch
    if channel == "all":
        channels_to_fetch = config.youtube.channels
    else:
        ch = config.youtube.get_channel(channel)
        if not ch:
            click.echo(f"Error: Unknown channel '{channel}'")
            sys.exit(1)
        channels_to_fetch = [ch]
    
    # If --all flag is set, fetch all videos
    if fetch_all:
        days = None
    
    total_stats = {"videos_fetched": 0, "videos_new": 0, "videos_updated": 0, "errors": []}
    
    for ch_config in channels_to_fetch:
        click.echo("=" * 60)
        click.echo(f"Channel: {ch_config.name} ({ch_config.handle})")
        click.echo("=" * 60)
        
        if fetch_all:
            click.echo(f"Fetching ALL videos...")
        else:
            click.echo(f"Fetching videos (last {days} days)...")
        click.echo(f"Mode: {'Full fetch' if full else 'Incremental'}")
        click.echo()
        
        # Fetch videos
        video_stats = fetch_videos(
            db=db,
            days=days,
            channel_handle=ch_config.handle,
            source_channel=ch_config.id,
            incremental=not full
        )
        
        click.echo()
        click.echo(f"Videos: {video_stats['videos_fetched']} fetched "
                   f"({video_stats['videos_new']} new, {video_stats['videos_updated']} updated)")
        
        total_stats["videos_fetched"] += video_stats["videos_fetched"]
        total_stats["videos_new"] += video_stats["videos_new"]
        total_stats["videos_updated"] += video_stats["videos_updated"]
        total_stats["errors"].extend(video_stats.get("errors", []))
        
        if video_stats.get("errors"):
            click.echo(f"Errors: {len(video_stats['errors'])}")
        
        # Fetch comments
        if comments:
            click.echo()
            click.echo("Fetching comments...")
            
            comment_stats = fetch_comments(
                db=db,
                max_comments_per_video=200,
                source_channel=ch_config.id
            )
            
            click.echo(f"Comments: {comment_stats['comments_fetched']} fetched "
                       f"from {comment_stats['videos_processed']} videos")
        
        click.echo()
    
    click.echo("=" * 60)
    click.echo(f"Fetch complete! Total: {total_stats['videos_fetched']} videos "
               f"({total_stats['videos_new']} new)")
    click.echo("=" * 60)


@cli.command()
@click.option("--gpt/--no-gpt", default=True, help="Use GPT for ambiguous cases")
@click.option("--reclassify", is_flag=True, help="Reclassify all videos")
@click.option("--threshold", default=0.5, help="Confidence threshold for GPT usage")
@click.option("--channel", default=None, 
              type=click.Choice(["pubgm", "freefire"]),
              help="Filter by channel: pubgm or freefire")
@click.pass_context
def classify(ctx, gpt, reclassify, threshold, channel):
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
    click.echo(f"Channel: {channel or 'all'}")
    click.echo(f"Mode: {'Reclassify all' if reclassify else 'Unclassified only'}")
    click.echo()
    
    stats = classify_collabs(
        db=db,
        use_gpt=gpt,
        gpt_threshold=threshold,
        reclassify_all=reclassify,
        source_channel=channel
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
@click.option("--all", "fetch_all", is_flag=True, help="Aggregate ALL data (ignore days limit)")
@click.pass_context
def aggregate(ctx, days, fetch_all):
    """Aggregate collab metrics by partner."""
    from pipeline.aggregate import aggregate_collabs, get_partner_rankings
    
    db = ctx.obj["db"]
    
    if fetch_all:
        days = None
        click.echo("Aggregating ALL collab metrics...")
    else:
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
@click.option("--all", "fetch_all", is_flag=True, help="Export ALL data (ignore days limit)")
@click.option("--full", is_flag=True, help="Generate full report with all CSVs")
@click.option("--sentiment", is_flag=True, help="Include sentiment analysis (requires GPT)")
@click.option("--upload", is_flag=True, help="Upload to cloud storage")
@click.pass_context
def export(ctx, out, days, fetch_all, full, sentiment, upload):
    """Export collab report to CSV."""
    from pipeline.export import (
        export_to_csv, export_report, upload_to_cloud
    )
    
    db = ctx.obj["db"]
    config = ctx.obj["config"]
    
    if fetch_all:
        days = None
        click.echo("Exporting ALL collab data...")
    else:
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
@click.option("--all", "fetch_all", is_flag=True, help="Fetch ALL videos (ignore days limit)")
@click.option("--channel", default="pubgm",
              type=click.Choice(["pubgm", "freefire", "all"]),
              help="Channel: pubgm, freefire, or all")
@click.option("--gpt/--no-gpt", default=True, help="Use GPT for classification")
@click.option("--out", "-o", default="./output", help="Output directory")
@click.pass_context
def run(ctx, days, fetch_all, channel, gpt, out):
    """Run the full pipeline (fetch → classify → aggregate → export)."""
    from pipeline.fetch import fetch_videos, fetch_comments
    from pipeline.classify import classify_collabs, normalize_partners
    from pipeline.aggregate import aggregate_collabs
    from pipeline.export import export_report
    
    db = ctx.obj["db"]
    config = ctx.obj["config"]
    
    # Validate API keys
    validation = validate_config()
    if not validation["youtube_api"]:
        click.echo("Error: YOUTUBE_API_KEY not set in .env")
        sys.exit(1)
    
    # Determine which channels to process
    if channel == "all":
        channels_to_process = config.youtube.channels
    else:
        ch = config.youtube.get_channel(channel)
        if not ch:
            click.echo(f"Error: Unknown channel '{channel}'")
            sys.exit(1)
        channels_to_process = [ch]
    
    # If --all flag is set, fetch all videos
    if fetch_all:
        days = None
    
    click.echo("=" * 60)
    click.echo("YouTube Collab Pipeline - Full Run")
    click.echo("=" * 60)
    channel_names = ", ".join([c.name for c in channels_to_process])
    click.echo(f"Channels: {channel_names}")
    click.echo(f"Period: {'ALL videos' if days is None else f'Last {days} days'}")
    click.echo(f"GPT: {'enabled' if gpt else 'disabled'}")
    click.echo("=" * 60)
    click.echo()
    
    total_videos = 0
    total_collabs = 0
    total_partners = 0
    total_views = 0
    
    for ch_config in channels_to_process:
        click.echo()
        click.echo("#" * 60)
        click.echo(f"# Processing: {ch_config.name}")
        click.echo("#" * 60)
        click.echo()
        
        # Step 1: Fetch
        click.echo("STEP 1: Fetching videos...")
        click.echo("-" * 40)
        video_stats = fetch_videos(
            db=db, 
            days=days, 
            channel_handle=ch_config.handle,
            source_channel=ch_config.id
        )
        click.echo(f"→ {video_stats['videos_fetched']} videos fetched")
        total_videos += video_stats['videos_fetched']
        click.echo()
        
        click.echo("Fetching comments...")
        comment_stats = fetch_comments(db=db, source_channel=ch_config.id)
        click.echo(f"→ {comment_stats['comments_fetched']} comments fetched")
        click.echo()
        
        # Step 2: Classify
        click.echo("STEP 2: Classifying collabs...")
        click.echo("-" * 40)
        
        if gpt and not validation["gpt_api"]:
            click.echo("Warning: GPT_API_KEY not set. Using rule-based only.")
            gpt = False
        
        classify_stats = classify_collabs(db=db, use_gpt=gpt, source_channel=ch_config.id)
        click.echo(f"→ {classify_stats['collabs_found']} collabs found")
        total_collabs += classify_stats['collabs_found']
        
        normalize_partners(db)
        click.echo()
        
        # Step 3: Aggregate
        click.echo("STEP 3: Aggregating metrics...")
        click.echo("-" * 40)
        agg_stats = aggregate_collabs(db=db, days=days, source_channel=ch_config.id)
        click.echo(f"→ {agg_stats['partners_processed']} partners processed")
        click.echo(f"→ {agg_stats['total_views']:,} total views")
        total_partners += agg_stats['partners_processed']
        total_views += agg_stats['total_views']
        click.echo()
    
    # Step 4: Export (all channels)
    click.echo()
    click.echo("STEP 4: Exporting reports...")
    click.echo("-" * 40)
    output_dir = Path(out)
    results = export_report(db=db, output_dir=output_dir, days=days)
    click.echo()
    
    # Summary
    click.echo("=" * 60)
    click.echo("Pipeline Complete!")
    click.echo("=" * 60)
    click.echo(f"Total videos processed: {total_videos}")
    click.echo(f"Total collabs identified: {total_collabs}")
    click.echo(f"Total partners analyzed: {total_partners}")
    click.echo(f"Total collab views: {total_views:,}")
    click.echo(f"Output directory: {output_dir.absolute()}")
    click.echo()


@cli.command()
@click.pass_context
def status(ctx):
    """Show pipeline status and database statistics."""
    db = ctx.obj["db"]
    config = ctx.obj["config"]
    
    click.echo("YouTube Collab Pipeline Status")
    click.echo("=" * 50)
    click.echo()
    
    # API Configuration
    click.echo("API Configuration:")
    validation = validate_config()
    click.echo(f"  YouTube API: {'[OK] Configured' if validation['youtube_api'] else '[X] Not set'}")
    click.echo(f"  GPT API: {'[OK] Configured' if validation['gpt_api'] else '[X] Not set'}")
    click.echo(f"  Cloud API: {'[OK] Configured' if validation['cloud_api'] else '[X] Not set'}")
    click.echo()
    
    # Configured channels
    click.echo("Configured Channels:")
    for ch in config.youtube.channels:
        click.echo(f"  - {ch.name} ({ch.id}): {ch.handle}")
    click.echo()
    
    # Database
    click.echo(f"Database: {config.database.db_path}")
    click.echo(f"  Total Videos: {db.get_video_count()}")
    click.echo(f"  Total Comments: {db.get_comment_count()}")
    click.echo()
    
    # Per-channel stats
    click.echo("Videos by Channel:")
    for ch in config.youtube.channels:
        count = db.get_video_count_by_channel(ch.id)
        collab_count = len(db.get_collab_videos_by_channel(ch.id, days=None))
        click.echo(f"  {ch.name}: {count} videos ({collab_count} collabs)")
    click.echo()
    
    last_date = db.get_last_video_date()
    if last_date:
        click.echo(f"Latest video: {last_date.strftime('%Y-%m-%d')}")
    click.echo()
    
    # Total collab stats
    aggs = db.get_all_collab_aggs()
    if aggs:
        click.echo(f"Total Partners tracked: {len(aggs)}")
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
