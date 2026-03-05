"""
Collab classification pipeline.
Uses rule-based classification first, then GPT for ambiguous cases.
"""

import re
from typing import Optional, Callable
from tqdm import tqdm

from config import get_config
from db.models import Database, Video
from clients.gpt_client import GPTClient, CollabClassification


def _rule_based_classify(
    title: str,
    description: str,
    config
) -> Optional[CollabClassification]:
    """
    Attempt rule-based collab classification.
    
    Returns:
        CollabClassification if confident, None if ambiguous
    """
    text = f"{title} {description}".lower()
    
    # Check for collab keywords
    has_collab_keyword = any(
        kw.lower() in text for kw in config.collab_keywords
    )
    
    if not has_collab_keyword:
        return None
    
    # Try to extract partner name from title patterns
    # Common patterns: "PUBG MOBILE x Partner", "PUBG MOBILE × Partner", 
    # "Partner Collab", "[Partner] Event"
    
    partner_patterns = [
        r"(?:pubg\s*mobile\s*)?[x×]\s*([A-Za-z0-9\s\-\']+?)(?:\s*[-–|:]|\s*collab|\s*event|\s*update|$)",
        r"(?:with|featuring|feat\.?|ft\.?)\s+([A-Za-z0-9\s\-\']+?)(?:\s*[-–|:]|\s*!|$)",
        r"\[([A-Za-z0-9\s\-\']+?)\]\s*(?:collab|event|crossover)",
        r"([A-Za-z0-9\s\-\']+?)\s*(?:콜라보|コラボ)",
    ]
    
    partner_name = None
    for pattern in partner_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            # Filter out common false positives
            if candidate.lower() not in ['pubg', 'mobile', 'pubg mobile', 'the', 'a', 'an']:
                partner_name = candidate
                break
    
    # Normalize partner name using aliases
    if partner_name:
        normalized = partner_name.lower().strip()
        for alias, canonical in config.partner_aliases.items():
            if alias in normalized or normalized in alias:
                partner_name = canonical
                break
    
    # If we found a partner name, this is a confident collab
    if partner_name:
        # Try to determine category
        category = _guess_category(partner_name, text)
        region = _guess_region(text)
        
        return CollabClassification(
            is_collab=True,
            partner_name=partner_name,
            category=category,
            region=region,
            one_line_summary=f"Collaboration with {partner_name}",
            confidence=0.85
        )
    
    # Has collab keyword but couldn't extract partner - return low confidence
    return CollabClassification(
        is_collab=True,
        partner_name=None,
        category="Other",
        region="Unknown",
        one_line_summary="Potential collaboration (partner unidentified)",
        confidence=0.4
    )


def _guess_category(partner_name: str, text: str) -> str:
    """Guess the collab category based on partner name and text."""
    partner_lower = partner_name.lower()
    
    # Known categories by partner
    anime_partners = [
        'dragon ball', 'jujutsu kaisen', 'evangelion', 'attack on titan',
        'naruto', 'one piece', 'demon slayer', 'spy x family'
    ]
    game_partners = [
        'resident evil', 'metro', 'assassin', 'tomb raider', 'league of legends',
        'valorant', 'fortnite', 'call of duty', 'apex'
    ]
    movie_partners = [
        'godzilla', 'kong', 'arcane', 'the boys', 'walking dead',
        'dune', 'matrix', 'john wick', 'transformers'
    ]
    artist_partners = [
        'blackpink', 'newjeans', 'bts', 'alan walker', 'marshmello',
        'dj', 'band', 'singer'
    ]
    brand_partners = [
        'lamborghini', 'mclaren', 'bugatti', 'koenigsegg', 'aston martin',
        'ferrari', 'porsche', 'bmw', 'mercedes', 'audi',
        'nike', 'adidas', 'puma', 'supreme'
    ]
    
    for anime in anime_partners:
        if anime in partner_lower:
            return "Anime"
    
    for game in game_partners:
        if game in partner_lower:
            return "Game"
    
    for movie in movie_partners:
        if movie in partner_lower:
            return "Movie"
    
    for artist in artist_partners:
        if artist in partner_lower:
            return "Artist"
    
    for brand in brand_partners:
        if brand in partner_lower:
            return "Brand"
    
    # Check text for category hints
    if any(kw in text for kw in ['anime', 'manga', 'アニメ', '漫画']):
        return "Anime"
    if any(kw in text for kw in ['movie', 'film', 'series', 'tv show', '영화', '드라마']):
        return "Movie"
    if any(kw in text for kw in ['artist', 'singer', 'band', 'music', 'concert']):
        return "Artist"
    
    return "IP"


def _guess_region(text: str) -> str:
    """Guess the region from text content."""
    text_lower = text.lower()
    
    region_keywords = {
        "KR": ['korea', 'korean', '한국', '한글', 'kr server'],
        "JP": ['japan', 'japanese', '日本', '日本語', 'jp server'],
        "NA": ['north america', 'usa', 'us server', 'na server'],
        "EU": ['europe', 'european', 'eu server'],
        "SEA": ['southeast asia', 'sea server', 'indonesia', 'thailand', 'vietnam', 'philippines'],
        "LATAM": ['latin america', 'latam', 'brazil', 'mexico', 'spanish'],
        "MENA": ['middle east', 'mena', 'arabic', 'arab']
    }
    
    for region, keywords in region_keywords.items():
        if any(kw in text_lower for kw in keywords):
            return region
    
    # Default to Global for most content
    return "Global"


def classify_collabs(
    db: Database,
    use_gpt: bool = True,
    gpt_threshold: float = 0.5,
    reclassify_all: bool = False,
    source_channel: Optional[str] = None,
    progress_callback: Optional[Callable[[str], None]] = None
) -> dict:
    """
    Classify videos as collab or non-collab.
    
    Args:
        db: Database instance
        use_gpt: Whether to use GPT for ambiguous cases
        gpt_threshold: Confidence threshold below which to use GPT
        reclassify_all: Reclassify all videos, not just unclassified
        source_channel: Filter by source channel ('pubgm' or 'freefire')
        progress_callback: Progress callback
    
    Returns:
        Classification statistics
    """
    config = get_config()
    
    stats = {
        "total_processed": 0,
        "rule_classified": 0,
        "gpt_classified": 0,
        "collabs_found": 0,
        "non_collabs": 0,
        "errors": []
    }
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Get videos to classify
    if reclassify_all:
        videos = db.get_all_videos(source_channel=source_channel)
        log(f"Reclassifying all {len(videos)} videos...")
    else:
        videos = db.get_unclassified_videos()
        # Filter by source_channel if specified
        if source_channel:
            videos = [v for v in videos if v.source_channel == source_channel]
        log(f"Classifying {len(videos)} unclassified videos...")
    
    if not videos:
        log("No videos to classify.")
        return stats
    
    # Initialize GPT client if needed
    gpt_client = None
    if use_gpt:
        try:
            gpt_client = GPTClient()
        except ValueError as e:
            log(f"GPT client not available: {e}")
            use_gpt = False
    
    # Process each video
    gpt_queue = []  # Videos needing GPT classification
    
    for video in tqdm(videos, desc="Rule-based classification"):
        # Try rule-based classification first
        result = _rule_based_classify(
            video.title,
            video.description or "",
            config
        )
        
        if result and result.confidence >= gpt_threshold:
            # Confident rule-based classification
            video.is_collab = result.is_collab
            video.collab_partner = result.partner_name
            video.collab_category = result.category
            video.collab_region = result.region
            video.collab_summary = result.one_line_summary
            video.collab_confidence = result.confidence
            video.classification_method = "rule"
            
            db.upsert_video(video)
            stats["rule_classified"] += 1
            
            if result.is_collab:
                stats["collabs_found"] += 1
            else:
                stats["non_collabs"] += 1
        else:
            # Need GPT classification
            if result and result.is_collab:
                # Low confidence collab - add to GPT queue
                gpt_queue.append(video)
            else:
                # No collab indicators - mark as non-collab
                video.is_collab = False
                video.collab_confidence = 0.9
                video.classification_method = "rule"
                db.upsert_video(video)
                stats["rule_classified"] += 1
                stats["non_collabs"] += 1
        
        stats["total_processed"] += 1
    
    # Process GPT queue
    if gpt_queue and use_gpt and gpt_client:
        log(f"Using GPT for {len(gpt_queue)} ambiguous videos...")
        
        for video in tqdm(gpt_queue, desc="GPT classification"):
            try:
                result = gpt_client.classify_collab(
                    title=video.title,
                    description=video.description or "",
                    db=db
                )
                
                video.is_collab = result.is_collab
                video.collab_partner = result.partner_name
                video.collab_category = result.category
                video.collab_region = result.region
                video.collab_summary = result.one_line_summary
                video.collab_confidence = result.confidence
                video.classification_method = "gpt"
                
                db.upsert_video(video)
                stats["gpt_classified"] += 1
                
                if result.is_collab:
                    stats["collabs_found"] += 1
                else:
                    stats["non_collabs"] += 1
                    
            except Exception as e:
                stats["errors"].append(f"{video.video_id}: {str(e)}")
                # Fall back to rule-based result
                video.is_collab = True
                video.collab_confidence = 0.3
                video.classification_method = "rule_fallback"
                db.upsert_video(video)
                stats["collabs_found"] += 1
    elif gpt_queue:
        # GPT not available, use rule-based results
        log(f"GPT not available. Using low-confidence rule results for {len(gpt_queue)} videos.")
        for video in gpt_queue:
            video.is_collab = True
            video.collab_confidence = 0.3
            video.classification_method = "rule_low_conf"
            db.upsert_video(video)
            stats["rule_classified"] += 1
            stats["collabs_found"] += 1
    
    log(f"Classification complete: {stats['collabs_found']} collabs, {stats['non_collabs']} non-collabs")
    log(f"Methods: {stats['rule_classified']} rule-based, {stats['gpt_classified']} GPT")
    
    return stats


def normalize_partners(
    db: Database,
    use_gpt: bool = False,
    progress_callback: Optional[Callable[[str], None]] = None
) -> dict:
    """
    Normalize partner names across all videos.
    Merges variations like "BLACKPINK" and "Black Pink".
    
    Args:
        db: Database instance
        use_gpt: Use GPT for advanced normalization
        progress_callback: Progress callback
    
    Returns:
        Normalization statistics
    """
    config = get_config()
    
    stats = {
        "partners_normalized": 0,
        "videos_updated": 0
    }
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Get all collab videos
    videos = db.get_collab_videos()
    
    # Get unique partner names
    partners = set(v.collab_partner for v in videos if v.collab_partner)
    log(f"Found {len(partners)} unique partner names")
    
    # Build normalization map
    normalization_map = {}
    for partner in partners:
        normalized = partner.lower().strip()
        
        # Check against known aliases
        for alias, canonical in config.partner_aliases.items():
            if alias in normalized or normalized in alias:
                normalization_map[partner] = canonical
                break
    
    if not normalization_map:
        log("No normalizations needed.")
        return stats
    
    log(f"Normalizing {len(normalization_map)} partner name variations...")
    
    # Update videos
    for video in videos:
        if video.collab_partner in normalization_map:
            video.collab_partner = normalization_map[video.collab_partner]
            db.upsert_video(video)
            stats["videos_updated"] += 1
    
    stats["partners_normalized"] = len(normalization_map)
    log(f"Normalized {stats['partners_normalized']} partner names, updated {stats['videos_updated']} videos")
    
    return stats
