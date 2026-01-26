"""
Sentiment analysis pipeline using VADER.
Fast, rule-based sentiment analysis for English social media text.
"""

from typing import Optional, Callable
from collections import defaultdict
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from tqdm import tqdm
import pandas as pd

from db.models import Database, Video


class SentimentAnalyzer:
    """VADER-based sentiment analyzer for YouTube comments."""
    
    def __init__(self):
        self.analyzer = SentimentIntensityAnalyzer()
    
    def analyze_text(self, text: str) -> dict:
        """
        Analyze sentiment of a single text.
        
        Returns:
            dict with 'neg', 'neu', 'pos', 'compound' scores
            compound: -1 (most negative) to +1 (most positive)
        """
        if not text:
            return {'neg': 0, 'neu': 1, 'pos': 0, 'compound': 0}
        return self.analyzer.polarity_scores(text)
    
    def classify_sentiment(self, compound: float) -> str:
        """
        Classify compound score into category.
        
        Args:
            compound: VADER compound score (-1 to +1)
        
        Returns:
            'positive', 'negative', or 'neutral'
        """
        if compound >= 0.05:
            return 'positive'
        elif compound <= -0.05:
            return 'negative'
        else:
            return 'neutral'
    
    def analyze_comments(self, comments: list[str]) -> dict:
        """
        Analyze sentiment of multiple comments.
        
        Args:
            comments: List of comment texts
        
        Returns:
            dict with aggregated sentiment stats
        """
        if not comments:
            return {
                'total': 0,
                'positive': 0,
                'negative': 0,
                'neutral': 0,
                'positive_ratio': 0,
                'negative_ratio': 0,
                'neutral_ratio': 0,
                'avg_compound': 0
            }
        
        sentiments = {'positive': 0, 'negative': 0, 'neutral': 0}
        compound_sum = 0
        
        for comment in comments:
            scores = self.analyze_text(comment)
            sentiment = self.classify_sentiment(scores['compound'])
            sentiments[sentiment] += 1
            compound_sum += scores['compound']
        
        total = len(comments)
        return {
            'total': total,
            'positive': sentiments['positive'],
            'negative': sentiments['negative'],
            'neutral': sentiments['neutral'],
            'positive_ratio': round(sentiments['positive'] / total * 100, 2),
            'negative_ratio': round(sentiments['negative'] / total * 100, 2),
            'neutral_ratio': round(sentiments['neutral'] / total * 100, 2),
            'avg_compound': round(compound_sum / total, 4)
        }


def analyze_video_sentiments(
    db: Database,
    only_collab: bool = True,
    progress_callback: Optional[Callable[[str], None]] = None
) -> list[dict]:
    """
    Analyze sentiment for all videos with comments.
    
    Args:
        db: Database instance
        only_collab: Only analyze collab videos
        progress_callback: Progress callback
    
    Returns:
        List of video sentiment results
    """
    analyzer = SentimentAnalyzer()
    
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Get videos
    if only_collab:
        videos = db.get_collab_videos()
        log(f"Analyzing sentiment for {len(videos)} collab videos...")
    else:
        videos = db.get_all_videos()
        log(f"Analyzing sentiment for {len(videos)} videos...")
    
    results = []
    
    for video in tqdm(videos, desc="Sentiment analysis"):
        comments = db.get_comments_for_video(video.video_id)
        
        if not comments:
            continue
        
        comment_texts = [c.text_original for c in comments if c.text_original]
        
        if not comment_texts:
            continue
        
        sentiment = analyzer.analyze_comments(comment_texts)
        
        results.append({
            'video_id': video.video_id,
            'title': video.title,
            'collab_partner': video.collab_partner,
            'collab_category': video.collab_category,
            'view_count': video.view_count,
            'comment_count': video.comment_count,
            'analyzed_comments': sentiment['total'],
            'positive': sentiment['positive'],
            'negative': sentiment['negative'],
            'neutral': sentiment['neutral'],
            'positive_ratio': sentiment['positive_ratio'],
            'negative_ratio': sentiment['negative_ratio'],
            'neutral_ratio': sentiment['neutral_ratio'],
            'avg_compound': sentiment['avg_compound'],
            'sentiment_label': 'positive' if sentiment['avg_compound'] >= 0.05 
                              else 'negative' if sentiment['avg_compound'] <= -0.05 
                              else 'neutral'
        })
    
    log(f"Analyzed {len(results)} videos with comments")
    return results


def aggregate_partner_sentiment(results: list[dict]) -> list[dict]:
    """
    Aggregate sentiment by collab partner.
    
    Args:
        results: List of video sentiment results
    
    Returns:
        List of partner sentiment aggregations
    """
    partner_data = defaultdict(lambda: {
        'videos': 0,
        'total_comments': 0,
        'positive': 0,
        'negative': 0,
        'neutral': 0,
        'compound_sum': 0,
        'category': None
    })
    
    for r in results:
        partner = r.get('collab_partner') or 'Unknown'
        data = partner_data[partner]
        data['videos'] += 1
        data['total_comments'] += r['analyzed_comments']
        data['positive'] += r['positive']
        data['negative'] += r['negative']
        data['neutral'] += r['neutral']
        data['compound_sum'] += r['avg_compound'] * r['analyzed_comments']
        if r.get('collab_category'):
            data['category'] = r['collab_category']
    
    aggregated = []
    for partner, data in partner_data.items():
        total = data['total_comments']
        if total == 0:
            continue
            
        avg_compound = data['compound_sum'] / total
        
        aggregated.append({
            'partner_name': partner,
            'category': data['category'],
            'video_count': data['videos'],
            'total_comments': total,
            'positive': data['positive'],
            'negative': data['negative'],
            'neutral': data['neutral'],
            'positive_ratio': round(data['positive'] / total * 100, 2),
            'negative_ratio': round(data['negative'] / total * 100, 2),
            'neutral_ratio': round(data['neutral'] / total * 100, 2),
            'avg_compound': round(avg_compound, 4),
            'overall_sentiment': 'positive' if avg_compound >= 0.05 
                                else 'negative' if avg_compound <= -0.05 
                                else 'neutral'
        })
    
    # Sort by total comments
    aggregated.sort(key=lambda x: x['total_comments'], reverse=True)
    return aggregated


def export_sentiment_csv(
    db: Database,
    output_path: str,
    only_collab: bool = True,
    progress_callback: Optional[Callable[[str], None]] = None
) -> tuple[str, str]:
    """
    Export sentiment analysis to CSV files.
    
    Args:
        db: Database instance
        output_path: Base output path (without extension)
        only_collab: Only analyze collab videos
        progress_callback: Progress callback
    
    Returns:
        Tuple of (video_csv_path, partner_csv_path)
    """
    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    # Analyze videos
    results = analyze_video_sentiments(db, only_collab, progress_callback)
    
    if not results:
        log("No videos with comments to analyze.")
        return None, None
    
    # Export video-level sentiment
    video_df = pd.DataFrame(results)
    video_path = f"{output_path}_videos.csv"
    video_df.to_csv(video_path, index=False, encoding='utf-8-sig')
    log(f"Exported video sentiment to {video_path}")
    
    # Export partner-level sentiment
    partner_results = aggregate_partner_sentiment(results)
    partner_df = pd.DataFrame(partner_results)
    partner_path = f"{output_path}_partners.csv"
    partner_df.to_csv(partner_path, index=False, encoding='utf-8-sig')
    log(f"Exported partner sentiment to {partner_path}")
    
    # Print summary
    log("\n=== Sentiment Summary ===")
    total_comments = sum(r['analyzed_comments'] for r in results)
    total_positive = sum(r['positive'] for r in results)
    total_negative = sum(r['negative'] for r in results)
    total_neutral = sum(r['neutral'] for r in results)
    
    log(f"Total comments analyzed: {total_comments:,}")
    log(f"Positive: {total_positive:,} ({total_positive/total_comments*100:.1f}%)")
    log(f"Neutral: {total_neutral:,} ({total_neutral/total_comments*100:.1f}%)")
    log(f"Negative: {total_negative:,} ({total_negative/total_comments*100:.1f}%)")
    
    return video_path, partner_path
