"""
Free Fire YouTube Channel API Test
"""
import os
import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from dotenv import load_dotenv
load_dotenv()

import requests

API_KEY = os.getenv('YOUTUBE_API_KEY')
FREEFIRE_HANDLE = '@GarenaFreeFireGlobal'

def test_channel_by_handle():
    """Test getting channel info by handle"""
    print("=" * 60)
    print("1. Free Fire Channel Info Test")
    print("=" * 60)
    
    # Get channel by handle
    url = f'https://www.googleapis.com/youtube/v3/channels'
    params = {
        'part': 'snippet,contentDetails,statistics',
        'forHandle': 'GarenaFreeFireGlobal',
        'key': API_KEY
    }
    
    response = requests.get(url, params=params)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        if data.get('items'):
            channel = data['items'][0]
            print(f"\nChannel ID: {channel['id']}")
            print(f"Title: {channel['snippet']['title']}")
            print(f"Description: {channel['snippet']['description'][:100]}...")
            print(f"Subscribers: {channel['statistics'].get('subscriberCount', 'Hidden')}")
            print(f"Total Videos: {channel['statistics']['videoCount']}")
            print(f"Total Views: {channel['statistics']['viewCount']}")
            print(f"Uploads Playlist: {channel['contentDetails']['relatedPlaylists']['uploads']}")
            return channel
        else:
            print("No channel found")
    else:
        print(f"Error: {response.text}")
    
    return None

def test_playlist_videos(uploads_playlist_id):
    """Test getting videos from uploads playlist"""
    print("\n" + "=" * 60)
    print("2. Playlist Videos Test (First 5)")
    print("=" * 60)
    
    url = 'https://www.googleapis.com/youtube/v3/playlistItems'
    params = {
        'part': 'snippet,contentDetails',
        'playlistId': uploads_playlist_id,
        'maxResults': 5,
        'key': API_KEY
    }
    
    response = requests.get(url, params=params)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Total Results: {data.get('pageInfo', {}).get('totalResults', 'Unknown')}")
        
        if data.get('items'):
            print("\nRecent Videos:")
            for i, item in enumerate(data['items'], 1):
                snippet = item['snippet']
                print(f"{i}. {snippet['title'][:60]}...")
                print(f"   Published: {snippet['publishedAt'][:10]}")
                print(f"   Video ID: {item['contentDetails']['videoId']}")
            return [item['contentDetails']['videoId'] for item in data['items']]
    else:
        print(f"Error: {response.text}")
    
    return []

def test_video_details(video_ids):
    """Test getting video details"""
    print("\n" + "=" * 60)
    print("3. Video Details Test")
    print("=" * 60)
    
    url = 'https://www.googleapis.com/youtube/v3/videos'
    params = {
        'part': 'snippet,statistics',
        'id': ','.join(video_ids[:3]),
        'key': API_KEY
    }
    
    response = requests.get(url, params=params)
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        if data.get('items'):
            print("\nVideo Stats:")
            for item in data['items']:
                stats = item['statistics']
                print(f"- {item['snippet']['title'][:50]}...")
                print(f"  Views: {stats.get('viewCount', 0)}")
                print(f"  Likes: {stats.get('likeCount', 0)}")
                print(f"  Comments: {stats.get('commentCount', 0)}")
    else:
        print(f"Error: {response.text}")

if __name__ == '__main__':
    print("Free Fire YouTube API Test")
    print(f"API Key: {API_KEY[:20]}...")
    print()
    
    # 1. Get channel info
    channel = test_channel_by_handle()
    
    if channel:
        # 2. Get playlist videos
        uploads_id = channel['contentDetails']['relatedPlaylists']['uploads']
        video_ids = test_playlist_videos(uploads_id)
        
        # 3. Get video details
        if video_ids:
            test_video_details(video_ids)
    
    print("\n" + "=" * 60)
    print("Test Complete!")
    print("=" * 60)
