import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import logging
from abc import ABC, abstractmethod

class SocialMediaAnalytics(ABC):
    def __init__(self, platform: str):
        self.platform = platform
        self.setup_logging()
        
        self.base_dir = Path('/Users/manuel/Documents/GitHub/fetch_organic_data_meta')
        self.data_dir = self.base_dir / 'data'
        self.env_path = self.base_dir / '.env'
        
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Loading environment from: {self.env_path}")
        load_dotenv(self.env_path)
        
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        if not self.access_token:
            raise ValueError(f"FACEBOOK_ACCESS_TOKEN not found in {self.env_path}")
            
        self.base_url = "https://graph.facebook.com/v18.0"

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{self.platform}_analytics.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(f'{self.platform.capitalize()}Analytics')

    def make_api_request(self, endpoint: str, params: dict, description: str) -> dict:
        self.logger.info(f"Making API request: {description}")
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Error response: {e.response.text}")
            raise

    def save_data(self, data: dict):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        save_dir = self.data_dir / timestamp
        save_dir.mkdir(parents=True, exist_ok=True)
        
        # Save raw JSON
        with open(save_dir / f'{self.platform}_raw_data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Save posts data
        if 'posts_data' in data and 'data' in data['posts_data']:
            self._save_posts_data(data['posts_data'], save_dir)
        
        self.logger.info(f"Data saved in {save_dir}")

    def _save_posts_data(self, posts_data: dict, save_dir: Path):
        posts_list = []
        for post in posts_data['data']:
            post_data = {
                'id': post.get('id', ''),
                'message': post.get('message', ''),
                'created_time': post.get('created_time', ''),
                'likes_count': post.get('likes', {}).get('summary', {}).get('total_count', 0),
                'comments_count': post.get('comments', {}).get('summary', {}).get('total_count', 0),
                'shares_count': post.get('shares', {}).get('count', 0) if 'shares' in post else 0
            }
            posts_list.append(post_data)
        
        if posts_list:
            posts_df = pd.DataFrame(posts_list)
            posts_df.to_csv(
                save_dir / f'{self.platform}_posts_data.csv',
                index=False,
                encoding='utf-8'
            )

class FacebookAnalytics(SocialMediaAnalytics):
    def __init__(self):
        super().__init__('facebook')
        self.page_id = os.getenv('PAGE_ID')
        if not self.page_id:
            raise ValueError(f"PAGE_ID not found in {self.env_path}")
        self.logger.info(f"Initialized Facebook Analytics for page ID: {self.page_id}")

    def get_insights(self, days_back: int = 30) -> dict:
        """Get Facebook Page posts data with engagement metrics"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Get basic page details
        page_data = self.make_api_request(
            f"{self.base_url}/{self.page_id}",
            {
                'access_token': self.access_token,
                'fields': 'name,fan_count,followers_count'
            },
            "fetch page details"
        )
        
        # Get detailed posts data with engagement metrics
        posts_data = self.make_api_request(
            f"{self.base_url}/{self.page_id}/posts",
            {
                'access_token': self.access_token,
                'fields': 'message,created_time,likes.summary(true),comments.summary(true).filter(stream),shares',
                'limit': 100,  # Maximum posts per request
            },
            "fetch posts data"
        )
        
        # Process pagination to get more posts
        all_posts = posts_data.get('data', [])
        next_page = posts_data.get('paging', {}).get('next')
        
        while next_page and len(all_posts) < 1000:  # Limit to 1000 posts or available
            posts_data = requests.get(next_page).json()
            all_posts.extend(posts_data.get('data', []))
            next_page = posts_data.get('paging', {}).get('next')
        
        return {
            'page_data': page_data,
            'posts_data': {'data': all_posts}
        }

class InstagramAnalytics(SocialMediaAnalytics):
    def __init__(self):
        super().__init__('instagram')
        self.instagram_account_id = os.getenv('INSTAGRAM_BUSINESS_ACCOUNT_ID')
        if not self.instagram_account_id:
            raise ValueError(f"INSTAGRAM_BUSINESS_ACCOUNT_ID not found in {self.env_path}")
        self.logger.info(f"Initialized Instagram Analytics for account ID: {self.instagram_account_id}")

    def get_insights(self, days_back: int = 30) -> dict:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        account_data = self.make_api_request(
            f"{self.base_url}/{self.instagram_account_id}",
            {
                'access_token': self.access_token,
                'fields': 'username,followers_count,follows_count,media_count'
            },
            "fetch account details"
        )
        
        metrics = [
            'reach',
            'impressions',
            'profile_views',
            'follower_count'
        ]
        
        insights_data = self.make_api_request(
            f"{self.base_url}/{self.instagram_account_id}/insights",
            {
                'access_token': self.access_token,
                'metric': ','.join(metrics),
                'period': 'day',
                'since': start_date.strftime('%Y-%m-%d'),
                'until': end_date.strftime('%Y-%m-%d')
            },
            "fetch account insights"
        )
        
        return {
            'account_data': account_data,
            'insights_data': insights_data
        }

def main():
    try:
        # Facebook Analytics
        print("Starting Facebook data extraction...")
        fb_analyzer = FacebookAnalytics()
        fb_data = fb_analyzer.get_insights(days_back=30)
        fb_analyzer.save_data(fb_data)
        print("Facebook data extraction completed")
        
        # Comment out Instagram temporarily while testing Facebook
        # print("\nStarting Instagram data extraction...")
        # ig_analyzer = InstagramAnalytics()
        # ig_data = ig_analyzer.get_insights(days_back=30)
        # ig_analyzer.save_data(ig_data)
        # print("Instagram data extraction completed")
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()