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
        
        # Setup paths
        self.base_dir = Path.cwd()
        self.data_dir = self.base_dir / 'data' / platform
        self.env_path = self.base_dir / '.env'
        
        # Create data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Load environment variables
        self.logger.info(f"Loading environment from: {self.env_path}")
        load_dotenv(self.env_path)
        
        # Load base configurations
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        if not self.access_token:
            raise ValueError(f"FACEBOOK_ACCESS_TOKEN not found in {self.env_path}")
            
        self.base_url = "https://graph.facebook.com/v18.0"

    def setup_logging(self):
        """Setup logging configuration"""
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
        """Make an API request with error handling and logging"""
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
        """Save data with enhanced organization and error handling"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        save_dir = self.data_dir / timestamp
        save_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Save raw JSON
            with open(save_dir / f'{self.platform}_raw_data.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Process insights data if available
            if 'insights_data' in data and 'data' in data['insights_data']:
                self._save_insights_data(data['insights_data'], save_dir)
            
            # Call platform-specific save methods
            self._save_platform_specific_data(data, save_dir)
            
            self.logger.info(f"Data saved successfully in {save_dir}")
            
        except Exception as e:
            self.logger.error(f"Error saving data: {str(e)}")
            raise

    def _save_insights_data(self, insights_data: dict, save_dir: Path):
        """Save insights data to CSV files"""
        insights_list = []
        for metric in insights_data['data']:
            for value in metric['values']:
                insights_list.append({
                    'metric': metric['name'],
                    'date': value['end_time'],
                    'value': value['value']
                })
        
        if insights_list:
            insights_df = pd.DataFrame(insights_list)
            
            # Save detailed insights
            insights_df.to_csv(
                save_dir / f'{self.platform}_daily_insights.csv', 
                index=False, 
                encoding='utf-8'
            )
            
            # Save pivot table
            insights_df['date'] = pd.to_datetime(insights_df['date'])
            pivot_df = insights_df.pivot(
                index='date', 
                columns='metric', 
                values='value'
            )
            pivot_df.to_csv(
                save_dir / f'{self.platform}_daily_insights_pivot.csv',
                encoding='utf-8'
            )

    @abstractmethod
    def _save_platform_specific_data(self, data: dict, save_dir: Path):
        """Save platform-specific data - to be implemented by child classes"""
        pass

    @abstractmethod
    def get_insights(self, days_back: int = 30) -> dict:
        """Get platform-specific insights - to be implemented by child classes"""
        pass

class FacebookAnalytics(SocialMediaAnalytics):
    def __init__(self):
        super().__init__('facebook')
        self.page_id = os.getenv('PAGE_ID')
        if not self.page_id:
            raise ValueError(f"PAGE_ID not found in {self.env_path}")
        self.logger.info(f"Initialized Facebook Analytics for page ID: {self.page_id}")

    def get_insights(self, days_back: int = 30) -> dict:
        """Get Facebook Page insights"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Get page details
        page_data = self.make_api_request(
            f"{self.base_url}/{self.page_id}",
            {
                'access_token': self.access_token,
                'fields': 'name,fan_count,followers_count,link,about,category,engagement'
            },
            "fetch page details"
        )
        
        # Get page insights
        metrics = [
            'page_impressions',
            'page_engaged_users',
            'page_post_engagements',
            'page_views_total',
            'page_actions_post_reactions_total',
            'page_fan_adds',
            'page_fan_removes',
            'page_negative_feedback',
            'page_posts_impressions',
            'page_video_views'
        ]
        
        insights_data = self.make_api_request(
            f"{self.base_url}/{self.page_id}/insights",
            {
                'access_token': self.access_token,
                'metric': ','.join(metrics),
                'period': 'day',
                'since': int(start_date.timestamp()),
                'until': int(end_date.timestamp())
            },
            "fetch page insights"
        )
        
        # Get recent posts
        posts_data = self.make_api_request(
            f"{self.base_url}/{self.page_id}/posts",
            {
                'access_token': self.access_token,
                'fields': 'message,created_time,likes.summary(true),comments.summary(true),'
                         'shares,reactions.summary(true),insights.metric(post_impressions,'
                         'post_engaged_users,post_negative_feedback),'
                         'attachments{media_type,title,description,url}',
                'limit': 100
            },
            "fetch recent posts"
        )
        
        return {
            'page_data': page_data,
            'insights_data': insights_data,
            'posts_data': posts_data
        }

    def _save_platform_specific_data(self, data: dict, save_dir: Path):
        """Save Facebook-specific data"""
        # Save page data
        if 'page_data' in data:
            page_df = pd.json_normalize(data['page_data'])
            page_df.to_csv(save_dir / 'facebook_page_data.csv', 
                          index=False, encoding='utf-8')
        
        # Save posts data
        if 'posts_data' in data and 'data' in data['posts_data']:
            posts_df = pd.json_normalize(
                data['posts_data']['data'],
                sep='_'
            )
            posts_df.to_csv(save_dir / 'facebook_posts_data.csv', 
                           index=False, encoding='utf-8')

class InstagramAnalytics(SocialMediaAnalytics):
    def __init__(self):
        super().__init__('instagram')
        self.instagram_account_id = os.getenv('INSTAGRAM_BUSINESS_ACCOUNT_ID')
        if not self.instagram_account_id:
            raise ValueError(f"INSTAGRAM_BUSINESS_ACCOUNT_ID not found in {self.env_path}")
        self.logger.info(f"Initialized Instagram Analytics for account ID: {self.instagram_account_id}")

    def get_insights(self, days_back: int = 30) -> dict:
        """Get Instagram Business Account insights"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Get account details
        account_data = self.make_api_request(
            f"{self.base_url}/{self.instagram_account_id}",
            {
                'access_token': self.access_token,
                'fields': 'username,followers_count,follows_count,media_count,profile_picture_url'
            },
            "fetch account details"
        )
        
        # Get account insights
        metrics = [
            'impressions',
            'reach',
            'profile_views',
            'website_clicks',
            'email_contacts',
            'get_directions_clicks',
            'phone_call_clicks',
            'text_message_clicks'
        ]
        
        insights_data = self.make_api_request(
            f"{self.base_url}/{self.instagram_account_id}/insights",
            {
                'access_token': self.access_token,
                'metric': ','.join(metrics),
                'period': 'day',
                'since': int(start_date.timestamp()),
                'until': int(end_date.timestamp())
            },
            "fetch account insights"
        )
        
        # Get recent media
        media_data = self.make_api_request(
            f"{self.base_url}/{self.instagram_account_id}/media",
            {
                'access_token': self.access_token,
                'fields': 'id,caption,media_type,media_url,permalink,thumbnail_url,'
                         'timestamp,comments_count,like_count,'
                         'insights.metric(engagement,impressions,reach,saved)',
                'limit': 100
            },
            "fetch recent media"
        )
        
        return {
            'account_data': account_data,
            'insights_data': insights_data,
            'media_data': media_data
        }

    def _save_platform_specific_data(self, data: dict, save_dir: Path):
        """Save Instagram-specific data"""
        # Save account data
        if 'account_data' in data:
            account_df = pd.DataFrame([data['account_data']])
            account_df.to_csv(save_dir / 'instagram_account_data.csv', 
                            index=False, encoding='utf-8')
        
        # Save media data
        if 'media_data' in data and 'data' in data['media_data']:
            media_df = pd.json_normalize(
                data['media_data']['data'],
                sep='_'
            )
            media_df.to_csv(save_dir / 'instagram_media_data.csv', 
                           index=False, encoding='utf-8')

def main():
    try:
        # Facebook Analytics
        fb_analyzer = FacebookAnalytics()
        fb_data = fb_analyzer.get_insights(days_back=30)
        fb_analyzer.save_data(fb_data)
        print("Facebook data extraction completed successfully")
        
        # Instagram Analytics
        ig_analyzer = InstagramAnalytics()
        ig_data = ig_analyzer.get_insights(days_back=30)
        ig_analyzer.save_data(ig_data)
        print("Instagram data extraction completed successfully")
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
