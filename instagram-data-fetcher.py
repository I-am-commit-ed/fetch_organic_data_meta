# facebook_data_fetcher.py
import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import logging

class FacebookAnalytics:
    def __init__(self):
        # Setup logging
        self.setup_logging()
        
        # Setup paths
        self.base_dir = Path.cwd()  # Use current working directory
        self.data_dir = self.base_dir / 'data' / 'facebook'
        self.env_path = self.base_dir / '.env'
        
        # Create data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Load environment variables
        self.logger.info(f"Loading environment from: {self.env_path}")
        load_dotenv(self.env_path)
        
        # Get credentials from environment variables
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        self.page_id = os.getenv('PAGE_ID')
        
        if not self.access_token:
            raise ValueError(f"FACEBOOK_ACCESS_TOKEN not found in {self.env_path}")
        if not self.page_id:
            raise ValueError(f"PAGE_ID not found in {self.env_path}")
            
        self.base_url = "https://graph.facebook.com/v18.0"
        self.logger.info(f"Initialized Facebook Analytics for page ID: {self.page_id}")

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('facebook_analytics.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('FacebookAnalytics')

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

    def get_page_insights(self, days_back=30) -> dict:
        """Get Facebook Page insights with specified date range"""
        self.logger.info(f"Fetching insights for Facebook Page ID: {self.page_id}")
        
        # Calculate date range
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
        
        # Get page insights with more metrics
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
        
        # Get recent posts with enhanced fields
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

    def save_data(self, data: dict):
        """Save Facebook data with enhanced organization and error handling"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        save_dir = self.data_dir / timestamp
        save_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Save raw JSON with proper encoding
            with open(save_dir / 'facebook_raw_data.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Process and save page data with error handling
            if 'page_data' in data:
                page_df = pd.json_normalize(data['page_data'])
                page_df.to_csv(save_dir / 'facebook_page_data.csv', index=False, encoding='utf-8')
            
            # Process and save insights data
            if 'insights_data' in data and 'data' in data['insights_data']:
                insights_list = []
                for metric in data['insights_data']['data']:
                    for value in metric['values']:
                        insights_list.append({
                            'metric': metric['name'],
                            'date': value['end_time'],
                            'value': value['value']
                        })
                
                if insights_list:
                    insights_df = pd.DataFrame(insights_list)
                    
                    # Save detailed insights
                    insights_df.to_csv(save_dir / 'facebook_daily_insights.csv', 
                                     index=False, encoding='utf-8')
                    
                    # Save pivot table with better date handling
                    insights_df['date'] = pd.to_datetime(insights_df['date'])
                    pivot_df = insights_df.pivot(index='date', 
                                               columns='metric', 
                                               values='value')
                    pivot_df.to_csv(save_dir / 'facebook_daily_insights_pivot.csv',
                                  encoding='utf-8')
            
            # Process and save posts data with enhanced error handling
            if 'posts_data' in data and 'data' in data['posts_data']:
                posts_df = pd.json_normalize(
                    data['posts_data']['data'],
                    sep='_'
                )
                posts_df.to_csv(save_dir / 'facebook_posts_data.csv', 
                               index=False, encoding='utf-8')
            
            self.logger.info(f"Data saved successfully in {save_dir}")
            
        except Exception as e:
            self.logger.error(f"Error saving data: {str(e)}")
            raise

def main():
    try:
        analyzer = FacebookAnalytics()
        data = analyzer.get_page_insights(days_back=30)  # Get last 30 days of data
        analyzer.save_data(data)
        print("Facebook data extraction completed successfully")
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()

# instagram_data_fetcher.py
import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import logging

class InstagramAnalytics:
    def __init__(self):
        # Setup logging
        self.setup_logging()
        
        # Setup paths
        self.base_dir = Path.cwd()
        self.data_dir = self.base_dir / 'data' / 'instagram'
        self.env_path = self.base_dir / '.env'
        
        # Create data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Load environment variables
        self.logger.info(f"Loading environment from: {self.env_path}")
        load_dotenv(self.env_path)
        
        # Get credentials from environment variables
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        self.instagram_account_id = os.getenv('INSTAGRAM_BUSINESS_ACCOUNT_ID')
        
        if not self.access_token:
            raise ValueError(f"FACEBOOK_ACCESS_TOKEN not found in {self.env_path}")
        if not self.instagram_account_id:
            raise ValueError(f"INSTAGRAM_BUSINESS_ACCOUNT_ID not found in {self.env_path}")
            
        self.base_url = "https://graph.facebook.com/v18.0"
        self.logger.info(f"Initialized Instagram Analytics for account ID: {self.instagram_account_id}")

    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('instagram_analytics.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger('InstagramAnalytics')

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

    def get_instagram_insights(self, days_back=30) -> dict:
        """Get Instagram Business Account insights"""
        self.logger.info(f"Fetching insights for Instagram Account ID: {self.instagram_account_id}")
        
        # Calculate date range
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
        
        # Get recent media with insights
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

    def save_data(self, data: dict):
        """Save Instagram data with enhanced organization and error handling"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        save_dir = self.data_dir / timestamp
        save_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Save raw JSON
            with open(save_dir / 'instagram_raw_data.json', 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            # Process and save account data
            if 'account_data' in data:
                account_df = pd.DataFrame([data['account_data']])
                account_df.to_csv(save_dir / 'instagram_account_data.csv', 
                                index=False, encoding='utf-8')
            
            # Process and save insights data
            if 'insights_data' in data and 'data' in data['insights_data']:
                insights_list = []
                for metric in data['insights_data']['data']:
                    for value in metric['values']:
                        insights_list.append({
                            'metric': metric['name'],
                            'date': value['end_time'],
                            'value': value['value']
                        })
                
                if insights_list:
                    insights_df = pd.DataFrame(insights_list)
                    
                    # Save detailed insights
                    insights_df.to_csv(save_dir / 'instagram_daily_insights.csv', 
                                     index=False, encoding='utf-8')
                    
                    # Save pivot table
                    insights_df['date'] = pd.to_datetime(insights_df['date'])
                    pivot_df = insights_df.pivot(index='date', 
                                               columns='metric', 
                                               values='value')
                    pivot_df.to_csv(save_dir / 'instagram_daily_insights_pivot.csv',
                                  encoding='utf-8')
            
            # Process and save media data
            if 'media_data' in data and 'data' in data['media_data']:
                media_df = pd.json_normalize(
                    data['media_data']['data'],
                    sep='_'
                )
                media_df.to_csv(save_dir / 'instagram_media_data.csv', 
                               index=False, encoding='utf-8')
            
            self.logger.info(f"Data saved successfully in {save_dir}")
            
        except Exception as e:
            self.logger.error(f"Error saving data: {str(e)}")
            raise

def main():
    try:
        analyzer = InstagramAnalytics()
        data = analyzer.get_instagram_insights(days_back=30)  # Get last 30 days of data
        analyzer.save_data(data)  # Fixed: Added missing parameter and parentheses
        print("Instagram data extraction completed successfully")
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()