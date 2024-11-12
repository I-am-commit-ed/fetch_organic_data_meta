import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import pandas as pd
from datetime import datetime
import json

class FacebookAnalytics:
    def __init__(self):
        # Setup paths
        self.base_dir = Path('/Users/manuel/Documents/GitHub/JeanPierreWeill/DataExtract')
        self.data_dir = self.base_dir / 'data' / 'facebook'
        self.env_path = self.base_dir / '.env'
        
        # Create data directory
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Load environment variables
        print(f"Loading environment from: {self.env_path}")
        load_dotenv(self.env_path)
        
        # Get credentials from environment variables
        self.access_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
        self.page_id = os.getenv('PAGE_ID')
        
        if not self.access_token:
            raise ValueError(f"FACEBOOK_ACCESS_TOKEN not found in {self.env_path}")
        if not self.page_id:
            raise ValueError(f"PAGE_ID not found in {self.env_path}")
            
        self.base_url = "https://graph.facebook.com/v18.0"
        print(f"Initialized Facebook Analytics for page ID: {self.page_id}")

    def get_page_insights(self) -> dict:
        """Get Facebook Page insights"""
        print(f"Fetching insights for Facebook Page ID: {self.page_id}")
        
        # Get page details
        page_endpoint = f"{self.base_url}/{self.page_id}"
        page_params = {
            'access_token': self.access_token,
            'fields': 'name,fan_count,followers_count,link,about,category'
        }
        page_response = requests.get(page_endpoint, params=page_params)
        page_response.raise_for_status()  # Raise exception for bad status codes
        page_data = page_response.json()
        
        # Get page insights
        insights_endpoint = f"{self.base_url}/{self.page_id}/insights"
        metrics = [
            'page_impressions',
            'page_engaged_users',
            'page_post_engagements',
            'page_views_total',
            'page_actions_post_reactions_total'
        ]
        
        insights_params = {
            'access_token': self.access_token,
            'metric': ','.join(metrics),
            'period': 'day'
        }
        insights_response = requests.get(insights_endpoint, params=insights_params)
        insights_response.raise_for_status()
        insights_data = insights_response.json()
        
        # Get recent posts
        posts_endpoint = f"{self.base_url}/{self.page_id}/posts"
        posts_params = {
            'access_token': self.access_token,
            'fields': 'message,created_time,likes.summary(true),comments.summary(true),shares',
            'limit': 100
        }
        posts_response = requests.get(posts_endpoint, params=posts_params)
        posts_response.raise_for_status()
        posts_data = posts_response.json()
        
        return {
            'page_data': page_data,
            'insights_data': insights_data,
            'posts_data': posts_data
        }

    def save_data(self, data: dict):
        """Save Facebook data in both JSON and CSV formats"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        save_dir = self.data_dir / timestamp
        save_dir.mkdir(parents=True, exist_ok=True)
        
        # Save raw JSON
        with open(save_dir / 'facebook_raw_data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Process and save page data
        if 'page_data' in data:
            page_df = pd.json_normalize(data['page_data'])
            page_df.to_csv(save_dir / 'facebook_page_data.csv', index=False)
        
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
                # Save detailed insights
                insights_df = pd.DataFrame(insights_list)
                insights_df.to_csv(save_dir / 'facebook_daily_insights.csv', index=False)
                
                # Save pivot table
                pivot_df = insights_df.pivot(index='date', columns='metric', values='value')
                pivot_df.to_csv(save_dir / 'facebook_daily_insights_pivot.csv')
        
        # Process and save posts data
        if 'posts_data' in data and 'data' in data['posts_data']:
            posts_df = pd.json_normalize(data['posts_data']['data'])
            posts_df.to_csv(save_dir / 'facebook_posts_data.csv', index=False)
        
        print(f"Data saved in {save_dir}")

def main():
    try:
        analyzer = FacebookAnalytics()
        
        # Get Facebook data
        data = analyzer.get_page_insights()
        
        # Save data
        analyzer.save_data(data)
        
        print("Facebook data extraction completed successfully")
        
    except requests.exceptions.RequestException as e:
        print(f"API Error: {str(e)}")
        response = getattr(e, 'response', None)
        if response:
            print(f"API Response: {response.text}")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
