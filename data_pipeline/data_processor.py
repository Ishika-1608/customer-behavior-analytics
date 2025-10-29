import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import json
from typing import Dict, List, Tuple

class CustomerDataProcessor:
    def __init__(self, raw_data_path: str = None, processed_data_path: str = None):
        # Get the project root directory (one level up from data_pipeline)
        self.project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        if raw_data_path is None:
            self.raw_data_path = os.path.join(self.project_root, "data", "raw")
        else:
            self.raw_data_path = raw_data_path
            
        if processed_data_path is None:
            self.processed_data_path = os.path.join(self.project_root, "data", "processed")
        else:
            self.processed_data_path = processed_data_path
            
        os.makedirs(self.processed_data_path, exist_ok=True)
        
    def load_raw_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load raw customer and interaction data"""
        customers_path = os.path.join(self.raw_data_path, "customers.csv")
        interactions_path = os.path.join(self.raw_data_path, "interactions.csv")
        
        print(f"Looking for customers data at: {customers_path}")
        print(f"Looking for interactions data at: {interactions_path}")
        
        if not os.path.exists(customers_path):
            raise FileNotFoundError(f"Customers data not found at {customers_path}")
        if not os.path.exists(interactions_path):
            raise FileNotFoundError(f"Interactions data not found at {interactions_path}")
            
        customers_df = pd.read_csv(customers_path)
        interactions_df = pd.read_csv(interactions_path)
        
        # Convert timestamp to datetime
        interactions_df['timestamp'] = pd.to_datetime(interactions_df['timestamp'])
        customers_df['registration_date'] = pd.to_datetime(customers_df['registration_date'])
        
        return customers_df, interactions_df
    
    
    def create_customer_journey(self, interactions_df: pd.DataFrame) -> pd.DataFrame:
        """Create customer journey sequences"""
        # Sort interactions by customer and timestamp
        interactions_sorted = interactions_df.sort_values(['customer_id', 'timestamp'])
        
        # Create journey sequences
        journey_data = []
        
        for customer_id, group in interactions_sorted.groupby('customer_id'):
            touchpoints = group['touchpoint'].tolist()
            timestamps = group['timestamp'].tolist()
            actions = group['action_taken'].tolist()
            revenues = group['revenue'].tolist()
            
            # Create journey path
            journey_path = ' -> '.join(touchpoints)
            total_interactions = len(touchpoints)
            total_revenue = sum(revenues)
            journey_duration = (timestamps[-1] - timestamps[0]).days if len(timestamps) > 1 else 0
            
            # Calculate touchpoint frequencies
            touchpoint_counts = group['touchpoint'].value_counts().to_dict()
            
            journey_data.append({
                'customer_id': customer_id,
                'journey_path': journey_path,
                'total_interactions': total_interactions,
                'total_revenue': total_revenue,
                'journey_duration_days': journey_duration,
                'first_touchpoint': touchpoints[0],
                'last_touchpoint': touchpoints[-1],
                'unique_touchpoints': len(set(touchpoints)),
                'conversion_occurred': total_revenue > 0,
                'touchpoint_counts': json.dumps(touchpoint_counts)
            })
        
        return pd.DataFrame(journey_data)
    
    def create_touchpoint_analysis(self, interactions_df: pd.DataFrame) -> pd.DataFrame:
        """Analyze touchpoint performance"""
        touchpoint_stats = interactions_df.groupby('touchpoint').agg({
            'interaction_id': 'count',
            'revenue': ['sum', 'mean'],
            'session_duration_minutes': 'mean',
            'customer_id': 'nunique'
        }).round(2)
        
        # Flatten column names
        touchpoint_stats.columns = [
            'total_interactions',
            'total_revenue',
            'avg_revenue_per_interaction',
            'avg_session_duration',
            'unique_customers'
        ]
        
        # Calculate conversion rate
        conversions = interactions_df[interactions_df['revenue'] > 0].groupby('touchpoint')['interaction_id'].count()
        touchpoint_stats['conversion_rate'] = (conversions / touchpoint_stats['total_interactions'] * 100).fillna(0).round(2)
        
        touchpoint_stats = touchpoint_stats.reset_index()
        return touchpoint_stats
    
    def create_customer_segments(self, customers_df: pd.DataFrame, interactions_df: pd.DataFrame) -> pd.DataFrame:
        """Create enhanced customer segments based on behavior"""
        # Aggregate customer behavior
        customer_behavior = interactions_df.groupby('customer_id').agg({
            'interaction_id': 'count',
            'revenue': 'sum',
            'touchpoint': lambda x: len(set(x)),
            'session_duration_minutes': 'mean',
            'timestamp': ['min', 'max']
        }).round(2)
        
        # Flatten column names
        customer_behavior.columns = [
            'total_interactions',
            'total_revenue',
            'unique_touchpoints_used',
            'avg_session_duration',
            'first_interaction',
            'last_interaction'
        ]
        
        # Calculate recency (days since last interaction)
        customer_behavior['recency_days'] = (datetime.now() - customer_behavior['last_interaction']).dt.days
        
        # Create RFM-like segments
        customer_behavior['frequency_score'] = pd.qcut(customer_behavior['total_interactions'], 3, labels=['Low', 'Medium', 'High'])
        customer_behavior['monetary_score'] = pd.qcut(customer_behavior['total_revenue'], 3, labels=['Low', 'Medium', 'High'])
        customer_behavior['recency_score'] = pd.qcut(customer_behavior['recency_days'], 3, labels=['Recent', 'Moderate', 'Distant'])
        
        # Combine with customer data
        customer_behavior = customer_behavior.reset_index()
        enhanced_customers = customers_df.merge(customer_behavior, on='customer_id', how='left')
        
        return enhanced_customers
    
    def create_time_series_data(self, interactions_df: pd.DataFrame) -> pd.DataFrame:
        """Create daily aggregated time series data"""
        interactions_df['date'] = interactions_df['timestamp'].dt.date
        
        daily_stats = interactions_df.groupby('date').agg({
            'interaction_id': 'count',
            'customer_id': 'nunique',
            'revenue': 'sum',
            'session_duration_minutes': 'mean'
        }).round(2)
        
        daily_stats.columns = [
            'daily_interactions',
            'daily_unique_customers',
            'daily_revenue',
            'avg_session_duration'
        ]
        
        # Add touchpoint breakdown
        touchpoint_daily = interactions_df.groupby(['date', 'touchpoint'])['interaction_id'].count().unstack(fill_value=0)
        touchpoint_daily.columns = [f'daily_{col}_interactions' for col in touchpoint_daily.columns]
        
        daily_stats = daily_stats.join(touchpoint_daily)
        daily_stats = daily_stats.reset_index()
        
        return daily_stats
    
    def process_all_data(self) -> Dict[str, pd.DataFrame]:
        """Run the complete data processing pipeline"""
        print("Starting data processing pipeline...")
        
        # Load raw data
        print("1. Loading raw data...")
        customers_df, interactions_df = self.load_raw_data()
        
        # Create processed datasets
        print("2. Creating customer journeys...")
        customer_journeys = self.create_customer_journey(interactions_df)
        
        print("3. Analyzing touchpoint performance...")
        touchpoint_analysis = self.create_touchpoint_analysis(interactions_df)
        
        print("4. Creating customer segments...")
        customer_segments = self.create_customer_segments(customers_df, interactions_df)
        
        print("5. Creating time series data...")
        time_series_data = self.create_time_series_data(interactions_df)
        
        # Save processed data
        processed_datasets = {
            'customer_journeys': customer_journeys,
            'touchpoint_analysis': touchpoint_analysis,
            'customer_segments': customer_segments,
            'time_series_data': time_series_data
        }
        
        print("6. Saving processed data...")
        for name, df in processed_datasets.items():
            df.to_csv(f"{self.processed_data_path}/{name}.csv", index=False)
            print(f"   Saved {name}.csv ({len(df)} rows)")
        
        print("Data processing pipeline completed!")
        return processed_datasets

if __name__ == "__main__":
    processor = CustomerDataProcessor()
    datasets = processor.process_all_data()
    
    # Display sample of each dataset
    for name, df in datasets.items():
        print(f"\n{name.upper()} - Sample Data:")
        print(df.head())
        print(f"Shape: {df.shape}")