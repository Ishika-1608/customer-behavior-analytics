import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import json
import os
from typing import List, Dict, Any

class CustomerBehaviorSimulator:
    def __init__(self, config_path: str = "config/config.yaml"):
        self.fake = Faker()
        self.config = self._load_config(config_path)
        self.customers = []
        self.interactions = []
        
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        import yaml
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            # Return default config if file doesn't exist
            return {
                'data_collection': {
                    'simulation': {
                        'num_customers': 1000,
                        'days_to_simulate': 30,
                        'touchpoints': ['website', 'mobile_app', 'email', 'social_media', 'store_visit', 'customer_service']
                    },
                    'output_path': 'data/raw/'
                },
                'touchpoint_weights': {
                    'website': 0.4,
                    'mobile_app': 0.3,
                    'email': 0.15,
                    'social_media': 0.1,
                    'store_visit': 0.03,
                    'customer_service': 0.02
                }
            }
    
    def generate_customers(self) -> List[Dict]:
        """Generate synthetic customer profiles"""
        customers = []
        num_customers = self.config['data_collection']['simulation']['num_customers']
        
        for i in range(num_customers):
            customer = {
                'customer_id': f"CUST_{i+1:06d}",
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'phone': self.fake.phone_number(),
                'age': random.randint(18, 75),
                'gender': random.choice(['M', 'F', 'Other']),
                'city': self.fake.city(),
                'state': self.fake.state(),
                'country': self.fake.country(),
                'registration_date': self.fake.date_between(start_date='-2y', end_date='today'),
                'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
                'preferred_channel': random.choice(['website', 'mobile_app', 'store']),
                'lifetime_value': round(random.uniform(100, 5000), 2)
            }
            customers.append(customer)
        
        self.customers = customers
        return customers
    
    def generate_interactions(self) -> List[Dict]:
        """Generate customer interactions across touchpoints"""
        if not self.customers:
            self.generate_customers()
            
        interactions = []
        touchpoints = self.config['data_collection']['simulation']['touchpoints']
        weights = list(self.config['touchpoint_weights'].values())
        days_to_simulate = self.config['data_collection']['simulation']['days_to_simulate']
        
        start_date = datetime.now() - timedelta(days=days_to_simulate)
        
        for customer in self.customers:
            # Each customer has 1-20 interactions over the period
            num_interactions = random.randint(1, 20)
            
            for _ in range(num_interactions):
                touchpoint = np.random.choice(touchpoints, p=weights)
                interaction_date = start_date + timedelta(
                    days=random.randint(0, days_to_simulate),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )
                
                interaction = {
                    'interaction_id': f"INT_{len(interactions)+1:08d}",
                    'customer_id': customer['customer_id'],
                    'touchpoint': touchpoint,
                    'timestamp': interaction_date.isoformat(),
                    'session_duration_minutes': random.randint(1, 120),
                    'pages_viewed': random.randint(1, 15) if touchpoint in ['website', 'mobile_app'] else None,
                    'action_taken': random.choice(['view', 'click', 'purchase', 'download', 'subscribe', 'contact']),
                    'product_category': random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty']),
                    'revenue': round(random.uniform(0, 500), 2) if random.random() < 0.3 else 0,
                    'device_type': random.choice(['Desktop', 'Mobile', 'Tablet']) if touchpoint in ['website', 'mobile_app'] else None,
                    'referrer_source': random.choice(['organic', 'paid_search', 'social', 'email', 'direct']) if touchpoint == 'website' else None,
                    'campaign_id': f"CAMP_{random.randint(1, 50):03d}" if random.random() < 0.4 else None
                }
                
                interactions.append(interaction)
        
        self.interactions = interactions
        return interactions
    
    def save_data(self) -> None:
        """Save generated data to CSV files"""
        output_path = self.config['data_collection']['output_path']
        os.makedirs(output_path, exist_ok=True)
        
        # Save customers
        customers_df = pd.DataFrame(self.customers)
        customers_df.to_csv(f"{output_path}/customers.csv", index=False)
        
        # Save interactions
        interactions_df = pd.DataFrame(self.interactions)
        interactions_df.to_csv(f"{output_path}/interactions.csv", index=False)
        
        print(f"Data saved to {output_path}")
        print(f"Generated {len(self.customers)} customers and {len(self.interactions)} interactions")
        
        return customers_df, interactions_df

if __name__ == "__main__":
    simulator = CustomerBehaviorSimulator()
    simulator.generate_customers()
    simulator.generate_interactions()
    customers_df, interactions_df = simulator.save_data()
    
    print("\nCustomer Data Sample:")
    print(customers_df.head())
    print("\nInteraction Data Sample:")
    print(interactions_df.head())