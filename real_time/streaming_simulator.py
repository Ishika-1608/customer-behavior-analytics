import pandas as pd
import numpy as np
import time
import json
import random
from datetime import datetime, timedelta
from faker import Faker
import threading
import queue
from typing import Dict, List
import os

class RealTimeCustomerSimulator:
    def __init__(self, customers_data_path="data/raw/customers_clean.csv"):
        self.fake = Faker()
        self.customers = pd.read_csv(customers_data_path) if os.path.exists(customers_data_path) else self.generate_customers()
        self.interaction_queue = queue.Queue()
        self.is_running = False
        
        # Real-time configuration
        self.interactions_per_minute = 50  # Adjust based on your needs
        self.touchpoints = ['website', 'mobile_app', 'email', 'social_media', 'store_visit', 'customer_service']
        self.touchpoint_weights = [0.4, 0.3, 0.15, 0.1, 0.03, 0.02]
        
    def generate_customers(self, num_customers=1000):
        """Generate customer base if not exists"""
        customers = []
        for i in range(num_customers):
            customer = {
                'customer_id': f"CUST_{i+1:06d}",
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name(),
                'email': self.fake.email(),
                'age': random.randint(18, 75),
                'customer_segment': random.choice(['Premium', 'Standard', 'Basic']),
                'registration_date': self.fake.date_between(start_date='-2y', end_date='today')
            }
            customers.append(customer)
        return pd.DataFrame(customers)
    
    def generate_real_time_interaction(self):
        """Generate a single real-time interaction"""
        customer = self.customers.sample(1).iloc[0]
        touchpoint = np.random.choice(self.touchpoints, p=self.touchpoint_weights)
        
        interaction = {
            'interaction_id': f"INT_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
            'customer_id': customer['customer_id'],
            'touchpoint': touchpoint,
            'timestamp': datetime.now().isoformat(),
            'session_duration_minutes': random.randint(1, 120),
            'pages_viewed': random.randint(1, 15) if touchpoint in ['website', 'mobile_app'] else None,
            'action_taken': random.choice(['view', 'click', 'purchase', 'download', 'subscribe', 'contact']),
            'product_category': random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty']),
            'revenue': round(random.uniform(0, 500), 2) if random.random() < 0.3 else 0,
            'device_type': random.choice(['Desktop', 'Mobile', 'Tablet']) if touchpoint in ['website', 'mobile_app'] else None,
            'campaign_id': f"CAMP_{random.randint(1, 50):03d}" if random.random() < 0.4 else None,
            'customer_segment': customer['customer_segment'],
            'customer_age': customer['age']
        }
        
        return interaction
    
    def start_streaming(self):
        """Start generating real-time interactions"""
        self.is_running = True
        print(f"🚀 Starting real-time simulation: {self.interactions_per_minute} interactions/minute")
        
        def generate_interactions():
            while self.is_running:
                interaction = self.generate_real_time_interaction()
                self.interaction_queue.put(interaction)
                
                # Sleep to maintain desired rate
                sleep_time = 60 / self.interactions_per_minute
                time.sleep(sleep_time)
        
        # Start background thread
        self.thread = threading.Thread(target=generate_interactions, daemon=True)
        self.thread.start()
    
    def stop_streaming(self):
        """Stop generating interactions"""
        self.is_running = False
        print("⏹️ Stopped real-time simulation")
    
    def get_recent_interactions(self, count=100):
        """Get recent interactions from queue"""
        interactions = []
        for _ in range(min(count, self.interaction_queue.qsize())):
            if not self.interaction_queue.empty():
                interactions.append(self.interaction_queue.get())
        return interactions
    
    def get_streaming_stats(self):
        """Get current streaming statistics"""
        return {
            'queue_size': self.interaction_queue.qsize(),
            'is_running': self.is_running,
            'interactions_per_minute': self.interactions_per_minute,
            'total_customers': len(self.customers)
        }

if __name__ == "__main__":
    # Test the real-time simulator
    simulator = RealTimeCustomerSimulator()
    simulator.start_streaming()
    
    try:
        for i in range(10):
            time.sleep(5)
            interactions = simulator.get_recent_interactions(10)
            print(f"Generated {len(interactions)} interactions in last 5 seconds")
            if interactions:
                print(f"Latest: {interactions[-1]['touchpoint']} - ${interactions[-1]['revenue']}")
    except KeyboardInterrupt:
        simulator.stop_streaming()