import os
from typing import Dict, Any

class BigQueryConfig:
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID', 'your-project-id')
        self.dataset_id = os.getenv('BQ_DATASET_ID', 'customer_behavior')
        self.credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'path/to/service-account.json')
        self.location = os.getenv('BQ_LOCATION', 'US')
    
    def get_config(self) -> Dict[str, Any]:
        return {
            'project_id': self.project_id,
            'dataset_id': self.dataset_id,
            'credentials_path': self.credentials_path,
            'location': self.location
        }
    
    def validate_config(self) -> bool:
        """Validate that all required configuration is present"""
        if not os.path.exists(self.credentials_path):
            print(f"Error: Credentials file not found at {self.credentials_path}")
            return False
        
        if self.project_id == 'your-project-id':
            print("Error: Please update your GCP project ID")
            return False
        
        return True