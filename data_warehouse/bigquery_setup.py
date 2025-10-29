import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from data_warehouse.bigquery_manager import BigQueryManager

def test_bigquery_connection():
    """Test BigQuery connection and setup"""
    
    # Get configuration from environment
    project_id = os.getenv('GCP_PROJECT_ID')
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    print(f"Project ID: {project_id}")
    print(f"Credentials: {credentials_path}")
    
    # Check if credentials file exists
    if not os.path.exists(credentials_path):
        print(f"❌ Error: Credentials file not found at {credentials_path}")
        print("Please download your service account JSON key and place it in the project root")
        return False
    
    if project_id == 'your-actual-project-id-here':
        print("❌ Error: Please update your GCP_PROJECT_ID in the .env file")
        return False
    
    try:
        # Initialize BigQuery manager
        print("\n🔄 Initializing BigQuery connection...")
        bq_manager = BigQueryManager(
            project_id=project_id,
            credentials_path=credentials_path
        )
        
        # Test connection by listing datasets
        datasets = list(bq_manager.client.list_datasets())
        print(f"✅ Successfully connected to BigQuery!")
        print(f"📊 Found {len(datasets)} existing datasets")
        
        return bq_manager
        
    except Exception as e:
        print(f"❌ Error connecting to BigQuery: {str(e)}")
        return False

def setup_complete_bigquery():
    """Set up BigQuery with all tables and upload data"""
    
    bq_manager = test_bigquery_connection()
    if not bq_manager:
        return False
    
    try:
        print("\n🏗️  Setting up BigQuery dataset and tables...")
        bq_manager.setup_all_tables()
        
        print("\n📤 Uploading data to BigQuery...")
        bq_manager.upload_all_processed_data()
        
        print("\n🧪 Testing with a sample query...")
        query = f"""
        SELECT 
            touchpoint,
            total_interactions,
            total_revenue,
            conversion_rate
        FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.touchpoint_analysis`
        ORDER BY total_revenue DESC
        LIMIT 5
        """
        
        results = bq_manager.query_data(query)
        print("\n📊 Sample Touchpoint Analysis:")
        print(results.to_string(index=False))
        
        print("\n✅ BigQuery setup completed successfully!")
        print(f"🌐 View your data at: https://console.cloud.google.com/bigquery?project={bq_manager.project_id}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during setup: {str(e)}")
        return False

if __name__ == "__main__":
    print("=== BigQuery Setup and Test ===")
    setup_complete_bigquery()