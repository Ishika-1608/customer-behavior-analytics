import os
import json
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

def verify_credentials():
    """Verify that credentials are valid"""
    
    # Check environment variables
    project_id = os.getenv('GCP_PROJECT_ID')
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    print(f"Project ID: {project_id}")
    print(f"Credentials file: {credentials_path}")
    
    # Check if file exists
    if not os.path.exists(credentials_path):
        print(f"❌ Credentials file not found: {credentials_path}")
        return False
    
    # Check file content
    try:
        with open(credentials_path, 'r') as f:
            creds = json.load(f)
            print(f"✅ Credentials file loaded")
            print(f"   Service account email: {creds.get('client_email', 'Not found')}")
            print(f"   Project ID in file: {creds.get('project_id', 'Not found')}")
    except Exception as e:
        print(f"❌ Error reading credentials file: {e}")
        return False
    
    # Test BigQuery connection
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        client = bigquery.Client(project=project_id)
        
        # Try to list datasets (this will fail if permissions are wrong)
        datasets = list(client.list_datasets())
        print(f"✅ BigQuery connection successful!")
        print(f"   Found {len(datasets)} datasets")
        
        return True
        
    except Exception as e:
        print(f"❌ BigQuery connection failed: {e}")
        return False

if __name__ == "__main__":
    print("=== Verifying BigQuery Setup ===")
    if verify_credentials():
        print("\n🎉 Setup is working! You can proceed to the next step.")
    else:
        print("\n❌ Setup needs to be fixed before proceeding.")