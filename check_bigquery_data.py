import os
from dotenv import load_dotenv
from google.cloud import bigquery
import streamlit as st

load_dotenv()

def check_bigquery_tables():
    """Check if BigQuery tables have data"""
    
    # Get configuration
    if hasattr(st, 'secrets'):
        project_id = st.secrets["GCP_PROJECT_ID"]
        # Set up credentials from secrets
        import json
        import tempfile
        credentials_dict = dict(st.secrets["GOOGLE_APPLICATION_CREDENTIALS"])
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(credentials_dict, f)
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f.name
    else:
        project_id = os.getenv('GCP_PROJECT_ID')
    
    client = bigquery.Client(project=project_id)
    dataset_id = "customer_behavior"
    
    tables_to_check = [
        'customers',
        'interactions', 
        'customer_journeys',
        'touchpoint_analysis',
        'customer_segments',
        'time_series_data'
    ]
    
    print("=== BigQuery Table Status ===")
    
    for table_name in tables_to_check:
        try:
            query = f"SELECT COUNT(*) as row_count FROM `{project_id}.{dataset_id}.{table_name}`"
            result = client.query(query)
            row_count = list(result)[0].row_count
            
            if row_count > 0:
                print(f"✅ {table_name}: {row_count:,} rows")
            else:
                print(f"⚠️ {table_name}: EMPTY")
        except Exception as e:
            print(f"❌ {table_name}: ERROR - {e}")
    
    print("\n=== Recommendations ===")
    print("If tables are empty:")
    print("1. Run: python run_pipeline.py")
    print("2. Run: python upload_to_bigquery.py")
    print("3. Check your data generation and upload process")

if __name__ == "__main__":
    check_bigquery_tables()