import os
import pandas as pd
from dotenv import load_dotenv
from data_warehouse.bigquery_manager import BigQueryManager

load_dotenv()

def main():
    print("=== Uploading Data to BigQuery ===")
    
    # Initialize BigQuery manager
    bq_manager = BigQueryManager(
        project_id=os.getenv('GCP_PROJECT_ID'),
        credentials_path=os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    )
    
    # Ensure tables exist
    bq_manager.setup_all_tables()
    
    # Upload data with better error handling
    try:
        bq_manager.upload_all_processed_data()
        
        # Test with a simple query
        print("\n🧪 Testing data upload with sample queries...")
        
        # Test customers table
        query = f"""
        SELECT COUNT(*) as customer_count
        FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.customers`
        """
        result = bq_manager.query_data(query)
        print(f"✅ Customers table: {result.iloc[0]['customer_count']} rows")
        
        # Test interactions table
        query = f"""
        SELECT COUNT(*) as interaction_count
        FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.interactions`
        """
        result = bq_manager.query_data(query)
        print(f"✅ Interactions table: {result.iloc[0]['interaction_count']} rows")
        
        # Test touchpoint analysis
        query = f"""
        SELECT touchpoint, total_revenue
        FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.touchpoint_analysis`
        ORDER BY total_revenue DESC
        LIMIT 3
        """
        result = bq_manager.query_data(query)
        print(f"✅ Touchpoint analysis table:")
        print(result.to_string(index=False))
        
        print("\n🎉 All data uploaded successfully!")
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    main()