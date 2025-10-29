import os
from dotenv import load_dotenv
from data_warehouse.bigquery_manager import BigQueryManager

load_dotenv()

def test_queries():
    bq_manager = BigQueryManager(
        project_id=os.getenv('GCP_PROJECT_ID'),
        credentials_path=os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    )
    
    print("=== Testing BigQuery Queries ===")
    
    # Test basic counts
    queries = {
        "Total Customers": f"SELECT COUNT(*) as count FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.customers`",
        "Total Interactions": f"SELECT COUNT(*) as count FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.interactions`",
        "Touchpoint Performance": f"""
        SELECT touchpoint, total_revenue, conversion_rate 
        FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.touchpoint_analysis` 
        ORDER BY total_revenue DESC
        """,
        "Customer Segments": f"""
        SELECT customer_segment, COUNT(*) as count, AVG(total_revenue) as avg_revenue
        FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.customer_segments`
        GROUP BY customer_segment
        ORDER BY avg_revenue DESC
        """
    }
    
    for name, query in queries.items():
        print(f"\n📊 {name}:")
        try:
            result = bq_manager.query_data(query)
            print(result.to_string(index=False))
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_queries()