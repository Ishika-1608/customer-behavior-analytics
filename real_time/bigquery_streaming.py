import pandas as pd
from google.cloud import bigquery
import os
from dotenv import load_dotenv
import time
from datetime import datetime
import threading
import queue

load_dotenv()

class BigQueryStreaming:
    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.dataset_id = 'customer_behavior_realtime'
        self.credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        
        if self.credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.credentials_path
        
        self.client = bigquery.Client(project=self.project_id)
        self.streaming_queue = queue.Queue()
        self.is_streaming = False
        
        self.setup_streaming_tables()
    
    def setup_streaming_tables(self):
        """Create real-time tables in BigQuery"""
        # Create dataset
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
        except:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            print(f"Created dataset {self.dataset_id}")
        
        # Create real-time interactions table
        table_id = f"{self.project_id}.{self.dataset_id}.realtime_interactions"
        
        schema = [
            bigquery.SchemaField("interaction_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("touchpoint", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("revenue", "FLOAT"),
            bigquery.SchemaField("action_taken", "STRING"),
            bigquery.SchemaField("customer_segment", "STRING"),
            bigquery.SchemaField("processed_timestamp", "TIMESTAMP"),
        ]
        
        try:
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
            print(f"Created table {table_id}")
        except:
            print(f"Table {table_id} already exists")
    
    def stream_interaction(self, interaction):
        """Add interaction to streaming queue"""
        # Prepare for BigQuery
        bq_interaction = {
            'interaction_id': interaction['interaction_id'],
            'customer_id': interaction['customer_id'],
            'touchpoint': interaction['touchpoint'],
            'timestamp': interaction['timestamp'],
            'revenue': interaction['revenue'],
            'action_taken': interaction['action_taken'],
            'customer_segment': interaction.get('customer_segment', ''),
            'processed_timestamp': datetime.now().isoformat()
        }
        
        self.streaming_queue.put(bq_interaction)
    
    def start_streaming_to_bigquery(self, batch_size=100, interval_seconds=30):
        """Start streaming data to BigQuery in batches"""
        self.is_streaming = True
        
        def stream_worker():
            table_id = f"{self.project_id}.{self.dataset_id}.realtime_interactions"
            
            while self.is_streaming:
                batch = []
                
                # Collect batch
                for _ in range(batch_size):
                    if not self.streaming_queue.empty():
                        batch.append(self.streaming_queue.get())
                    else:
                        break
                
                # Stream to BigQuery if we have data
                if batch:
                    try:
                        errors = self.client.insert_rows_json(
                            self.client.get_table(table_id), 
                            batch
                        )
                        if not errors:
                            print(f"✅ Streamed {len(batch)} interactions to BigQuery")
                        else:
                            print(f"❌ Errors streaming to BigQuery: {errors}")
                    except Exception as e:
                        print(f"❌ Error streaming to BigQuery: {e}")
                
                time.sleep(interval_seconds)
        
        # Start background thread
        self.thread = threading.Thread(target=stream_worker, daemon=True)
        self.thread.start()
        print("🚀 Started streaming to BigQuery")
    
    def stop_streaming(self):
        """Stop streaming to BigQuery"""
        self.is_streaming = False
        print("⏹️ Stopped streaming to BigQuery")
    
    def get_realtime_analytics(self):
        """Get real-time analytics from BigQuery"""
        queries = {
            'last_hour_summary': f"""
            SELECT 
                COUNT(*) as interactions,
                SUM(revenue) as total_revenue,
                COUNT(DISTINCT customer_id) as unique_customers,
                AVG(revenue) as avg_revenue
            FROM `{self.project_id}.{self.dataset_id}.realtime_interactions`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            """,
            
            'touchpoint_performance': f"""
            SELECT 
                touchpoint,
                COUNT(*) as interactions,
                SUM(revenue) as revenue,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM `{self.project_id}.{self.dataset_id}.realtime_interactions`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            GROUP BY touchpoint
            ORDER BY revenue DESC
            """,
            
            'minute_trends': f"""
            SELECT 
                TIMESTAMP_TRUNC(timestamp, MINUTE) as minute,
                COUNT(*) as interactions,
                SUM(revenue) as revenue
            FROM `{self.project_id}.{self.dataset_id}.realtime_interactions`
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            GROUP BY minute
            ORDER BY minute
            """
        }
        
        results = {}
        for name, query in queries.items():
            try:
                results[name] = self.client.query(query).to_dataframe()
            except Exception as e:
                print(f"Error running {name}: {e}")
                results[name] = pd.DataFrame()
        
        return results