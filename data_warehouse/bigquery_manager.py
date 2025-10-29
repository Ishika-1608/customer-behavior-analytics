import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
import json
from typing import Dict, List, Optional
from datetime import datetime

class BigQueryManager:
    def __init__(self, project_id: str, dataset_id: str = "customer_behavior", 
                 credentials_path: str = None):
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        # Set up credentials
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset_id)
        
    def create_dataset(self) -> None:
        """Create BigQuery dataset if it doesn't exist"""
        try:
            self.client.get_dataset(self.dataset_ref)
            print(f"Dataset {self.dataset_id} already exists")
        except NotFound:
            dataset = bigquery.Dataset(self.dataset_ref)
            dataset.location = "US"  # or your preferred location
            dataset.description = "Customer behavior tracking data"
            
            dataset = self.client.create_dataset(dataset, timeout=30)
            print(f"Created dataset {self.dataset_id}")
    
    def get_table_schema(self, table_name: str) -> List[bigquery.SchemaField]:
        """Define schemas for different tables"""
        schemas = {
            'customers': [
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("first_name", "STRING"),
                bigquery.SchemaField("last_name", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("phone", "STRING"),
                bigquery.SchemaField("age", "INTEGER"),
                bigquery.SchemaField("gender", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("state", "STRING"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("registration_date", "DATE"),
                bigquery.SchemaField("customer_segment", "STRING"),
                bigquery.SchemaField("preferred_channel", "STRING"),
                bigquery.SchemaField("lifetime_value", "FLOAT"),
            ],
            'interactions': [
                bigquery.SchemaField("interaction_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("touchpoint", "STRING"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("session_duration_minutes", "INTEGER"),
                bigquery.SchemaField("pages_viewed", "INTEGER"),
                bigquery.SchemaField("action_taken", "STRING"),
                bigquery.SchemaField("product_category", "STRING"),
                bigquery.SchemaField("revenue", "FLOAT"),
                bigquery.SchemaField("device_type", "STRING"),
                bigquery.SchemaField("referrer_source", "STRING"),
                bigquery.SchemaField("campaign_id", "STRING"),
            ],
            'customer_journeys': [
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("journey_path", "STRING"),
                bigquery.SchemaField("total_interactions", "INTEGER"),
                bigquery.SchemaField("total_revenue", "FLOAT"),
                bigquery.SchemaField("journey_duration_days", "INTEGER"),
                bigquery.SchemaField("first_touchpoint", "STRING"),
                bigquery.SchemaField("last_touchpoint", "STRING"),
                bigquery.SchemaField("unique_touchpoints", "INTEGER"),
                bigquery.SchemaField("conversion_occurred", "BOOLEAN"),
                bigquery.SchemaField("touchpoint_counts", "STRING"),
            ],
            'touchpoint_analysis': [
                bigquery.SchemaField("touchpoint", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("total_interactions", "INTEGER"),
                bigquery.SchemaField("total_revenue", "FLOAT"),
                bigquery.SchemaField("avg_revenue_per_interaction", "FLOAT"),
                bigquery.SchemaField("avg_session_duration", "FLOAT"),
                bigquery.SchemaField("unique_customers", "INTEGER"),
                bigquery.SchemaField("conversion_rate", "FLOAT"),
            ],
            'customer_segments': [
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("first_name", "STRING"),
                bigquery.SchemaField("last_name", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("phone", "STRING"),
                bigquery.SchemaField("age", "INTEGER"),
                bigquery.SchemaField("gender", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("state", "STRING"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("registration_date", "DATE"),
                bigquery.SchemaField("customer_segment", "STRING"),
                bigquery.SchemaField("preferred_channel", "STRING"),
                bigquery.SchemaField("lifetime_value", "FLOAT"),
                bigquery.SchemaField("total_interactions", "INTEGER"),
                bigquery.SchemaField("total_revenue", "FLOAT"),
                bigquery.SchemaField("unique_touchpoints_used", "INTEGER"),
                bigquery.SchemaField("avg_session_duration", "FLOAT"),
                bigquery.SchemaField("first_interaction", "TIMESTAMP"),
                bigquery.SchemaField("last_interaction", "TIMESTAMP"),
                bigquery.SchemaField("recency_days", "INTEGER"),
                bigquery.SchemaField("frequency_score", "STRING"),
                bigquery.SchemaField("monetary_score", "STRING"),
                bigquery.SchemaField("recency_score", "STRING"),
            ],
            'time_series_data': [
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("daily_interactions", "INTEGER"),
                bigquery.SchemaField("daily_unique_customers", "INTEGER"),
                bigquery.SchemaField("daily_revenue", "FLOAT"),
                bigquery.SchemaField("avg_session_duration", "FLOAT"),
                bigquery.SchemaField("daily_website_interactions", "INTEGER"),
                bigquery.SchemaField("daily_mobile_app_interactions", "INTEGER"),
                bigquery.SchemaField("daily_email_interactions", "INTEGER"),
                bigquery.SchemaField("daily_social_media_interactions", "INTEGER"),
                bigquery.SchemaField("daily_store_visit_interactions", "INTEGER"),
                bigquery.SchemaField("daily_customer_service_interactions", "INTEGER"),
            ]
        }
        
        return schemas.get(table_name, [])

    def create_table(self, table_name: str) -> None:
        """Create a table with the appropriate schema"""
        table_ref = self.dataset_ref.table(table_name)
        
        try:
            self.client.get_table(table_ref)
            print(f"Table {table_name} already exists")
        except NotFound:
            schema = self.get_table_schema(table_name)
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            print(f"Created table {table_name}")
    
    def upload_dataframe(self, df: pd.DataFrame, table_name: str, 
                        write_disposition: str = "WRITE_TRUNCATE") -> None:
        """Upload a pandas DataFrame to BigQuery"""
        table_ref = self.dataset_ref.table(table_name)
        
        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,  # WRITE_TRUNCATE, WRITE_APPEND, or WRITE_EMPTY
            schema=self.get_table_schema(table_name)
        )
        
        # Upload the data
        job = self.client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        
        print(f"Uploaded {len(df)} rows to {table_name}")
    
    def upload_csv_file(self, file_path: str, table_name: str,
                       write_disposition: str = "WRITE_TRUNCATE") -> None:
        """Upload a CSV file directly to BigQuery"""
        table_ref = self.dataset_ref.table(table_name)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=False,
            schema=self.get_table_schema(table_name),
            write_disposition=write_disposition
        )
        
        with open(file_path, "rb") as source_file:
            job = self.client.load_table_from_file(source_file, table_ref, job_config=job_config)
        
        job.result()  # Wait for the job to complete
        print(f"Uploaded {file_path} to {table_name}")
    
    def query_data(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame"""
        return self.client.query(query).to_dataframe()
    
    def setup_all_tables(self) -> None:
        """Create dataset and all required tables"""
        print("Setting up BigQuery dataset and tables...")
        
        self.create_dataset()
        
        tables = ['customers', 'interactions', 'customer_journeys', 
                 'touchpoint_analysis', 'customer_segments', 'time_series_data']
        
        for table in tables:
            self.create_table(table)
        
        print("BigQuery setup completed!")
    
    def upload_all_processed_data(self, data_path: str = "data/processed/") -> None:
        """Upload all processed data using DataFrames for better compatibility"""
        print("Uploading processed data to BigQuery...")
        
        try:
            # Upload raw data first
            raw_data_path = "data/raw/"
            
            # Upload customers
            customers_clean_path = os.path.join(raw_data_path, "customers_clean.csv")
            if os.path.exists(customers_clean_path):
                customers_df = pd.read_csv(customers_clean_path)
                # Convert date column
                customers_df['registration_date'] = pd.to_datetime(customers_df['registration_date']).dt.date
                self.upload_dataframe(customers_df, "customers")
            
            # Upload interactions
            interactions_clean_path = os.path.join(raw_data_path, "interactions_clean.csv")
            if os.path.exists(interactions_clean_path):
                interactions_df = pd.read_csv(interactions_clean_path)
                # Convert timestamp column
                interactions_df['timestamp'] = pd.to_datetime(interactions_df['timestamp'])
                self.upload_dataframe(interactions_df, "interactions")
            
            # Upload processed data
            processed_files = {
                'customer_journeys_clean.csv': 'customer_journeys',
                'touchpoint_analysis_clean.csv': 'touchpoint_analysis', 
                'customer_segments_clean.csv': 'customer_segments',
                'time_series_data_clean.csv': 'time_series_data'
            }
            
            for file_name, table_name in processed_files.items():
                file_path = os.path.join(data_path, file_name)
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    
                    # Handle specific data type conversions
                    if table_name == 'customer_segments':
                        # Convert date columns
                        if 'registration_date' in df.columns:
                            df['registration_date'] = pd.to_datetime(df['registration_date']).dt.date
                        if 'first_interaction' in df.columns:
                            df['first_interaction'] = pd.to_datetime(df['first_interaction'])
                        if 'last_interaction' in df.columns:
                            df['last_interaction'] = pd.to_datetime(df['last_interaction'])
                    
                    elif table_name == 'time_series_data':
                        # Convert date column
                        if 'date' in df.columns:
                            df['date'] = pd.to_datetime(df['date']).dt.date
                    
                    # Select only columns that match our schema
                    schema_columns = [field.name for field in self.get_table_schema(table_name)]
                    df_filtered = df[[col for col in schema_columns if col in df.columns]]
                    
                    self.upload_dataframe(df_filtered, table_name)
                else:
                    # Try original file if clean version doesn't exist
                    original_file = file_name.replace('_clean', '')
                    original_path = os.path.join(data_path, original_file)
                    if os.path.exists(original_path):
                        df = pd.read_csv(original_path)
                        
                        # Apply same transformations
                        if table_name == 'customer_segments':
                            if 'registration_date' in df.columns:
                                df['registration_date'] = pd.to_datetime(df['registration_date']).dt.date
                            if 'first_interaction' in df.columns:
                                df['first_interaction'] = pd.to_datetime(df['first_interaction'])
                            if 'last_interaction' in df.columns:
                                df['last_interaction'] = pd.to_datetime(df['last_interaction'])
                        elif table_name == 'time_series_data':
                            if 'date' in df.columns:
                                df['date'] = pd.to_datetime(df['date']).dt.date
                        
                        # Select only schema columns
                        schema_columns = [field.name for field in self.get_table_schema(table_name)]
                        df_filtered = df[[col for col in schema_columns if col in df.columns]]
                        
                        self.upload_dataframe(df_filtered, table_name)
                    else:
                        print(f"Warning: Neither {file_path} nor {original_path} found")
            
            print("Data upload completed!")
            
        except Exception as e:
            print(f"Error during upload: {e}")
            raise e

# Example usage and testing
if __name__ == "__main__":
    # You'll need to update these with your actual values
    PROJECT_ID = "copper-imprint-476613-d6"  # Replace with your GCP project ID
    CREDENTIALS_PATH = "service-account-key.json" # Replace with your key file path
    
    # Initialize BigQuery manager
    bq_manager = BigQueryManager(
        project_id=PROJECT_ID,
        credentials_path=CREDENTIALS_PATH
    )
    
    # Set up tables
    bq_manager.setup_all_tables()
    
    # Upload data
    bq_manager.upload_all_processed_data()
    
    # Test query
    query = """
    SELECT touchpoint, total_interactions, total_revenue, conversion_rate
    FROM `{}.customer_behavior.touchpoint_analysis`
    ORDER BY total_revenue DESC
    """.format(PROJECT_ID)
    
    results = bq_manager.query_data(query)
    print("\nTouchpoint Analysis Results:")
    print(results)