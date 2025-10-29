import pandas as pd
import numpy as np
import os

class DataCleaner:
    def __init__(self, raw_data_path="data/raw/", processed_data_path="data/processed/"):
        self.raw_data_path = raw_data_path
        self.processed_data_path = processed_data_path
    
    def clean_interactions_data(self):
        """Clean interactions data for BigQuery compatibility"""
        interactions_path = os.path.join(self.raw_data_path, "interactions.csv")
        interactions_df = pd.read_csv(interactions_path)
        
        print(f"Original interactions shape: {interactions_df.shape}")
        
        # Convert decimal columns to integers (handle NaN values)
        integer_columns = ['session_duration_minutes', 'pages_viewed']
        for col in integer_columns:
            if col in interactions_df.columns:
                # Convert to numeric first, then to int (handling NaN)
                interactions_df[col] = pd.to_numeric(interactions_df[col], errors='coerce')
                interactions_df[col] = interactions_df[col].fillna(0).astype(int)
        
        # Convert revenue to float (ensure proper decimal handling)
        interactions_df['revenue'] = pd.to_numeric(interactions_df['revenue'], errors='coerce').fillna(0.0)
        
        # Convert timestamp to proper datetime format
        interactions_df['timestamp'] = pd.to_datetime(interactions_df['timestamp'])
        
        # Handle NULL values for string columns
        string_columns = ['device_type', 'referrer_source', 'campaign_id']
        for col in string_columns:
            if col in interactions_df.columns:
                interactions_df[col] = interactions_df[col].fillna('')
        
        # Save cleaned data
        clean_path = os.path.join(self.raw_data_path, "interactions_clean.csv")
        interactions_df.to_csv(clean_path, index=False)
        print(f"Cleaned interactions saved to: {clean_path}")
        
        return interactions_df
    
    def clean_customers_data(self):
        """Clean customers data for BigQuery compatibility"""
        customers_path = os.path.join(self.raw_data_path, "customers.csv")
        customers_df = pd.read_csv(customers_path)
        
        print(f"Original customers shape: {customers_df.shape}")
        
        # Convert age to integer
        customers_df['age'] = pd.to_numeric(customers_df['age'], errors='coerce').fillna(0).astype(int)
        
        # Convert lifetime_value to float
        customers_df['lifetime_value'] = pd.to_numeric(customers_df['lifetime_value'], errors='coerce').fillna(0.0)
        
        # Convert registration_date to proper date format
        customers_df['registration_date'] = pd.to_datetime(customers_df['registration_date']).dt.date
        
        # Handle NULL values for string columns
        string_columns = ['phone', 'city', 'state', 'country']
        for col in string_columns:
            if col in customers_df.columns:
                customers_df[col] = customers_df[col].fillna('')
        
        # Save cleaned data
        clean_path = os.path.join(self.raw_data_path, "customers_clean.csv")
        customers_df.to_csv(clean_path, index=False)
        print(f"Cleaned customers saved to: {clean_path}")
        
        return customers_df
    
    def clean_all_processed_data(self):
        """Clean all processed data files"""
        processed_files = [
            'customer_journeys.csv',
            'touchpoint_analysis.csv', 
            'customer_segments.csv',
            'time_series_data.csv'
        ]
        
        for filename in processed_files:
            filepath = os.path.join(self.processed_data_path, filename)
            if os.path.exists(filepath):
                print(f"Cleaning {filename}...")
                df = pd.read_csv(filepath)
                
                # Convert all numeric columns properly
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Try to convert to numeric if it looks like a number
                        try:
                            numeric_series = pd.to_numeric(df[col], errors='coerce')
                            if not numeric_series.isna().all():
                                # If conversion worked for most values, use it
                                if numeric_series.notna().sum() > len(df) * 0.8:
                                    df[col] = numeric_series.fillna(0)
                                    # Convert to int if all values are whole numbers
                                    if (df[col] % 1 == 0).all():
                                        df[col] = df[col].astype(int)
                        except:
                            pass
                
                # Handle date columns
                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date']).dt.date
                
                # Save cleaned file
                clean_path = os.path.join(self.processed_data_path, f"{filename.replace('.csv', '_clean.csv')}")
                df.to_csv(clean_path, index=False)
                print(f"Cleaned {filename} saved as {clean_path}")
    
    def clean_all_data(self):
        """Clean all data files"""
        print("=== Starting Data Cleaning Process ===")
        
        # Clean raw data
        self.clean_customers_data()
        self.clean_interactions_data()
        
        # Clean processed data
        self.clean_all_processed_data()
        
        print("=== Data Cleaning Completed ===")

if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.clean_all_data()