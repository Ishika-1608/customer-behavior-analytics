import schedule
import time
from datetime import datetime
import logging
from data_processor import CustomerDataProcessor
from data_collection.customer_simulator import CustomerBehaviorSimulator

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

class DataPipelineScheduler:
    def __init__(self):
        self.simulator = CustomerBehaviorSimulator()
        self.processor = CustomerDataProcessor()
        
    def run_data_collection(self):
        """Run data collection job"""
        try:
            logging.info("Starting data collection job...")
            self.simulator.generate_customers()
            self.simulator.generate_interactions()
            self.simulator.save_data()
            logging.info("Data collection completed successfully")
        except Exception as e:
            logging.error(f"Data collection failed: {str(e)}")
    
    def run_data_processing(self):
        """Run data processing job"""
        try:
            logging.info("Starting data processing job...")
            self.processor.process_all_data()
            logging.info("Data processing completed successfully")
        except Exception as e:
            logging.error(f"Data processing failed: {str(e)}")
    
    def run_full_pipeline(self):
        """Run the complete pipeline"""
        logging.info("="*50)
        logging.info("STARTING FULL DATA PIPELINE")
        logging.info("="*50)
        
        self.run_data_collection()
        time.sleep(2)  # Small delay between jobs
        self.run_data_processing()
        
        logging.info("="*50)
        logging.info("PIPELINE COMPLETED")
        logging.info("="*50)

def main():
    scheduler = DataPipelineScheduler()
    
    # Schedule jobs
    schedule.every().day.at("02:00").do(scheduler.run_full_pipeline)
    schedule.every().hour.do(scheduler.run_data_processing)  # Process data every hour
    
    print("Data pipeline scheduler started...")
    print("Scheduled jobs:")
    print("- Full pipeline: Daily at 2:00 AM")
    print("- Data processing: Every hour")
    print("Press Ctrl+C to stop")
    
    # Run once immediately for testing
    print("\nRunning pipeline once for testing...")
    scheduler.run_full_pipeline()
    
    # Keep the scheduler running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()