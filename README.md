# 🎯 Customer Behavior Analytics Platform

A comprehensive real-time customer behavior tracking system with AI-powered insights and external data integration.

LIVE DEMO: https://customer-behavior-analytics.streamlit.app/

## 🚀 Features

- **Real-time Data Streaming** - Live customer interaction tracking
- **AI-Powered Insights** - Natural language Q&A about customer behavior
- **External API Integration** - Weather, market, and news sentiment correlation
- **Interactive Dashboards** - Historical and real-time analytics
- **Cloud Data Warehouse** - BigQuery integration for scalable storage
- **Automated Data Pipeline** - ETL processing with data quality checks

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- Google Cloud Platform account
- (Optional) OpenAI API key for advanced AI features

### Quick Start

1. **Clone the repository**
   ```bash
   git clone https://github.com/Tanmay-1007/customer-behavior-analytics.git
   cd customer-behavior-analytics

2. **Set up the virtual environment**
    python -m venv customer_behavior_env
    source customer_behavior_env/bin/activate  # On Windows: customer_behavior_env\Scripts\activate

3. **Install dependencies**
    pip install -r requirements.txt

4. **Configure environment variables**
    cp .env.example .env
    # Edit .env with your API keys and configuration

5. **Set Up Google Cloud**
    Create a GCP project
    Enable BigQuery API
    Create service account and download JSON key
    Place key file as service-account-key.json

6. **Initialize the system**
    python run_pipeline.py
    python upload_to_bigquery.py

##  🚀 Usage
**Historical Analytics Dashboard**
streamlit run dashboard/working_dashboard.py

**Real-time Analytics Dashboard**
streamlit run real_time/enhanced_real_time_dashboard.py

**AI Insights**
python rag_system/local_insights_ai.py

##  📊 Dashboards
**Historical Analytics**
    Customer segmentation analysis
    Journey path optimization
    Touchpoint performance metrics
    Time series trends

**Real-time Analytics**
    Live interaction monitoring
    External factor correlation (weather, market, news)
    Real-time conversion tracking
    Dynamic recommendations

##  🤖 AI Features
    Natural Language Q&A - Ask questions about your data in plain English
    Automated Insights - AI-generated recommendations and observations
    Correlation Analysis - Discover hidden patterns in customer behavior
    Predictive Analytics - Forecast trends based on external factors

##  🌐 External Integrations
    Weather Data - OpenWeatherMap API
    Stock Market - Alpha Vantage API
    News Sentiment - News API
    Economic Indicators - Custom economic data feeds

##  📁 Project Structure
customer-behavior-system/
├── config/                 # Configuration files
├── data/                   # Data storage (raw & processed)
├── data_collection/        # Data generation and collection
├── data_pipeline/          # ETL processing
├── data_warehouse/         # BigQuery integration
├── dashboard/              # Streamlit dashboards
├── rag_system/            # AI insights system
├── real_time/             # Real-time processing
├── external_apis/         # External API integrations
└── requirements.txt       # Python dependencies

##  🔧 Configuration
**Environment Variables (.env)**
# Google Cloud Platform
GCP_PROJECT_ID=your-project-id
BQ_DATASET_ID=customer_behavior
GOOGLE_APPLICATION_CREDENTIALS=service-account-key.json

# External APIs (Optional)
OPENWEATHER_API_KEY=your-weather-api-key
NEWS_API_KEY=your-news-api-key
ALPHA_VANTAGE_API_KEY=your-stock-api-key
OPENAI_API_KEY=your-openai-api-key

##  🚀 Deployment
**Local Development**
streamlit run dashboard/working_dashboard.py

**Docker Deployment**
docker build -t customer-analytics .
docker run -p 8501:8501 customer-analytics

**Cloud Deployment**
Streamlit Cloud - Connect GitHub repo for automatic deployment
Google Cloud Run - Containerized deployment
AWS ECS - Enterprise container deployment
Heroku - Simple PaaS deployment

##  📈 Performance
Real-time Processing - 50+ interactions/minute
Dashboard Response - <2 seconds for most queries
Data Pipeline - Processes 100K+ interactions/hour
AI Insights - <5 seconds for complex analysis

##  🤝 Contributing
Fork the repository
Create a feature branch (git checkout -b feature/amazing-feature)
Commit your changes (git commit -m 'Add amazing feature')
Push to the branch (git push origin feature/amazing-feature)
Open a Pull Request

##  📄 License
This project is licensed under the MIT License - see the LICENSE file for details.

##  🙏 Acknowledgments
Built with Streamlit, BigQuery, and OpenAI
External data from OpenWeatherMap, Alpha Vantage, and News API
Inspired by modern data analytics and customer intelligence platforms

##  📞 Support
For questions and support:

Create an issue on GitHub
Email: malavtanmay712@gmail.com
