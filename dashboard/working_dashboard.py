import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import json
import tempfile
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables for local development
load_dotenv()

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

st.set_page_config(
    page_title="Customer Behavior Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def get_credentials():
    """Get credentials from Streamlit secrets or environment"""
    try:
        if hasattr(st, 'secrets') and 'GOOGLE_APPLICATION_CREDENTIALS' in st.secrets:
            # Running in Streamlit Cloud
            credentials_dict = dict(st.secrets["GOOGLE_APPLICATION_CREDENTIALS"])
            
            # Create temporary file for credentials
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                json.dump(credentials_dict, f)
                return f.name
        else:
            # Running locally
            return os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'service-account-key.json')
    except Exception as e:
        st.error(f"Error loading credentials: {e}")
        return None

def get_config():
    """Get configuration from Streamlit secrets or environment"""
    try:
        if hasattr(st, 'secrets'):
            # Running in Streamlit Cloud
            return {
                'project_id': st.secrets.get("GCP_PROJECT_ID"),
                'dataset_id': st.secrets.get("BQ_DATASET_ID", "customer_behavior"),
                'credentials_path': get_credentials()
            }
        else:
            # Running locally
            return {
                'project_id': os.getenv('GCP_PROJECT_ID'),
                'dataset_id': os.getenv('BQ_DATASET_ID', 'customer_behavior'),
                'credentials_path': os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'service-account-key.json')
            }
    except Exception as e:
        st.error(f"Error loading configuration: {e}")
        return None

@st.cache_resource
def get_bigquery_client():
    """Initialize BigQuery client with proper credentials - using cache_resource for client objects"""
    try:
        config = get_config()
        if not config:
            raise Exception("Configuration not loaded")
        
        project_id = config['project_id']
        credentials_path = config['credentials_path']
        
        if not project_id:
            raise Exception("GCP_PROJECT_ID not found in configuration")
        
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        
        client = bigquery.Client(project=project_id)
        
        # Test the connection
        try:
            list(client.list_datasets(max_results=1))
            st.success(f"✅ Connected to BigQuery project: {project_id}")
        except Exception as e:
            st.error(f"❌ BigQuery connection test failed: {e}")
            return None, None
        
        return client, project_id
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {e}")
        return None, None

@st.cache_data
def load_data_from_bigquery():
    """Load data from BigQuery with caching - using cache_data for data"""
    try:
        client, project_id = get_bigquery_client()
        if not client or not project_id:
            raise Exception("BigQuery client not initialized")
        
        dataset_id = "customer_behavior"
        
        # Define queries
        queries = {
            'touchpoint_analysis': f"""
            SELECT * FROM `{project_id}.{dataset_id}.touchpoint_analysis`
            """,
            'customer_segments_summary': f"""
            SELECT 
                customer_segment,
                COUNT(*) as customer_count,
                AVG(total_revenue) as avg_revenue,
                AVG(total_interactions) as avg_interactions
            FROM `{project_id}.{dataset_id}.customer_segments`
            GROUP BY customer_segment
            ORDER BY avg_revenue DESC
            """,
            'time_series_data': f"""
            SELECT * FROM `{project_id}.{dataset_id}.time_series_data`
            ORDER BY date
            """,
            'journey_summary': f"""
            SELECT 
                first_touchpoint,
                last_touchpoint,
                COUNT(*) as journey_count,
                AVG(total_revenue) as avg_revenue,
                AVG(journey_duration_days) as avg_duration
            FROM `{project_id}.{dataset_id}.customer_journeys`
            GROUP BY first_touchpoint, last_touchpoint
            ORDER BY journey_count DESC
            LIMIT 10
            """,
            'overview_metrics': f"""
            SELECT 
                COUNT(DISTINCT customer_id) as total_customers,
                SUM(total_revenue) as total_revenue,
                SUM(total_interactions) as total_interactions,
                AVG(total_revenue) as avg_customer_value
            FROM `{project_id}.{dataset_id}.customer_segments`
            """
        }
        
        data = {}
        for name, query in queries.items():
            try:
                st.info(f"Loading {name}...")
                result = client.query(query)
                data[name] = result.to_dataframe()
                st.success(f"✅ Loaded {name}: {len(data[name])} rows")
            except Exception as e:
                st.warning(f"⚠️ Error loading {name}: {e}")
                data[name] = pd.DataFrame()
        
        if all(df.empty for df in data.values()):
            st.error("❌ No data loaded from any table")
            return {}
        
        return data
    except Exception as e:
        st.error(f"❌ Error loading data from BigQuery: {e}")
        return {}

# Load AI insights system
@st.cache_resource
def load_ai_insights():
    """Load AI insights system - using cache_resource for AI system objects"""
    try:
        from rag_system.local_insights_ai import LocalCustomerInsightsAI
        return LocalCustomerInsightsAI()
    except Exception as e:
        st.warning(f"⚠️ AI insights not available: {e}")
        return None

def show_ai_insights(ai_system):
    """Show AI-powered insights"""
    st.header("🤖 AI-Powered Customer Insights")
    
    if not ai_system:
        st.warning("AI insights system not available. Running in basic mode.")
        return
    
    # Question input
    st.subheader("💬 Ask Questions About Your Data")
    
    # Predefined questions
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🎯 Best Performing Touchpoints"):
            st.session_state.ai_question = "What is the best performing touchpoint?"
        
        if st.button("👥 Most Valuable Segments"):
            st.session_state.ai_question = "Which customer segment is most valuable?"
    
    with col2:
        if st.button("🛤️ Journey Patterns"):
            st.session_state.ai_question = "What are the customer journey patterns?"
        
        if st.button("💡 Strategic Recommendations"):
            st.session_state.ai_question = "What recommendations do you have?"
    
    # Custom question input
    custom_question = st.text_input("Or ask your own question:", placeholder="e.g., How can we improve conversion rates?")
    
    # Determine which question to answer
    question_to_ask = None
    if custom_question:
        question_to_ask = custom_question
    elif hasattr(st.session_state, 'ai_question'):
        question_to_ask = st.session_state.ai_question
    
    # Display answer
    if question_to_ask:
        with st.spinner("🧠 Analyzing your data..."):
            result = ai_system.ask_question(question_to_ask)
            
            st.subheader(f"❓ {result['question']}")
            st.markdown(result['answer'])
    
    # Comprehensive insights
    st.markdown("---")
    st.subheader("📊 Comprehensive Insights Summary")
    
    if st.button("🚀 Generate Full Analysis"):
        with st.spinner("🔍 Generating comprehensive insights..."):
            summary = ai_system.get_insights_summary()
            st.markdown(summary)

def main():
    st.title("🎯 Customer Behavior Analytics Dashboard")
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading data from BigQuery..."):
        try:
            data = load_data_from_bigquery()
            if data and any(not df.empty for df in data.values()):
                st.success("✅ Data loaded successfully!")
            else:
                st.error("❌ No data loaded - check your BigQuery connection and data")
                st.info("💡 Make sure your BigQuery tables contain data. Run the data pipeline first if needed.")
                st.stop()
        except Exception as e:
            st.error(f"❌ Error loading data: {e}")
            st.stop()
    
    # Load AI system
    with st.spinner("🤖 Initializing AI insights..."):
        try:
            ai_system = load_ai_insights()
            if ai_system:
                st.success("🧠 AI insights ready!")
        except Exception as e:
            st.warning(f"⚠️ AI system not available: {e}")
            ai_system = None
    
    # Sidebar for navigation
    st.sidebar.title("📋 Navigation")
    page = st.sidebar.selectbox(
        "Choose a view:",
        ["Overview", "Touchpoint Analysis", "Customer Segments", "Journey Analysis", "Time Trends", "🤖 AI Insights"]
    )
    
    if page == "Overview":
        show_overview(data)
    elif page == "Touchpoint Analysis":
        show_touchpoint_analysis(data)
    elif page == "Customer Segments":
        show_customer_segments(data)
    elif page == "Journey Analysis":
        show_journey_analysis(data)
    elif page == "Time Trends":
        show_time_trends(data)
    elif page == "🤖 AI Insights":
        show_ai_insights(ai_system)

def show_overview(data):
    st.header("📈 Overview Dashboard")
    
    # Key metrics
    if 'overview_metrics' in data and not data['overview_metrics'].empty:
        metrics = data['overview_metrics'].iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Customers", f"{int(metrics['total_customers']):,}")
        
        with col2:
            st.metric("Total Revenue", f"${metrics['total_revenue']:,.2f}")
        
        with col3:
            st.metric("Total Interactions", f"{int(metrics['total_interactions']):,}")
        
        with col4:
            st.metric("Avg Customer Value", f"${metrics['avg_customer_value']:.2f}")
    else:
        st.warning("⚠️ Overview metrics not available")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        if 'touchpoint_analysis' in data and not data['touchpoint_analysis'].empty:
            st.subheader("Revenue by Touchpoint")
            fig = px.bar(
                data['touchpoint_analysis'],
                x='touchpoint',
                y='total_revenue',
                title="Revenue by Touchpoint",
                color='total_revenue',
                color_continuous_scale='viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("⚠️ Touchpoint analysis data not available")
    
    with col2:
        if 'customer_segments_summary' in data and not data['customer_segments_summary'].empty:
            st.subheader("Customer Segment Distribution")
            fig = px.pie(
                data['customer_segments_summary'],
                values='customer_count',
                names='customer_segment',
                title="Customer Segments"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("⚠️ Customer segments data not available")

def show_touchpoint_analysis(data):
    st.header("🎯 Touchpoint Performance Analysis")
    
    if 'touchpoint_analysis' in data and not data['touchpoint_analysis'].empty:
        touchpoint_data = data['touchpoint_analysis']
        
        # Metrics table
        st.subheader("Touchpoint Metrics")
        st.dataframe(touchpoint_data, use_container_width=True)
        
        # Visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Conversion Rate by Touchpoint")
            fig = px.bar(
                touchpoint_data,
                x='touchpoint',
                y='conversion_rate',
                title="Conversion Rate (%)",
                color='conversion_rate',
                color_continuous_scale='viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Revenue vs Interactions")
            fig = px.scatter(
                touchpoint_data,
                x='total_interactions',
                y='total_revenue',
                size='unique_customers',
                hover_name='touchpoint',
                title="Revenue vs Interactions (Size = Unique Customers)"
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ Touchpoint analysis data not available")

def show_customer_segments(data):
    st.header("👥 Customer Segmentation Analysis")
    
    if 'customer_segments_summary' in data and not data['customer_segments_summary'].empty:
        segments_data = data['customer_segments_summary']
        
        # Display the data
        st.subheader("Segment Performance")
        st.dataframe(segments_data, use_container_width=True)
        
        # Visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Average Revenue by Segment")
            fig = px.bar(
                segments_data,
                x='customer_segment',
                y='avg_revenue',
                title="Average Revenue by Customer Segment",
                color='avg_revenue',
                color_continuous_scale='blues'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Customer Count by Segment")
            fig = px.bar(
                segments_data,
                x='customer_segment',
                y='customer_count',
                title="Number of Customers by Segment",
                color='customer_count',
                color_continuous_scale='greens'
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ Customer segments data not available")

def show_journey_analysis(data):
    st.header("🛤️ Customer Journey Analysis")
    
    if 'journey_summary' in data and not data['journey_summary'].empty:
        journey_data = data['journey_summary']
        
        st.subheader("Top Journey Paths")
        st.dataframe(journey_data, use_container_width=True)
        
        # Visualization
        st.subheader("Journey Performance")
        fig = px.scatter(
            journey_data,
            x='avg_duration',
            y='avg_revenue',
            size='journey_count',
            hover_data=['first_touchpoint', 'last_touchpoint'],
            title="Journey Duration vs Revenue (Size = Journey Count)"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ Journey analysis data not available")

def show_time_trends(data):
    st.header("📅 Time Series Analysis")
    
    if 'time_series_data' in data and not data['time_series_data'].empty:
        time_data = data['time_series_data']
        time_data['date'] = pd.to_datetime(time_data['date'])
        
        # Time series charts
        st.subheader("Daily Trends")
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.line(
                time_data,
                x='date',
                y='daily_revenue',
                title="Daily Revenue Trend"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.line(
                time_data,
                x='date',
                y='daily_interactions',
                title="Daily Interactions Trend"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Touchpoint trends
        st.subheader("Touchpoint Trends Over Time")
        touchpoint_cols = [col for col in time_data.columns if col.startswith('daily_') and col.endswith('_interactions')]
        
        if touchpoint_cols:
            fig = go.Figure()
            for col in touchpoint_cols:
                touchpoint_name = col.replace('daily_', '').replace('_interactions', '')
                fig.add_trace(go.Scatter(
                    x=time_data['date'],
                    y=time_data[col],
                    mode='lines',
                    name=touchpoint_name
                ))
            
            fig.update_layout(title="Daily Interactions by Touchpoint")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("💡 Touchpoint trend data not available")
    else:
        st.warning("⚠️ Time series data not available")

if __name__ == "__main__":
    main()