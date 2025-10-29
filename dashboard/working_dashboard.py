import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import sys
from dotenv import load_dotenv

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from rag_system.local_insights_ai import LocalCustomerInsightsAI
from data_warehouse.bigquery_manager import BigQueryManager

# Load environment variables
load_dotenv()

# Configure Streamlit page
st.set_page_config(
    page_title="Customer Behavior Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data_from_bigquery():
    """Load data from BigQuery with caching"""
    bq_manager = BigQueryManager(
        project_id=os.getenv('GCP_PROJECT_ID'),
        credentials_path=os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    )
    
    # Load all datasets
    queries = {
        'touchpoint_analysis': f"""
        SELECT * FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.touchpoint_analysis`
        """,
        'customer_segments': f"""
        SELECT * FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.customer_segments`
        """,
        'time_series_data': f"""
        SELECT * FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.time_series_data`
        ORDER BY date
        """,
        'customer_journeys': f"""
        SELECT * FROM `{bq_manager.project_id}.{bq_manager.dataset_id}.customer_journeys`
        """
    }
    
    data = {}
    for name, query in queries.items():
        data[name] = bq_manager.query_data(query)
    
    return data

def main():
    st.title("🎯 Customer Behavior Analytics Dashboard")
    st.markdown("---")
    
    # Load data
    with st.spinner("Loading data from BigQuery..."):
        try:
            data = load_data_from_bigquery()
            st.success("✅ Data loaded successfully!")
        except Exception as e:
            st.error(f"❌ Error loading data: {e}")
            st.stop()
    
    # Load AI system
    with st.spinner("🤖 Initializing AI insights..."):
        try:
            ai_system = load_ai_insights()
            st.success("🧠 AI insights ready!")
        except Exception as e:
            st.error(f"❌ Error loading AI: {e}")
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
        if ai_system:
            show_ai_insights(ai_system)
        else:
            st.error("AI system not available")

def show_overview(data):
    st.header("📈 Overview Dashboard")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_customers = len(data['customer_segments'])
        st.metric("Total Customers", f"{total_customers:,}")
    
    with col2:
        total_revenue = data['touchpoint_analysis']['total_revenue'].sum()
        st.metric("Total Revenue", f"${total_revenue:,.2f}")
    
    with col3:
        total_interactions = data['touchpoint_analysis']['total_interactions'].sum()
        st.metric("Total Interactions", f"{total_interactions:,}")
    
    with col4:
        avg_conversion = data['touchpoint_analysis']['conversion_rate'].mean()
        st.metric("Avg Conversion Rate", f"{avg_conversion:.1f}%")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Revenue by Touchpoint")
        fig = px.bar(
            data['touchpoint_analysis'],
            x='touchpoint',
            y='total_revenue',
            title="Revenue by Touchpoint"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Customer Segment Distribution")
        segment_counts = data['customer_segments']['customer_segment'].value_counts()
        fig = px.pie(
            values=segment_counts.values,
            names=segment_counts.index,
            title="Customer Segments"
        )
        st.plotly_chart(fig, use_container_width=True)

def show_touchpoint_analysis(data):
    st.header("🎯 Touchpoint Performance Analysis")
    
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

def show_customer_segments(data):
    st.header("👥 Customer Segmentation Analysis")
    
    segments_data = data['customer_segments']
    
    # Segment overview
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Frequency Distribution")
        freq_dist = segments_data['frequency_score'].value_counts()
        fig = px.bar(x=freq_dist.index, y=freq_dist.values, title="Frequency Scores")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Monetary Distribution")
        mon_dist = segments_data['monetary_score'].value_counts()
        fig = px.bar(x=mon_dist.index, y=mon_dist.values, title="Monetary Scores")
        st.plotly_chart(fig, use_container_width=True)
    
    with col3:
        st.subheader("Recency Distribution")
        rec_dist = segments_data['recency_score'].value_counts()
        fig = px.bar(x=rec_dist.index, y=rec_dist.values, title="Recency Scores")
        st.plotly_chart(fig, use_container_width=True)
    
    # Detailed analysis
    st.subheader("Segment Performance")
    segment_summary = segments_data.groupby('customer_segment').agg({
        'total_revenue': ['mean', 'sum'],
        'total_interactions': 'mean',
        'unique_touchpoints_used': 'mean'
    }).round(2)
    
    st.dataframe(segment_summary, use_container_width=True)

def show_journey_analysis(data):
    st.header("🛤️ Customer Journey Analysis")
    
    journey_data = data['customer_journeys']
    
    # Journey metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Journey Length Distribution")
        fig = px.histogram(
            journey_data,
            x='total_interactions',
            title="Distribution of Journey Lengths",
            nbins=20
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Journey Duration vs Revenue")
        fig = px.scatter(
            journey_data,
            x='journey_duration_days',
            y='total_revenue',
            color='conversion_occurred',
            title="Journey Duration vs Revenue"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Top journey paths
    st.subheader("Most Common Journey Paths")
    top_paths = journey_data['journey_path'].value_counts().head(10)
    st.bar_chart(top_paths)

@st.cache_resource
def load_ai_insights():
    """Load AI insights system"""
    return LocalCustomerInsightsAI()

def show_ai_insights(ai_system):
    st.header("🤖 AI-Powered Customer Insights")
    
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

def show_time_trends(data):
    st.header("📅 Time Series Analysis")
    
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

if __name__ == "__main__":
    main()