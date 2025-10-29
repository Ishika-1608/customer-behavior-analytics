import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime, timedelta
import sys
import os
import json

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from real_time.streaming_simulator import RealTimeCustomerSimulator
from real_time.streaming_processor import EnhancedRealTimeDataProcessor
from external_apis.api_manager import ExternalAPIManager

st.set_page_config(
    page_title="Enhanced Real-Time Analytics",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'enhanced_simulator' not in st.session_state:
    st.session_state.enhanced_simulator = RealTimeCustomerSimulator()
    st.session_state.enhanced_processor = EnhancedRealTimeDataProcessor()
    st.session_state.api_manager = ExternalAPIManager()
    st.session_state.is_enhanced_streaming = False

def start_enhanced_streaming():
    """Start enhanced real-time streaming"""
    if not st.session_state.is_enhanced_streaming:
        st.session_state.enhanced_simulator.start_streaming()
        st.session_state.is_enhanced_streaming = True

def stop_enhanced_streaming():
    """Stop enhanced real-time streaming"""
    if st.session_state.is_enhanced_streaming:
        st.session_state.enhanced_simulator.stop_streaming()
        st.session_state.is_enhanced_streaming = False

def process_enhanced_interactions():
    """Process new interactions with external data enrichment"""
    new_interactions = st.session_state.enhanced_simulator.get_recent_interactions(50)
    if new_interactions:
        st.session_state.enhanced_processor.add_interactions_batch(new_interactions)
    return len(new_interactions)

def show_external_data_panel():
    """Show current external data"""
    st.subheader("🌐 Live External Data")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**🌤️ Weather Data**")
        weather = st.session_state.api_manager.get_weather_data("New York")
        st.metric("Temperature", f"{weather['temperature']}°C")
        st.metric("Condition", weather['weather_condition'])
        st.metric("Humidity", f"{weather['humidity']}%")
    
    with col2:
        st.markdown("**📈 Market Data**")
        stocks = st.session_state.api_manager.get_stock_market_data("SPY")
        st.metric("S&P 500", f"${stocks['price']:.2f}", f"{stocks['change']:.2f}")
        st.metric("Market Sentiment", stocks['market_sentiment'].title())
        st.metric("Volume", f"{stocks['volume']:,}")
    
    with col3:
        st.markdown("**📰 News Sentiment**")
        news = st.session_state.api_manager.get_news_sentiment("retail shopping")
        st.metric("Sentiment", news['sentiment_label'].title())
        st.metric("Score", f"{news['sentiment_score']:.2f}")
        st.metric("Articles", news['articles_analyzed'])

def show_external_insights(dashboard_data):
    """Show insights from external data correlation"""
    if 'external_insights' not in dashboard_data:
        st.info("External insights will appear as data accumulates...")
        return
    
    insights = dashboard_data['external_insights']
    
    st.subheader("🔍 External Factor Analysis")
    
    # Weather impact
    if 'weather' in insights:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🌤️ Weather Impact on Sales**")
            weather_data = insights['weather']['performance_by_condition']
            
            if weather_data:
                weather_df = pd.DataFrame([
                    {'condition': k, 'avg_revenue': v['avg_revenue'], 'interactions': v['interaction_count']}
                    for k, v in weather_data.items()
                ])
                
                fig = px.bar(
                    weather_df, 
                    x='condition', 
                    y='avg_revenue',
                    title="Average Revenue by Weather Condition",
                    color='avg_revenue',
                    color_continuous_scale='viridis'
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.success(f"🏆 Best weather for sales: **{insights['weather']['best_condition']}** (${insights['weather']['best_avg_revenue']:.2f} avg)")
        
        with col2:
            st.markdown("**📊 Weather Distribution**")
            if weather_data:
                fig = px.pie(
                    weather_df,
                    values='interactions',
                    names='condition',
                    title="Interactions by Weather Condition"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    # Market correlation
    if 'market' in insights:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**📈 Market Sentiment Correlation**")
            correlation_score = insights['market']['correlation_score']
            interpretation = insights['market']['interpretation']
            
            # Create gauge chart for correlation
            fig = go.Figure(go.Indicator(
                mode = "gauge+number+delta",
                value = correlation_score,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Market Correlation Score"},
                delta = {'reference': 5},
                gauge = {
                    'axis': {'range': [None, 10]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 2], 'color': "lightgray"},
                        {'range': [2, 5], 'color': "yellow"},
                        {'range': [5, 10], 'color': "green"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 7
                    }
                }
            ))
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
            
            st.info(f"📊 **Interpretation:** {interpretation}")
        
        with col2:
            st.markdown("**💡 Market Insights**")
            current_market = st.session_state.api_manager.get_stock_market_data()
            
            if float(current_market['change_percent']) > 0:
                st.success("📈 Market is up - expect higher conversion rates!")
            else:
                st.warning("📉 Market is down - customers may be more cautious")
            
            st.metric("Current Market Change", f"{current_market['change_percent']}%")
            
            # Recommendations based on market
            if float(current_market['change_percent']) > 2:
                st.success("💡 **Recommendation:** Promote premium products - customers feeling confident!")
            elif float(current_market['change_percent']) < -2:
                st.info("💡 **Recommendation:** Focus on value deals and discounts")
    
    # News sentiment impact
    if 'sentiment' in insights:
        st.markdown("**📰 News Sentiment Impact**")
        sentiment_data = insights['sentiment']['performance_by_sentiment']
        
        if sentiment_data:
            sentiment_df = pd.DataFrame([
                {
                    'sentiment': k, 
                    'conversion_rate': v['conversion_rate'], 
                    'avg_revenue': v['avg_revenue'],
                    'count': v['count']
                }
                for k, v in sentiment_data.items()
            ])
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    sentiment_df,
                    x='sentiment',
                    y='conversion_rate',
                    title="Conversion Rate by News Sentiment",
                    color='conversion_rate',
                    color_continuous_scale='RdYlGn'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(
                    sentiment_df,
                    x='sentiment',
                    y='avg_revenue',
                    title="Average Revenue by News Sentiment",
                    color='avg_revenue',
                    color_continuous_scale='Blues'
                )
                st.plotly_chart(fig, use_container_width=True)

def show_enriched_interactions(dashboard_data):
    """Show sample of enriched interactions"""
    if 'enriched_interactions' not in dashboard_data or not dashboard_data['enriched_interactions']:
        st.info("Enriched interactions will appear as external data is integrated...")
        return
    
    st.subheader("🔗 Live Enriched Interactions")
    
    enriched_df = pd.DataFrame(dashboard_data['enriched_interactions'])
    
    # Show recent enriched interactions in a nice table
    display_columns = [
        'interaction_id', 'touchpoint', 'revenue', 'weather_condition', 
        'weather_temperature', 'market_sentiment', 'news_sentiment'
    ]
    
    available_columns = [col for col in display_columns if col in enriched_df.columns]
    
    if available_columns:
        st.dataframe(
            enriched_df[available_columns].tail(10),
            use_container_width=True
        )
        
        # Show correlation analysis
        if len(enriched_df) > 10:
            st.subheader("📊 Real-Time Correlation Analysis")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Revenue vs Temperature correlation
                if 'weather_temperature' in enriched_df.columns and 'revenue' in enriched_df.columns:
                    fig = px.scatter(
                        enriched_df.tail(100),
                        x='weather_temperature',
                        y='revenue',
                        color='weather_condition',
                        title="Revenue vs Temperature",
                        trendline="ols"
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Market sentiment vs conversion
                if 'market_sentiment' in enriched_df.columns:
                    conversion_by_market = enriched_df.groupby('market_sentiment').agg({
                        'revenue': lambda x: (x > 0).mean() * 100
                    }).reset_index()
                    conversion_by_market.columns = ['market_sentiment', 'conversion_rate']
                    
                    fig = px.bar(
                        conversion_by_market,
                        x='market_sentiment',
                        y='conversion_rate',
                        title="Conversion Rate by Market Sentiment",
                        color='conversion_rate',
                        color_continuous_scale='RdYlGn'
                    )
                    st.plotly_chart(fig, use_container_width=True)

def main():
    st.title("🌐 Enhanced Real-Time Customer Analytics")
    st.markdown("*Powered by External Data Integration*")
    st.markdown("---")
    
    # Control panel
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🚀 Start Enhanced Streaming", disabled=st.session_state.is_enhanced_streaming):
            start_enhanced_streaming()
            st.success("Enhanced streaming started!")
            st.rerun()
    
    with col2:
        if st.button("⏹️ Stop Streaming", disabled=not st.session_state.is_enhanced_streaming):
            stop_enhanced_streaming()
            st.success("Streaming stopped!")
            st.rerun()
    
    with col3:
        streaming_status = "🟢 LIVE + ENRICHED" if st.session_state.is_enhanced_streaming else "🔴 STOPPED"
        st.metric("Status", streaming_status)
    
    with col4:
        auto_refresh = st.checkbox("Auto Refresh (10s)", value=True)
    
    # Show external data panel
    show_external_data_panel()
    
    st.markdown("---")
    
    # Process new data
    new_interactions_count = process_enhanced_interactions()
    
    # Get enhanced dashboard data
    dashboard_data = st.session_state.enhanced_processor.get_enhanced_dashboard_data()
    
    # Enhanced metrics with external context
    st.subheader("📊 Enhanced Live Metrics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "Interactions Today", 
            dashboard_data['current_metrics']['total_interactions_today'],
            delta=new_interactions_count if new_interactions_count > 0 else None
        )
    
    with col2:
        st.metric(
            "Revenue Today", 
            f"${dashboard_data['current_metrics']['total_revenue_today']:.2f}"
        )
    
    with col3:
        st.metric(
            "Active Customers", 
            len(dashboard_data['current_metrics']['active_customers'])
        )
    
    with col4:
        st.metric(
            "Conversion Rate (5min)", 
            f"{dashboard_data['current_metrics']['conversion_rate_window']:.1f}%"
        )
    
    with col5:
        # Show weather-adjusted conversion rate
        weather = st.session_state.api_manager.get_weather_data()
        weather_emoji = "☀️" if weather['weather_condition'] == "Clear" else "☁️" if weather['weather_condition'] == "Clouds" else "🌧️"
        st.metric(
            f"Weather {weather_emoji}", 
            f"{weather['temperature']:.1f}°C"
        )
    
    # Show external insights
    show_external_insights(dashboard_data)
    
    st.markdown("---")
    
    # Show enriched interactions
    show_enriched_interactions(dashboard_data)
    
    # Traditional real-time charts (from original dashboard)
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Interactions Trend (Last Hour)")
        if dashboard_data['minute_trends']:
            trends_df = pd.DataFrame(dashboard_data['minute_trends'])
            trends_df['minute'] = pd.to_datetime(trends_df['minute'])
            
            fig = px.line(
                trends_df, 
                x='minute', 
                y='interaction_id',
                title="Interactions per Minute"
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet. Start streaming to see real-time trends!")
    
    with col2:
        st.subheader("💰 Revenue Trend (Last Hour)")
        if dashboard_data['minute_trends']:
            fig = px.line(
                trends_df, 
                x='minute', 
                y='revenue',
                title="Revenue per Minute"
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data yet. Start streaming to see real-time revenue!")
    
    # System stats with API status
    with st.expander("🔧 System & API Status"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Streaming Stats**")
            stats = st.session_state.enhanced_simulator.get_streaming_stats()
            st.json({
                'queue_size': stats['queue_size'],
                'interactions_per_minute': stats['interactions_per_minute'],
                'enrichment_queue': len(st.session_state.enhanced_processor.enrichment_queue),
                'enriched_interactions': len(st.session_state.enhanced_processor.enriched_interactions)
            })
        
        with col2:
            st.markdown("**API Cache Status**")
            cache_info = {}
            for key in st.session_state.api_manager.cache:
                cache_info[key] = {
                    'cached_at': datetime.fromtimestamp(st.session_state.api_manager.cache[key]['timestamp']).strftime('%H:%M:%S'),
                    'age_seconds': int(time.time() - st.session_state.api_manager.cache[key]['timestamp'])
                }
            st.json(cache_info)
    
    # Auto-refresh
    if auto_refresh and st.session_state.is_enhanced_streaming:
        time.sleep(10)
        st.rerun()

if __name__ == "__main__":
    main()