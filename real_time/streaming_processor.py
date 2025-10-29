import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import threading
from collections import deque
from typing import Dict, List, Any
import json
import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from external_apis.api_manager import ExternalAPIManager

class EnhancedRealTimeDataProcessor:
    def __init__(self, window_size_minutes=5):
        self.window_size = window_size_minutes
        self.interactions_buffer = deque(maxlen=10000)
        self.enriched_interactions = deque(maxlen=5000)
        self.metrics_cache = {}
        self.last_update = datetime.now()
        
        # Initialize API manager
        self.api_manager = ExternalAPIManager()
        
        # Enhanced real-time metrics
        self.current_metrics = {
            'total_interactions_today': 0,
            'total_revenue_today': 0.0,
            'active_customers': set(),
            'touchpoint_counts': {},
            'conversion_rate_window': 0.0,
            'avg_session_duration': 0.0,
            # External data metrics
            'weather_impact': {},
            'market_correlation': 0.0,
            'sentiment_impact': {},
            'economic_factors': {}
        }
        
        # Background enrichment thread
        self.enrichment_queue = deque()
        self.start_enrichment_worker()
    
    def start_enrichment_worker(self):
        """Start background thread for API enrichment"""
        def enrichment_worker():
            while True:
                if self.enrichment_queue:
                    interaction = self.enrichment_queue.popleft()
                    try:
                        enriched = self.api_manager.enrich_interaction(interaction)
                        self.enriched_interactions.append(enriched)
                        self.update_external_metrics(enriched)
                    except Exception as e:
                        print(f"Error enriching interaction: {e}")
                        # Add original interaction if enrichment fails
                        self.enriched_interactions.append(interaction)
                
                time.sleep(1)  # Process one interaction per second
        
        thread = threading.Thread(target=enrichment_worker, daemon=True)
        thread.start()
    
    def add_interaction(self, interaction: Dict):
        """Add new interaction to processing buffer"""
        interaction['processed_timestamp'] = datetime.now()
        self.interactions_buffer.append(interaction)
        
        # Add to enrichment queue
        self.enrichment_queue.append(interaction.copy())
        
        self.update_real_time_metrics(interaction)
    
    def update_external_metrics(self, enriched_interaction: Dict):
        """Update metrics based on enriched external data"""
        # Weather impact analysis
        weather_condition = enriched_interaction.get('weather_condition', 'Unknown')
        revenue = enriched_interaction.get('revenue', 0)
        
        if weather_condition not in self.current_metrics['weather_impact']:
            self.current_metrics['weather_impact'][weather_condition] = {
                'total_revenue': 0,
                'interaction_count': 0,
                'avg_revenue': 0
            }
        
        weather_stats = self.current_metrics['weather_impact'][weather_condition]
        weather_stats['total_revenue'] += revenue
        weather_stats['interaction_count'] += 1
        weather_stats['avg_revenue'] = weather_stats['total_revenue'] / weather_stats['interaction_count']
        
        # Market sentiment correlation
        market_sentiment = enriched_interaction.get('market_sentiment', 'neutral')
        if market_sentiment == 'positive' and revenue > 0:
            self.current_metrics['market_correlation'] += 0.1
        elif market_sentiment == 'negative' and revenue == 0:
            self.current_metrics['market_correlation'] += 0.1
        
        # News sentiment impact
        news_sentiment = enriched_interaction.get('news_sentiment', 'neutral')
        if news_sentiment not in self.current_metrics['sentiment_impact']:
            self.current_metrics['sentiment_impact'][news_sentiment] = {
                'conversion_rate': 0,
                'avg_revenue': 0,
                'count': 0
            }
        
        sentiment_stats = self.current_metrics['sentiment_impact'][news_sentiment]
        sentiment_stats['count'] += 1
        if revenue > 0:
            sentiment_stats['conversion_rate'] = (sentiment_stats['conversion_rate'] * (sentiment_stats['count'] - 1) + 1) / sentiment_stats['count']
        sentiment_stats['avg_revenue'] = (sentiment_stats['avg_revenue'] * (sentiment_stats['count'] - 1) + revenue) / sentiment_stats['count']
    
    def get_enhanced_dashboard_data(self):
        """Get enhanced dashboard data with external insights"""
        base_data = self.get_real_time_dashboard_data()
        
        # Add external data insights
        external_insights = self.analyze_external_factors()
        base_data['external_insights'] = external_insights
        
        # Add enriched interactions sample
        if self.enriched_interactions:
            recent_enriched = list(self.enriched_interactions)[-20:]
            base_data['enriched_interactions'] = recent_enriched
        
        return base_data
    
    def analyze_external_factors(self):
        """Analyze impact of external factors"""
        insights = {}
        
        # Weather impact analysis
        if self.current_metrics['weather_impact']:
            weather_performance = {}
            for condition, stats in self.current_metrics['weather_impact'].items():
                weather_performance[condition] = {
                    'avg_revenue': round(stats['avg_revenue'], 2),
                    'interaction_count': stats['interaction_count']
                }
            
            # Find best weather for sales
            best_weather = max(weather_performance.items(), 
                             key=lambda x: x[1]['avg_revenue'], 
                             default=('Unknown', {'avg_revenue': 0}))
            
            insights['weather'] = {
                'performance_by_condition': weather_performance,
                'best_condition': best_weather[0],
                'best_avg_revenue': best_weather[1]['avg_revenue']
            }
        
        # Market correlation
        insights['market'] = {
            'correlation_score': round(self.current_metrics['market_correlation'], 2),
            'interpretation': 'Strong positive correlation' if self.current_metrics['market_correlation'] > 5 
                           else 'Moderate correlation' if self.current_metrics['market_correlation'] > 2 
                           else 'Weak correlation'
        }
        
        # Sentiment impact
        if self.current_metrics['sentiment_impact']:
            sentiment_performance = {}
            for sentiment, stats in self.current_metrics['sentiment_impact'].items():
                sentiment_performance[sentiment] = {
                    'conversion_rate': round(stats['conversion_rate'] * 100, 1),
                    'avg_revenue': round(stats['avg_revenue'], 2),
                    'count': stats['count']
                }
            
            insights['sentiment'] = {
                'performance_by_sentiment': sentiment_performance
            }
        
        return insights
    
    def get_real_time_dashboard_data(self):
        """Get basic real-time dashboard data"""
        # Recent interactions trend (last hour by minute)
        recent_df = self.get_recent_interactions_df(minutes=60)
        
        if recent_df.empty:
            return self.get_empty_dashboard_data()
        
        # Group by minute for trend
        recent_df['minute'] = pd.to_datetime(recent_df['processed_timestamp']).dt.floor('T')
        minute_trends = recent_df.groupby('minute').agg({
            'interaction_id': 'count',
            'revenue': 'sum',
            'customer_id': 'nunique'
        }).reset_index()
        
        # Touchpoint performance (last 30 minutes)
        touchpoint_df = self.get_recent_interactions_df(minutes=30)
        touchpoint_performance = touchpoint_df.groupby('touchpoint').agg({
            'interaction_id': 'count',
            'revenue': 'sum',
            'customer_id': 'nunique'
        }).reset_index() if not touchpoint_df.empty else pd.DataFrame()
        
        return {
            'current_metrics': self.current_metrics.copy(),
            'minute_trends': minute_trends.to_dict('records'),
            'touchpoint_performance': touchpoint_performance.to_dict('records'),
            'recent_interactions': recent_df.tail(20).to_dict('records'),
            'last_updated': datetime.now().isoformat()
        }
    
    def get_recent_interactions_df(self, minutes=30):
        """Get recent interactions as DataFrame"""
        if not self.interactions_buffer:
            return pd.DataFrame()
        
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        recent_interactions = [
            interaction for interaction in self.interactions_buffer
            if interaction.get('processed_timestamp', datetime.now()) > cutoff_time
        ]
        
        return pd.DataFrame(recent_interactions) if recent_interactions else pd.DataFrame()
    
    def get_empty_dashboard_data(self):
        """Return empty dashboard data structure"""
        return {
            'current_metrics': self.current_metrics.copy(),
            'minute_trends': [],
            'touchpoint_performance': [],
            'recent_interactions': [],
            'external_insights': {},
            'last_updated': datetime.now().isoformat()
        }
    
    # Include all other methods from the original processor...
    def add_interactions_batch(self, interactions: List[Dict]):
        """Add multiple interactions"""
        for interaction in interactions:
            self.add_interaction(interaction)
    
    def update_real_time_metrics(self, interaction: Dict):
        """Update real-time metrics with new interaction"""
        # Update daily totals
        self.current_metrics['total_interactions_today'] += 1
        self.current_metrics['total_revenue_today'] += interaction.get('revenue', 0)
        
        # Track active customers
        self.current_metrics['active_customers'].add(interaction['customer_id'])
        
        # Update touchpoint counts
        touchpoint = interaction['touchpoint']
        if touchpoint not in self.current_metrics['touchpoint_counts']:
            self.current_metrics['touchpoint_counts'][touchpoint] = 0
        self.current_metrics['touchpoint_counts'][touchpoint] += 1
        
        # Update window-based metrics every minute
        if (datetime.now() - self.last_update).seconds >= 60:
            self.calculate_window_metrics()
            self.last_update = datetime.now()
    
    def calculate_window_metrics(self):
        """Calculate metrics for the current time window"""
        if not self.interactions_buffer:
            return
        
        # Get interactions from last window
        cutoff_time = datetime.now() - timedelta(minutes=self.window_size)
        recent_interactions = [
            interaction for interaction in self.interactions_buffer
            if interaction.get('processed_timestamp', datetime.now()) > cutoff_time
        ]
        
        if not recent_interactions:
            return
        
        # Calculate window metrics
        df = pd.DataFrame(recent_interactions)
        
        # Conversion rate (interactions with revenue > 0)
        conversions = len(df[df['revenue'] > 0])
        self.current_metrics['conversion_rate_window'] = (conversions / len(df)) * 100 if len(df) > 0 else 0
        
        # Average session duration
        self.current_metrics['avg_session_duration'] = df['session_duration_minutes'].mean()