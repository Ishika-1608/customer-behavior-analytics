import os
import pandas as pd
import json
from typing import Dict, Any

class LocalCustomerInsightsAI:
    def __init__(self):
        # Load customer data
        self.load_customer_data()
        self.create_data_summary()
        self.create_insight_templates()
    
    def load_customer_data(self):
        """Load processed customer data from CSV files"""
        data_path = "data/processed/"
        
        self.data = {}
        files_to_load = {
            'touchpoint_analysis': 'touchpoint_analysis_clean.csv',
            'customer_segments': 'customer_segments_clean.csv', 
            'customer_journeys': 'customer_journeys_clean.csv',
            'time_series': 'time_series_data_clean.csv'
        }
        
        for key, filename in files_to_load.items():
            filepath = os.path.join(data_path, filename)
            if os.path.exists(filepath):
                self.data[key] = pd.read_csv(filepath)
                print(f"✅ Loaded {key}: {len(self.data[key])} rows")
            else:
                # Try without _clean suffix
                alt_filepath = os.path.join(data_path, filename.replace('_clean', ''))
                if os.path.exists(alt_filepath):
                    self.data[key] = pd.read_csv(alt_filepath)
                    print(f"✅ Loaded {key}: {len(self.data[key])} rows")
                else:
                    print(f"⚠️ Warning: {filename} not found")
                    self.data[key] = pd.DataFrame()
    
    def create_data_summary(self):
        """Create a comprehensive data summary"""
        summary = {}
        
        # Touchpoint Analysis Summary
        if not self.data['touchpoint_analysis'].empty:
            tp_data = self.data['touchpoint_analysis']
            summary['touchpoints'] = {
                'best_revenue': tp_data.loc[tp_data['total_revenue'].idxmax()],
                'best_conversion': tp_data.loc[tp_data['conversion_rate'].idxmax()],
                'most_interactions': tp_data.loc[tp_data['total_interactions'].idxmax()],
                'all_data': tp_data
            }
        
        # Customer Segments Summary
        if not self.data['customer_segments'].empty:
            seg_data = self.data['customer_segments']
            segment_summary = seg_data.groupby('customer_segment').agg({
                'total_revenue': ['count', 'mean', 'sum'],
                'total_interactions': 'mean',
                'unique_touchpoints_used': 'mean'
            }).round(2)
            
            summary['segments'] = {
                'summary_table': segment_summary,
                'best_segment': segment_summary.loc[segment_summary[('total_revenue', 'sum')].idxmax()],
                'most_customers': segment_summary.loc[segment_summary[('total_revenue', 'count')].idxmax()]
            }
        
        # Journey Analysis Summary
        if not self.data['customer_journeys'].empty:
            journey_data = self.data['customer_journeys']
            summary['journeys'] = {
                'avg_length': journey_data['total_interactions'].mean(),
                'avg_duration': journey_data['journey_duration_days'].mean(),
                'conversion_rate': journey_data['conversion_occurred'].sum() / len(journey_data) * 100,
                'top_first_touchpoints': journey_data['first_touchpoint'].value_counts().head(3),
                'top_last_touchpoints': journey_data['last_touchpoint'].value_counts().head(3),
                'high_value_journeys': journey_data[journey_data['total_revenue'] > journey_data['total_revenue'].quantile(0.8)]
            }
        
        # Time Series Summary
        if not self.data['time_series'].empty:
            ts_data = self.data['time_series']
            summary['time_trends'] = {
                'total_revenue': ts_data['daily_revenue'].sum(),
                'avg_daily_revenue': ts_data['daily_revenue'].mean(),
                'peak_day': ts_data.loc[ts_data['daily_revenue'].idxmax()],
                'trend': 'increasing' if ts_data['daily_revenue'].iloc[-5:].mean() > ts_data['daily_revenue'].iloc[:5].mean() else 'decreasing'
            }
        
        self.summary = summary
        print("✅ Data summary created")
    
    def create_insight_templates(self):
        """Create templates for different types of insights"""
        self.templates = {
            'touchpoint_performance': self._analyze_touchpoint_performance,
            'customer_segments': self._analyze_customer_segments,
            'journey_patterns': self._analyze_journey_patterns,
            'recommendations': self._generate_recommendations,
            'revenue_trends': self._analyze_revenue_trends
        }
    
    def _analyze_touchpoint_performance(self):
        """Analyze touchpoint performance"""
        if 'touchpoints' not in self.summary:
            return "No touchpoint data available."
        
        best_revenue = self.summary['touchpoints']['best_revenue']
        best_conversion = self.summary['touchpoints']['best_conversion']
        
        analysis = f"""
        🎯 **Touchpoint Performance Analysis:**
        
        **Best Revenue Generator:** {best_revenue['touchpoint']}
        - Total Revenue: ${best_revenue['total_revenue']:,.2f}
        - Total Interactions: {best_revenue['total_interactions']:,}
        - Conversion Rate: {best_revenue['conversion_rate']:.1f}%
        
        **Highest Conversion Rate:** {best_conversion['touchpoint']}
        - Conversion Rate: {best_conversion['conversion_rate']:.1f}%
        - Revenue per Interaction: ${best_conversion['avg_revenue_per_interaction']:.2f}
        
        **Key Insights:**
        - {best_revenue['touchpoint']} is your revenue powerhouse
        - {best_conversion['touchpoint']} has the most efficient conversion process
        - Focus optimization efforts on these high-performing channels
        """
        
        return analysis
    
    def _analyze_customer_segments(self):
        """Analyze customer segments"""
        if 'segments' not in self.summary:
            return "No customer segment data available."
        
        best_segment = self.summary['segments']['best_segment']
        segment_name = best_segment.name
        
        analysis = f"""
        👥 **Customer Segment Analysis:**
        
        **Most Valuable Segment:** {segment_name}
        - Total Revenue: ${best_segment[('total_revenue', 'sum')]:,.2f}
        - Customer Count: {int(best_segment[('total_revenue', 'count')]):,}
        - Average Revenue per Customer: ${best_segment[('total_revenue', 'mean')]:,.2f}
        - Average Interactions: {best_segment[('total_interactions', 'mean')]:,.1f}
        
        **Strategic Recommendations:**
        - Prioritize retention strategies for {segment_name} customers
        - Develop targeted campaigns to convert other segments to {segment_name}
        - Analyze what makes {segment_name} customers more valuable
        """
        
        return analysis
    
    def _analyze_journey_patterns(self):
        """Analyze customer journey patterns"""
        if 'journeys' not in self.summary:
            return "No journey data available."
        
        journeys = self.summary['journeys']
        top_first = journeys['top_first_touchpoints'].index[0]
        top_last = journeys['top_last_touchpoints'].index[0]
        
        analysis = f"""
        🛤️ **Customer Journey Analysis:**
        
        **Journey Characteristics:**
        - Average Journey Length: {journeys['avg_length']:.1f} interactions
        - Average Duration: {journeys['avg_duration']:.1f} days
        - Overall Conversion Rate: {journeys['conversion_rate']:.1f}%
        
        **Common Journey Patterns:**
        - Most Common First Touchpoint: {top_first} ({journeys['top_first_touchpoints'].iloc[0]} customers)
        - Most Common Last Touchpoint: {top_last} ({journeys['top_last_touchpoints'].iloc[0]} customers)
        
        **Optimization Opportunities:**
        - Optimize the {top_first} → {top_last} journey path
        - Reduce average journey duration to improve conversion speed
        - Focus on converting more {journeys['conversion_rate']:.1f}% of journeys
        """
        
        return analysis
    
    def _analyze_revenue_trends(self):
        """Analyze revenue trends"""
        if 'time_trends' not in self.summary:
            return "No time series data available."
        
        trends = self.summary['time_trends']
        peak_day = trends['peak_day']
        
        analysis = f"""
        📈 **Revenue Trends Analysis:**
        
        **Overall Performance:**
        - Total Revenue: ${trends['total_revenue']:,.2f}
        - Average Daily Revenue: ${trends['avg_daily_revenue']:,.2f}
        - Revenue Trend: {trends['trend'].title()}
        
        **Peak Performance:**
        - Best Day: {peak_day['date']}
        - Peak Revenue: ${peak_day['daily_revenue']:,.2f}
        - Peak Interactions: {peak_day['daily_interactions']:,}
        
        **Strategic Insights:**
        - Revenue is trending {trends['trend']}
        - Analyze what made {peak_day['date']} successful
        - Replicate peak day strategies for consistent growth
        """
        
        return analysis
    
    def _generate_recommendations(self):
        """Generate actionable recommendations"""
        recommendations = """
        💡 **Strategic Recommendations:**
        
        **Immediate Actions:**
        1. **Optimize High-Performing Touchpoints:** Double down on your best revenue-generating channels
        2. **Improve Conversion Funnels:** Focus on touchpoints with high traffic but low conversion
        3. **Segment-Specific Campaigns:** Create targeted campaigns for your most valuable customer segments
        
        **Medium-Term Strategies:**
        1. **Journey Optimization:** Streamline customer journeys to reduce friction and time-to-conversion
        2. **Cross-Channel Integration:** Ensure consistent experience across all touchpoints
        3. **Predictive Analytics:** Implement models to identify high-value prospects early
        
        **Long-Term Growth:**
        1. **Customer Lifetime Value:** Focus on retention and expansion of high-value segments
        2. **New Channel Exploration:** Test new touchpoints based on customer preferences
        3. **Personalization:** Implement AI-driven personalization across all customer interactions
        """
        
        return recommendations
    
    def ask_question(self, question: str) -> Dict[str, Any]:
        """Answer questions about customer behavior data"""
        question_lower = question.lower()
        
        # Simple keyword matching to determine question type
        if any(word in question_lower for word in ['touchpoint', 'channel', 'performance']):
            answer = self._analyze_touchpoint_performance()
        elif any(word in question_lower for word in ['segment', 'customer', 'valuable']):
            answer = self._analyze_customer_segments()
        elif any(word in question_lower for word in ['journey', 'path', 'conversion']):
            answer = self._analyze_journey_patterns()
        elif any(word in question_lower for word in ['trend', 'revenue', 'time']):
            answer = self._analyze_revenue_trends()
        elif any(word in question_lower for word in ['recommend', 'improve', 'strategy']):
            answer = self._generate_recommendations()
        else:
            # Default comprehensive answer
            answer = f"""
            {self._analyze_touchpoint_performance()}
            
            {self._analyze_customer_segments()}
            
            {self._analyze_journey_patterns()}
            """
        
        return {
            "question": question,
            "answer": answer,
            "success": True
        }
    
    def get_insights_summary(self) -> str:
        """Generate a comprehensive insights summary"""
        summary = "# 🎯 Customer Behavior Insights Dashboard\n\n"
        
        summary += self._analyze_touchpoint_performance() + "\n\n"
        summary += self._analyze_customer_segments() + "\n\n" 
        summary += self._analyze_journey_patterns() + "\n\n"
        summary += self._analyze_revenue_trends() + "\n\n"
        summary += self._generate_recommendations() + "\n\n"
        
        return summary

# Example usage and testing
if __name__ == "__main__":
    print("=== Initializing Local Customer Insights AI ===")
    ai = LocalCustomerInsightsAI()
    
    # Test questions
    test_questions = [
        "What is the best performing touchpoint?",
        "Which customer segment is most valuable?",
        "What are the customer journey patterns?",
        "How are our revenue trends?",
        "What recommendations do you have?"
    ]
    
    print("\n=== Testing Local Customer Insights AI ===\n")
    
    for question in test_questions:
        print(f"❓ Question: {question}")
        result = ai.ask_question(question)
        print(f"💡 Answer: {result['answer']}\n")
        print("-" * 100 + "\n")
    
    # Generate comprehensive summary
    print("=== Comprehensive Insights Summary ===")
    summary = ai.get_insights_summary()
    print(summary)