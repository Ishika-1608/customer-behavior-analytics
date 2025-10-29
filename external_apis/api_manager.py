import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import time
import asyncio
import aiohttp
from dotenv import load_dotenv

load_dotenv()

class ExternalAPIManager:
    def __init__(self):
        # API Keys (add these to your .env file)
        self.weather_api_key = os.getenv('OPENWEATHER_API_KEY')
        self.news_api_key = os.getenv('NEWS_API_KEY')
        self.alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        
        # Cache for API responses (avoid rate limits)
        self.cache = {}
        self.cache_duration = 300  # 5 minutes
        
        # Rate limiting
        self.last_api_calls = {}
        self.rate_limits = {
            'weather': 1,  # 1 second between calls
            'news': 2,     # 2 seconds between calls
            'stocks': 1,   # 1 second between calls
        }
    
    def is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid"""
        if cache_key not in self.cache:
            return False
        
        cache_time = self.cache[cache_key].get('timestamp', 0)
        return (time.time() - cache_time) < self.cache_duration
    
    def get_cached_or_fetch(self, cache_key: str, fetch_function, *args, **kwargs):
        """Get data from cache or fetch from API"""
        if self.is_cache_valid(cache_key):
            return self.cache[cache_key]['data']
        
        # Fetch new data
        data = fetch_function(*args, **kwargs)
        if data:
            self.cache[cache_key] = {
                'data': data,
                'timestamp': time.time()
            }
        
        return data
    
    def respect_rate_limit(self, api_name: str):
        """Ensure we don't exceed API rate limits"""
        if api_name in self.last_api_calls:
            time_since_last = time.time() - self.last_api_calls[api_name]
            min_interval = self.rate_limits.get(api_name, 1)
            
            if time_since_last < min_interval:
                time.sleep(min_interval - time_since_last)
        
        self.last_api_calls[api_name] = time.time()
    
    def get_weather_data(self, city: str = "New York") -> Dict[str, Any]:
        """Get current weather data"""
        if not self.weather_api_key:
            return self._get_mock_weather_data()
        
        cache_key = f"weather_{city}"
        return self.get_cached_or_fetch(cache_key, self._fetch_weather_data, city)
    
    def _fetch_weather_data(self, city: str) -> Dict[str, Any]:
        """Fetch weather data from OpenWeatherMap API"""
        self.respect_rate_limit('weather')
        
        try:
            url = f"http://api.openweathermap.org/data/2.5/weather"
            params = {
                'q': city,
                'appid': self.weather_api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            return {
                'temperature': data['main']['temp'],
                'humidity': data['main']['humidity'],
                'weather_condition': data['weather'][0]['main'],
                'weather_description': data['weather'][0]['description'],
                'wind_speed': data['wind']['speed'],
                'pressure': data['main']['pressure'],
                'visibility': data.get('visibility', 0) / 1000,  # Convert to km
                'city': city,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Error fetching weather data: {e}")
            return self._get_mock_weather_data()
    
    def _get_mock_weather_data(self) -> Dict[str, Any]:
        """Generate mock weather data when API is not available"""
        import random
        return {
            'temperature': round(random.uniform(15, 30), 1),
            'humidity': random.randint(40, 80),
            'weather_condition': random.choice(['Clear', 'Clouds', 'Rain', 'Snow']),
            'weather_description': 'mock data',
            'wind_speed': round(random.uniform(0, 15), 1),
            'pressure': random.randint(1000, 1020),
            'visibility': round(random.uniform(5, 15), 1),
            'city': 'Mock City',
            'timestamp': datetime.now().isoformat()
        }
    
    def get_stock_market_data(self, symbol: str = "SPY") -> Dict[str, Any]:
        """Get stock market data"""
        if not self.alpha_vantage_key:
            return self._get_mock_stock_data()
        
        cache_key = f"stock_{symbol}"
        return self.get_cached_or_fetch(cache_key, self._fetch_stock_data, symbol)
    
    def _fetch_stock_data(self, symbol: str) -> Dict[str, Any]:
        """Fetch stock data from Alpha Vantage API"""
        self.respect_rate_limit('stocks')
        
        try:
            url = f"https://www.alphavantage.co/query"
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol,
                'apikey': self.alpha_vantage_key
            }
            
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            quote = data.get('Global Quote', {})
            
            return {
                'symbol': symbol,
                'price': float(quote.get('05. price', 0)),
                'change': float(quote.get('09. change', 0)),
                'change_percent': quote.get('10. change percent', '0%').replace('%', ''),
                'volume': int(quote.get('06. volume', 0)),
                'market_sentiment': 'positive' if float(quote.get('09. change', 0)) > 0 else 'negative',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Error fetching stock data: {e}")
            return self._get_mock_stock_data()
    
    def _get_mock_stock_data(self) -> Dict[str, Any]:
        """Generate mock stock data"""
        import random
        change = round(random.uniform(-5, 5), 2)
        return {
            'symbol': 'SPY',
            'price': round(random.uniform(400, 450), 2),
            'change': change,
            'change_percent': f"{(change/420)*100:.2f}",
            'volume': random.randint(50000000, 100000000),
            'market_sentiment': 'positive' if change > 0 else 'negative',
            'timestamp': datetime.now().isoformat()
        }
    
    def get_news_sentiment(self, query: str = "retail shopping") -> Dict[str, Any]:
        """Get news sentiment data"""
        if not self.news_api_key:
            return self._get_mock_news_sentiment()
        
        cache_key = f"news_{query}"
        return self.get_cached_or_fetch(cache_key, self._fetch_news_sentiment, query)
    
    def _fetch_news_sentiment(self, query: str) -> Dict[str, Any]:
        """Fetch news sentiment from News API"""
        self.respect_rate_limit('news')
        
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                'q': query,
                'apiKey': self.news_api_key,
                'language': 'en',
                'sortBy': 'publishedAt',
                'pageSize': 10,
                'from': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            }
            
            response = requests.get(url, params=params, timeout=5)
            response.raise_for_status()
            
            data = response.json()
            articles = data.get('articles', [])
            
            # Simple sentiment analysis based on keywords
            positive_keywords = ['growth', 'increase', 'positive', 'success', 'profit', 'gain']
            negative_keywords = ['decline', 'decrease', 'negative', 'loss', 'drop', 'fall']
            
            sentiment_scores = []
            for article in articles:
                title = article.get('title', '').lower()
                description = article.get('description', '').lower()
                text = f"{title} {description}"
                
                positive_count = sum(1 for word in positive_keywords if word in text)
                negative_count = sum(1 for word in negative_keywords if word in text)
                
                if positive_count > negative_count:
                    sentiment_scores.append(1)
                elif negative_count > positive_count:
                    sentiment_scores.append(-1)
                else:
                    sentiment_scores.append(0)
            
            avg_sentiment = sum(sentiment_scores) / len(sentiment_scores) if sentiment_scores else 0
            
            return {
                'query': query,
                'sentiment_score': round(avg_sentiment, 2),
                'sentiment_label': 'positive' if avg_sentiment > 0 else 'negative' if avg_sentiment < 0 else 'neutral',
                'articles_analyzed': len(articles),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Error fetching news sentiment: {e}")
            return self._get_mock_news_sentiment()
    
    def _get_mock_news_sentiment(self) -> Dict[str, Any]:
        """Generate mock news sentiment"""
        import random
        sentiment_score = round(random.uniform(-1, 1), 2)
        return {
            'query': 'retail shopping',
            'sentiment_score': sentiment_score,
            'sentiment_label': 'positive' if sentiment_score > 0 else 'negative' if sentiment_score < 0 else 'neutral',
            'articles_analyzed': random.randint(5, 15),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_economic_indicators(self) -> Dict[str, Any]:
        """Get economic indicators (mock data for demo)"""
        import random
        return {
            'inflation_rate': round(random.uniform(2, 6), 2),
            'unemployment_rate': round(random.uniform(3, 8), 2),
            'consumer_confidence': round(random.uniform(80, 120), 1),
            'gdp_growth': round(random.uniform(-2, 4), 2),
            'interest_rate': round(random.uniform(0, 5), 2),
            'timestamp': datetime.now().isoformat()
        }
    
    def get_geographic_data(self, city: str) -> Dict[str, Any]:
        """Get geographic and demographic data"""
        # Mock geographic data
        city_data = {
            'New York': {'population': 8400000, 'median_income': 65000, 'timezone': 'EST'},
            'Los Angeles': {'population': 4000000, 'median_income': 62000, 'timezone': 'PST'},
            'Chicago': {'population': 2700000, 'median_income': 58000, 'timezone': 'CST'},
            'Houston': {'population': 2300000, 'median_income': 55000, 'timezone': 'CST'},
        }
        
        default_data = {'population': 1000000, 'median_income': 60000, 'timezone': 'EST'}
        data = city_data.get(city, default_data)
        
        return {
            'city': city,
            'population': data['population'],
            'median_income': data['median_income'],
            'timezone': data['timezone'],
            'timestamp': datetime.now().isoformat()
        }
    
    def enrich_interaction(self, interaction: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich customer interaction with external data"""
        enriched = interaction.copy()
        
        # Get customer's city from interaction or use default
        customer_city = interaction.get('city', 'New York')
        
        # Add weather data
        weather_data = self.get_weather_data(customer_city)
        enriched.update({
            'weather_temperature': weather_data['temperature'],
            'weather_condition': weather_data['weather_condition'],
            'weather_humidity': weather_data['humidity']
        })
        
        # Add market sentiment
        stock_data = self.get_stock_market_data()
        enriched.update({
            'market_sentiment': stock_data['market_sentiment'],
            'market_change_percent': stock_data['change_percent']
        })
        
        # Add news sentiment
        news_sentiment = self.get_news_sentiment()
        enriched.update({
            'news_sentiment': news_sentiment['sentiment_label'],
            'news_sentiment_score': news_sentiment['sentiment_score']
        })
        
        # Add economic indicators
        economic_data = self.get_economic_indicators()
        enriched.update({
            'inflation_rate': economic_data['inflation_rate'],
            'consumer_confidence': economic_data['consumer_confidence']
        })
        
        # Add geographic data
        geo_data = self.get_geographic_data(customer_city)
        enriched.update({
            'city_population': geo_data['population'],
            'city_median_income': geo_data['median_income']
        })
        
        return enriched

if __name__ == "__main__":
    # Test the API manager
    api_manager = ExternalAPIManager()
    
    print("=== Testing External API Integrations ===\n")
    
    # Test weather data
    print("🌤️ Weather Data:")
    weather = api_manager.get_weather_data("New York")
    print(json.dumps(weather, indent=2))
    
    print("\n📈 Stock Market Data:")
    stocks = api_manager.get_stock_market_data("SPY")
    print(json.dumps(stocks, indent=2))
    
    print("\n📰 News Sentiment:")
    news = api_manager.get_news_sentiment("retail shopping")
    print(json.dumps(news, indent=2))
    
    print("\n💰 Economic Indicators:")
    economic = api_manager.get_economic_indicators()
    print(json.dumps(economic, indent=2))
    
    # Test interaction enrichment
    print("\n🔗 Enriched Interaction:")
    sample_interaction = {
        'interaction_id': 'test_123',
        'customer_id': 'CUST_001',
        'touchpoint': 'website',
        'revenue': 150.00,
        'city': 'New York'
    }
    
    enriched = api_manager.enrich_interaction(sample_interaction)
    print(json.dumps(enriched, indent=2))