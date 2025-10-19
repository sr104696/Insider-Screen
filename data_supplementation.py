import requests
from bs4 import BeautifulSoup
import json
from typing import Dict, Any, List, Optional
import time

class DataSupplementationService:
    """Service to supplement SEC data with additional sources"""
    
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (compatible SEC Financial Analysis Tool)'
        }
    
    def get_alpha_vantage_data(self, ticker: str, api_key: Optional[str] = None) -> Optional[Dict]:
        """Get financial data from Alpha Vantage API (free tier available)"""
        if not api_key:
            # User would need to provide their own API key
            return None
        
        try:
            # Income Statement (annual)
            url = f"https://www.alphavantage.co/query?function=INCOME_STATEMENT&symbol={ticker}&apikey={api_key}"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if 'annualReports' in data:
                    return {
                        'source': 'Alpha Vantage',
                        'annual_reports': data['annualReports'],
                        'quarterly_reports': data.get('quarterlyReports', [])
                    }
        except Exception as e:
            print(f"Alpha Vantage API error: {e}")
        
        return None
    
    def scrape_yahoo_finance_summary(self, ticker: str) -> Optional[Dict]:
        """Scrape basic financial metrics from Yahoo Finance"""
        try:
            url = f"https://finance.yahoo.com/quote/{ticker}/key-statistics"
            response = requests.get(url, headers=self.headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract key metrics (simplified example)
                metrics = {}
                
                # Look for revenue, profit margin, etc.
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 2:
                            metric_name = cells[0].get_text(strip=True)
                            metric_value = cells[1].get_text(strip=True)
                            
                            if any(keyword in metric_name.lower() for keyword in ['revenue', 'income', 'margin']):
                                metrics[metric_name] = metric_value
                
                return {
                    'source': 'Yahoo Finance',
                    'metrics': metrics,
                    'url': url
                }
                
        except Exception as e:
            print(f"Yahoo Finance scraping error: {e}")
        
        return None
    
    def get_industry_benchmarks(self, ticker: str, industry: str = 'payments') -> Dict[str, Any]:
        """Get industry-specific benchmarks and metrics"""
        
        payment_industry_benchmarks = {
            'typical_metrics': {
                'gross_payment_volume_growth': '15-25% annually',
                'net_revenue_margin': '1.5-3.5% of GPV',
                'transaction_count_growth': '20-40% annually',
                'cost_of_revenue_ratio': '40-60% of net revenue'
            },
            'key_ratios': {
                'revenue_per_transaction': 'Varies by merchant size',
                'processing_fee_rate': '2.3-3.5% average',
                'chargeback_rate': '<1% of transactions'
            },
            'seasonal_patterns': {
                'q4_boost': '15-25% higher volume (holidays)',
                'q1_decline': '10-15% sequential decline',
                'covid_impact': 'E-commerce acceleration'
            }
        }
        
        return {
            'industry': industry,
            'ticker': ticker,
            'benchmarks': payment_industry_benchmarks,
            'notes': [
                'Payment companies often report Gross Payment Volume (GPV) as key metric',
                'Net revenue is typically percentage of GPV',
                'Transaction count and average ticket size are key drivers'
            ]
        }
    
    def suggest_missing_data_sources(self, ticker: str, missing_metrics: List[str]) -> Dict[str, List[str]]:
        """Suggest specific sources for missing financial metrics"""
        
        suggestions = {
            'free_api_sources': [],
            'web_scraping_targets': [],
            'sec_document_search': [],
            'industry_reports': []
        }
        
        if 'revenue' in missing_metrics:
            suggestions['free_api_sources'].extend([
                f"Alpha Vantage Income Statement - {ticker}",
                f"Financial Modeling Prep - {ticker}",
                f"Yahoo Finance API - {ticker}"
            ])
            suggestions['web_scraping_targets'].extend([
                f"Yahoo Finance {ticker} financials",
                f"MarketWatch {ticker} income statement",
                f"Google Finance {ticker}"
            ])
        
        if 'payment_volume' in missing_metrics:
            suggestions['sec_document_search'].extend([
                f"Search 10-K for 'gross payment volume' or 'total volume processed'",
                f"Search 10-Q for 'payment volume' or 'transaction volume'",
                f"Look for management discussion sections"
            ])
            suggestions['industry_reports'].extend([
                "Nilson Report payment industry data",
                "Payment processor industry benchmarks"
            ])
        
        return suggestions