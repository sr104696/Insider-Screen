"""
REAL DATA Financial Scraper - Actually extracts financial data from live sources
Uses advanced circuit breaker and focuses on getting ACTUAL data, not just SEC
"""

import requests
import time
import random
import re
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
from production_circuit_breaker import ProductionWindowBreaker

class RealDataFinancialScraper:
    """
    REAL DATA scraper that actually extracts financial information from live sources
    Focus: Get actual data from Yahoo Finance, Google Finance, company websites, news sources
    """
    
    def __init__(self, logger=None):
        self.logger = logger
        self.cache_dir = Path("cache/real_data_scraping")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Production circuit breaker with user's advanced implementation
        self.circuit_breaker = ProductionWindowBreaker(
            fail_threshold=3,  # Lower threshold for faster recovery
            window_sec=120,    # 2 minute sliding window
            open_sec=30,       # 30 second cooldown
            halfopen_max=2     # Allow 2 concurrent probes
        )
        
        # Real financial data sources
        self.data_sources = {
            'yahoo_finance': 'https://finance.yahoo.com/quote/{ticker}',
            'yahoo_financials': 'https://finance.yahoo.com/quote/{ticker}/financials',
            'yahoo_key_stats': 'https://finance.yahoo.com/quote/{ticker}/key-statistics',
            'marketwatch': 'https://www.marketwatch.com/investing/stock/{ticker}',
            'seeking_alpha': 'https://seekingalpha.com/symbol/{ticker}',
            'company_website': 'https://{ticker_lower}.com/investors',
            'company_ir': 'https://investors.{ticker_lower}.com',
            'google_finance': 'https://www.google.com/finance/quote/{ticker}:NASDAQ'
        }
        
        # Revenue extraction patterns for REAL data
        self.revenue_patterns = [
            # Yahoo Finance patterns
            r'Total Revenue.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*[BMK]',
            r'Revenue.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million|B|M)',
            r'Net Sales.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million|B|M)',
            
            # Quarterly patterns
            r'Q[1-4]\s+\d{4}.*?Revenue.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million|B|M)',
            r'Quarter.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million|B|M).*?revenue',
            
            # Annual patterns  
            r'FY\s*\d{4}.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million|B|M).*?revenue',
            r'Fiscal Year.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million|B|M)',
            
            # MarketWatch patterns
            r'Sales/Revenue.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*[BMK]',
            r'Total Revenue \(ttm\).*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*[BMK]'
        ]

    def create_real_session(self):
        """Create session optimized for real financial data extraction"""
        session = requests.Session()
        
        # Headers that work well with financial sites
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Chromium";v="117", "Not;A=Brand";v="8", "Google Chrome";v="117"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
        })
        
        return session

    def extract_real_financial_data(self, ticker: str) -> Dict[str, Any]:
        """
        Extract REAL financial data from live sources
        Focus on getting actual current data, not just historical SEC filings
        """
        if self.logger:
            self.logger.log_comprehensive('real_data_extraction_start',
                                        {'ticker': ticker},
                                        ticker=ticker,
                                        agent_context="Starting REAL financial data extraction from live sources")
        
        extracted_data = {
            'annual': [],
            'quarterly': [],
            'sources': [],
            'extraction_method': 'real_data_scraping',
            'circuit_breaker_stats': {},
            'live_data': True
        }
        
        session = self.create_real_session()
        successful_sources = 0
        
        # Extract from each real data source
        for source_name, url_template in self.data_sources.items():
            try:
                # Format URL for ticker
                if '{ticker}' in url_template:
                    url = url_template.format(ticker=ticker)
                elif '{ticker_lower}' in url_template:
                    url = url_template.format(ticker_lower=ticker.lower())
                else:
                    continue
                
                # Check circuit breaker
                if not self.circuit_breaker.allow(source_name):
                    if self.logger:
                        self.logger.log_comprehensive('circuit_breaker_block',
                                                    {'source': source_name, 'state': self.circuit_breaker.get_state(source_name)},
                                                    ticker=ticker)
                    continue
                
                # Attempt real data extraction
                self.circuit_breaker.on_attempt(source_name)
                
                try:
                    if self.logger:
                        self.logger.log_comprehensive('real_data_scrape_attempt',
                                                    {'source': source_name, 'url': url},
                                                    ticker=ticker)
                    
                    # Human-like delay
                    time.sleep(random.uniform(1, 3))
                    
                    response = session.get(url, timeout=20, allow_redirects=True)
                    
                    if response.status_code == 200:
                        # Extract real financial data
                        annual_data = self._extract_real_annual_data(response.text, source_name, url)
                        quarterly_data = self._extract_real_quarterly_data(response.text, source_name, url)
                        
                        if annual_data or quarterly_data:
                            extracted_data['annual'].extend(annual_data)
                            extracted_data['quarterly'].extend(quarterly_data)
                            extracted_data['sources'].append(f"{source_name}: {url}")
                            successful_sources += 1
                            
                            self.circuit_breaker.record_success(source_name)
                            
                            if self.logger:
                                self.logger.log_comprehensive('real_data_success',
                                                            {'source': source_name,
                                                             'annual_found': len(annual_data),
                                                             'quarterly_found': len(quarterly_data)},
                                                            ticker=ticker)
                        else:
                            # No data found but successful response
                            self.circuit_breaker.record_success(source_name)
                    else:
                        # Failed response
                        self.circuit_breaker.record_failure(source_name)
                        
                except Exception as e:
                    self.circuit_breaker.record_failure(source_name)
                    if self.logger:
                        self.logger.log_comprehensive('real_data_scrape_error',
                                                    {'source': source_name, 'error': str(e)[:200]},
                                                    e, ticker=ticker)
                finally:
                    self.circuit_breaker.on_attempt_done(source_name)
                    
            except Exception as e:
                if self.logger:
                    self.logger.log_comprehensive('real_data_source_error',
                                                {'source': source_name, 'error': str(e)[:200]},
                                                e, ticker=ticker)
        
        # Add circuit breaker statistics
        extracted_data['circuit_breaker_stats'] = {
            source: {
                'state': self.circuit_breaker.get_state(source),
                'failures': self.circuit_breaker.get_failure_count(source)
            } for source in self.data_sources.keys()
        }
        
        # Deduplicate and sort data
        extracted_data = self._process_real_data(extracted_data, ticker)
        
        if self.logger:
            self.logger.log_comprehensive('real_data_extraction_complete',
                                        {'ticker': ticker,
                                         'successful_sources': successful_sources,
                                         'annual_periods': len(extracted_data['annual']),
                                         'quarterly_periods': len(extracted_data['quarterly']),
                                         'total_sources': len(extracted_data['sources'])},
                                        ticker=ticker,
                                        agent_context="REAL financial data extraction completed")
        
        return extracted_data

    def _extract_real_annual_data(self, html_text: str, source: str, url: str) -> List[Dict]:
        """Extract annual financial data from real web sources"""
        annual_data = []
        
        # Look for revenue data in the HTML
        for pattern in self.revenue_patterns:
            matches = re.finditer(pattern, html_text, re.IGNORECASE | re.DOTALL)
            
            for match in matches:
                try:
                    # Extract value and convert to number
                    value_str = match.group(1).replace(',', '')
                    value = float(value_str)
                    
                    # Determine scale (million vs billion)
                    context = html_text[max(0, match.start()-100):match.end()+100].lower()
                    if 'billion' in context or ' b' in context:
                        value *= 1000000000
                    elif 'million' in context or ' m' in context:
                        value *= 1000000
                    elif 'k' in context and value > 100:  # Likely thousands
                        value *= 1000
                    
                    # Try to extract year from surrounding context
                    year_context = html_text[max(0, match.start()-300):match.end()+300]
                    year_matches = re.findall(r'\b(20\d{2})\b', year_context)
                    
                    if year_matches:
                        for year in year_matches:
                            annual_data.append({
                                'metric': 'revenue',
                                'value': int(value),
                                'fiscal_year': int(year),
                                'source_url': url,
                                'source_name': source,
                                'extraction_method': 'real_data_pattern_matching',
                                'context': context[:150]
                            })
                    else:
                        # Current/latest year if no specific year found
                        current_year = datetime.now().year
                        annual_data.append({
                            'metric': 'revenue',
                            'value': int(value),
                            'fiscal_year': current_year,
                            'source_url': url,
                            'source_name': source,
                            'extraction_method': 'real_data_pattern_matching',
                            'context': context[:150]
                        })
                        
                except (ValueError, IndexError):
                    continue
        
        return annual_data

    def _extract_real_quarterly_data(self, html_text: str, source: str, url: str) -> List[Dict]:
        """Extract quarterly financial data from real web sources"""
        quarterly_data = []
        
        # Quarterly patterns
        quarterly_patterns = [
            r'Q([1-4])\s+(\d{4}).*?Revenue.*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million|B|M)',
            r'([1-4])(?:st|nd|rd|th)\s+quarter.*?(\d{4}).*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million|B|M)',
            r'Three months ended.*?(\d{4}).*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:billion|million|B|M)'
        ]
        
        for pattern in quarterly_patterns:
            matches = re.finditer(pattern, html_text, re.IGNORECASE | re.DOTALL)
            
            for match in matches:
                try:
                    groups = match.groups()
                    
                    if len(groups) >= 3:
                        # Extract quarter, year, and value
                        if 'Q' in pattern:
                            quarter = f"Q{groups[0]}"
                            year = int(groups[1])
                            value_str = groups[2]
                        else:
                            quarter = f"Q{groups[0]}"
                            year = int(groups[1]) 
                            value_str = groups[2]
                        
                        value = float(value_str.replace(',', ''))
                        
                        # Determine scale
                        context = html_text[max(0, match.start()-100):match.end()+100].lower()
                        if 'billion' in context or ' b' in context:
                            value *= 1000000000
                        elif 'million' in context or ' m' in context:
                            value *= 1000000
                        
                        quarterly_data.append({
                            'metric': 'revenue',
                            'value': int(value),
                            'fiscal_year': year,
                            'fiscal_quarter': quarter,
                            'source_url': url,
                            'source_name': source,
                            'extraction_method': 'real_quarterly_pattern_matching',
                            'context': context[:150]
                        })
                        
                except (ValueError, IndexError, TypeError):
                    continue
        
        return quarterly_data

    def _process_real_data(self, data: Dict, ticker: str) -> Dict:
        """Process and deduplicate real financial data"""
        # Remove duplicates by value and year
        annual_seen = set()
        quarterly_seen = set()
        
        unique_annual = []
        for item in data['annual']:
            key = (item.get('fiscal_year'), item.get('value'))
            if key not in annual_seen and key[0] and key[1]:
                annual_seen.add(key)
                unique_annual.append(item)
        
        unique_quarterly = []
        for item in data['quarterly']:
            key = (item.get('fiscal_year'), item.get('fiscal_quarter'), item.get('value'))
            if key not in quarterly_seen and all(key):
                quarterly_seen.add(key)
                unique_quarterly.append(item)
        
        # Sort by year (most recent first)
        unique_annual.sort(key=lambda x: x.get('fiscal_year', 0), reverse=True)
        unique_quarterly.sort(key=lambda x: (x.get('fiscal_year', 0), x.get('fiscal_quarter', 'Q1')), reverse=True)
        
        data['annual'] = unique_annual
        data['quarterly'] = unique_quarterly
        data['real_data_processing_applied'] = True
        data['total_real_extractions'] = len(unique_annual) + len(unique_quarterly)
        
        return data