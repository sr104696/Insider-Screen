"""
Web Financial Scraper - Parallel data source for SEC financial analysis
Scrapes financial data from company websites, investor pages, and press releases
Integrates with existing multi-tier fallback system
"""

import requests
import trafilatura
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
import time
from pathlib import Path
import json
import random
from urllib.parse import urljoin, urlparse

class WebFinancialScraper:
    def __init__(self, logger=None):
        self.logger = logger
        self.cache_dir = Path('./web_scrape_cache')
        self.cache_dir.mkdir(exist_ok=True)
        
        # Common financial data extraction patterns
        self.revenue_patterns = [
            # "Revenue of $1.2 billion", "Revenues: $500M"
            r'(?:revenue|revenues?)(?:\s+of|\s*:)?\s*\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            # "Net sales were $X million"
            r'(?:net\s+)?sales\s+(?:were|of|totaled)?\s*\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            # "Total revenue: $X.X billion"
            r'total\s+revenue\s*:\s*\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            # "Q1 2024 revenue was $X million"
            r'(Q[1-4]\s+\d{4}|FY\s*\d{4}|fiscal\s+\d{4})\s+revenue\s+(?:was|of)?\s*\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            # "Generated $X in revenue for Q1"
            r'generated\s+\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)?\s+in\s+revenue\s+for\s+(Q[1-4]|fiscal\s+\d{4})',
        ]
        
        self.net_income_patterns = [
            r'(?:net\s+income|earnings?)\s+(?:of|was|:)?\s*\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            r'profit\s+(?:of|was|:)?\s*\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
        ]

    def get_company_urls(self, ticker: str) -> List[str]:
        """Get relevant URLs for financial data scraping - Enhanced with proven patterns"""
        
        # Enhanced company-specific URLs based on research patterns
        company_domains = {
            'AAPL': ['apple.com/investor', 'apple.com/newsroom', 'sec.gov/Archives/edgar/data/0000320193'],
            'MSFT': ['microsoft.com/investor', 'news.microsoft.com', 'sec.gov/Archives/edgar/data/0000789019'],
            'GOOGL': ['abc.xyz/investor', 'investor.google.com', 'sec.gov/Archives/edgar/data/0001652044'],
            'FOUR': ['shift4.com/investors', 'shift4.com/news', 'shift4.com/about'],
        }
        
        base_urls = []
        
        # Primary company URLs
        if ticker in company_domains:
            for domain in company_domains[ticker]:
                if 'sec.gov' in domain:
                    # SEC EDGAR direct links
                    base_urls.append(f"https://{domain}/")
                else:
                    base_urls.extend([
                        f"https://{domain}/",
                        f"https://{domain}/earnings",
                        f"https://{domain}/press-releases", 
                        f"https://{domain}/financial-results",
                        f"https://{domain}/quarterly-results",
                        f"https://{domain}/annual-reports",
                    ])
        
        # Generic patterns for any ticker
        company_name = ticker.lower()
        base_urls.extend([
            f"https://{company_name}.com/investors",
            f"https://{company_name}.com/investor-relations", 
            f"https://investors.{company_name}.com",
            f"https://ir.{company_name}.com",
        ])
        
        # Financial data aggregator sites (inspired by annualreports.com pattern)
        first_letter = ticker[0].lower()
        base_urls.extend([
            f"https://annualreports.com/Company/{ticker}",
            f"https://www.annualreports.com/HostedData/AnnualReportArchive/{first_letter}/NYSE_{ticker}_2023.pdf",
            f"https://www.annualreports.com/HostedData/AnnualReportArchive/{first_letter}/NASDAQ_{ticker}_2023.pdf",
        ])
        
        return list(set(base_urls))  # Remove duplicates

    def scrape_annual_reports_pdfs(self, ticker: str) -> Dict[str, Any]:
        """Scrape PDF annual reports using annualreports.com pattern"""
        try:
            extracted_data = {'annual': [], 'quarterly': [], 'sources': []}
            
            # Apply the proven annualreports.com URL pattern
            first_letter = ticker[0].lower()
            stock_exchanges = ['NYSE', 'NASDAQ']
            years = range(2019, 2025)  # Last 5 years
            
            for exchange in stock_exchanges:
                for year in years:
                    try:
                        # Use exact URL pattern from attached scraper
                        url = f"https://annualreports.com/HostedData/AnnualReportArchive/{first_letter}/{exchange}_{ticker}_{year}.pdf"
                        
                        # Rate limiting (copied from attached pattern)
                        time.sleep(0.2)  # Quick delay for web responsiveness
                        
                        response = requests.get(url, timeout=60, headers={
                            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
                        })
                        
                        if response.status_code == 200:
                            # PDF found - save and process later
                            pdf_path = self.cache_dir / f"{ticker}_{year}_{exchange}.pdf"
                            with open(pdf_path, 'wb') as f:
                                f.write(response.content)
                            
                            extracted_data['sources'].append(url)
                            
                            if self.logger:
                                self.logger.log_comprehensive('pdf_download_success',
                                                            {'ticker': ticker, 'year': year, 'exchange': exchange},
                                                            ticker=ticker)
                    except Exception as e:
                        continue  # Try next combination
            
            return extracted_data
            
        except Exception as e:
            if self.logger:
                self.logger.log_comprehensive('pdf_scraping_error',
                                            {'ticker': ticker, 'error': str(e)},
                                            e, ticker=ticker)
            return {'annual': [], 'quarterly': [], 'error': f"PDF scraping failed: {str(e)}"}

    def scrape_financial_data(self, ticker: str) -> Dict[str, Any]:
        """Scrape financial data from company websites and investor pages"""
        if self.logger:
            self.logger.log_comprehensive('web_scraping_start', 
                                        {'ticker': ticker},
                                        ticker=ticker, 
                                        agent_context="Starting web scraping for financial data")
        
        try:
            urls = self.get_company_urls(ticker)
            extracted_data = {
                'annual': [],
                'quarterly': [],
                'extraction_method': 'web_scraping',
                'sources': []
            }
            
            # Enhanced scraping with retry patterns from attached materials
            for url in urls[:8]:  # Increased limit for better coverage
                max_retries = 3
                retry_count = 0
                
                while retry_count < max_retries:
                    try:
                        if self.logger:
                            self.logger.log_comprehensive('web_scrape_url', 
                                                        {'url': url, 'attempt': retry_count + 1},
                                                        ticker=ticker)
                        
                        # Enhanced headers to avoid blocking (from attached CORS examples)
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.19 Safari/537.36',
                            'Referer': urlparse(url).netloc,
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                            'Accept-Language': 'en-US,en;q=0.5',
                            'Connection': 'keep-alive',
                        }
                        
                        # Use trafilatura (note: headers passed differently)
                        downloaded = trafilatura.fetch_url(url)
                        if not downloaded:
                            retry_count += 1
                            time.sleep(0.5)  # Reduced delay for web responsiveness
                            continue
                        
                        text_content = trafilatura.extract(downloaded)
                        if not text_content:
                            retry_count += 1
                            continue
                        
                        break  # Success - exit retry loop
                        
                    except requests.exceptions.ConnectionError:
                        # Apply connection error handling from attached scraper
                        retry_count += 1
                        if self.logger:
                            self.logger.log_comprehensive('connection_error_retry',
                                                        {'url': url, 'attempt': retry_count},
                                                        ticker=ticker)
                        time.sleep(0.5 + retry_count * 0.2)  # Reduced increasing delay
                        continue
                    except Exception as e:
                        retry_count += 1
                        continue
                
                if retry_count >= max_retries:
                    continue  # Skip this URL after max retries
                
                # Extract financial data from text
                revenue_data = self.extract_revenue_from_text(text_content, ticker)
                net_income_data = self.extract_net_income_from_text(text_content, ticker)
                
                # Combine and categorize data
                for data in revenue_data + net_income_data:
                    data['source_url'] = url
                    
                    # Determine if annual or quarterly based on period info
                    period_info = data.get('period', '').lower()
                    if any(q in period_info for q in ['q1', 'q2', 'q3', 'q4', 'quarter']):
                        extracted_data['quarterly'].append(data)
                    elif any(fy in period_info for fy in ['fy', 'fiscal', 'year', 'annual']):
                        extracted_data['annual'].append(data)
                    else:
                        # Default to annual if unclear
                        extracted_data['annual'].append(data)
                
                extracted_data['sources'].append(url)
                
                # Be respectful - delay between requests
                time.sleep(1)
            
            # Deduplicate and sort
            extracted_data['annual'] = self._deduplicate_financial_data(extracted_data['annual'])
            extracted_data['quarterly'] = self._deduplicate_financial_data(extracted_data['quarterly'])
            
            if self.logger:
                self.logger.log_comprehensive('web_scraping_success',
                                            {'annual_count': len(extracted_data['annual']),
                                             'quarterly_count': len(extracted_data['quarterly']),
                                             'sources': len(extracted_data['sources'])},
                                            ticker=ticker,
                                            agent_context="Web scraping completed successfully")
            
            return extracted_data
            
        except Exception as e:
            if self.logger:
                self.logger.log_comprehensive('web_scraping_error',
                                            {'ticker': ticker, 'error': str(e)},
                                            e, ticker=ticker,
                                            agent_context="Web scraping failed")
            return {'annual': [], 'quarterly': [], 'error': f"Web scraping failed: {str(e)}"}

    def extract_revenue_from_text(self, text: str, ticker: str) -> List[Dict]:
        """Extract revenue data from text using regex patterns"""
        revenue_data = []
        
        for pattern in self.revenue_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE)
            
            for match in matches:
                try:
                    groups = match.groups()
                    
                    # Parse value and unit
                    if len(groups) >= 2:
                        value_str = groups[-3] if len(groups) >= 3 else groups[0]  # Get numeric value
                        unit_str = groups[-2] if len(groups) >= 3 else groups[1]   # Get unit
                        period_str = groups[0] if len(groups) >= 3 else ''         # Get period if available
                    else:
                        continue
                    
                    value = float(value_str.replace(',', ''))
                    
                    # Convert to actual dollars
                    if unit_str.lower() in ['million', 'm']:
                        value *= 1_000_000
                    elif unit_str.lower() in ['billion', 'b']:
                        value *= 1_000_000_000
                    
                    # Extract fiscal year/quarter info
                    fiscal_year = self._extract_fiscal_year(period_str, text)
                    fiscal_quarter = self._extract_fiscal_quarter(period_str)
                    
                    revenue_data.append({
                        'metric': 'revenue',
                        'value': int(value),
                        'fiscal_year': fiscal_year,
                        'fiscal_quarter': fiscal_quarter,
                        'period': period_str,
                        'extraction_method': 'web_scraping_regex',
                        'raw_match': match.group(0)
                    })
                    
                except (ValueError, IndexError):
                    continue
        
        return revenue_data

    def extract_net_income_from_text(self, text: str, ticker: str) -> List[Dict]:
        """Extract net income data from text using regex patterns"""
        income_data = []
        
        for pattern in self.net_income_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE)
            
            for match in matches:
                try:
                    groups = match.groups()
                    if len(groups) >= 2:
                        value_str = groups[0].replace(',', '')
                        unit_str = groups[1]
                        
                        value = float(value_str)
                        
                        # Convert to actual dollars
                        if unit_str.lower() in ['million', 'm']:
                            value *= 1_000_000
                        elif unit_str.lower() in ['billion', 'b']:
                            value *= 1_000_000_000
                        
                        # Extract fiscal info from surrounding text
                        context = match.string[max(0, match.start()-100):match.end()+100]
                        fiscal_year = self._extract_fiscal_year('', context)
                        fiscal_quarter = self._extract_fiscal_quarter(context)
                        
                        income_data.append({
                            'metric': 'net_income',
                            'value': int(value),
                            'fiscal_year': fiscal_year,
                            'fiscal_quarter': fiscal_quarter,
                            'extraction_method': 'web_scraping_regex',
                            'raw_match': match.group(0)
                        })
                        
                except (ValueError, IndexError):
                    continue
        
        return income_data

    def _extract_fiscal_year(self, period_str: str, context: str = '') -> int:
        """Extract fiscal year from period string or context"""
        # Look for 4-digit years
        year_pattern = r'\b(20\d{2})\b'
        
        # First try the period string
        match = re.search(year_pattern, period_str)
        if match:
            return int(match.group(1))
        
        # Then try the context
        match = re.search(year_pattern, context)
        if match:
            return int(match.group(1))
        
        # Default to current year - 1 (most recent completed fiscal year)
        return datetime.now().year - 1

    def _extract_fiscal_quarter(self, text: str) -> Optional[str]:
        """Extract fiscal quarter from text"""
        quarter_pattern = r'\b(Q[1-4])\b'
        match = re.search(quarter_pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        # Look for written quarters
        quarter_words = {
            'first': 'Q1', 'second': 'Q2', 'third': 'Q3', 'fourth': 'Q4',
            '1st': 'Q1', '2nd': 'Q2', '3rd': 'Q3', '4th': 'Q4'
        }
        
        for word, quarter in quarter_words.items():
            if word in text.lower():
                return quarter
        
        return None

    def _deduplicate_financial_data(self, data: List[Dict]) -> List[Dict]:
        """Remove duplicate financial data entries"""
        if not data:
            return []
        
        # Sort by fiscal year and value (descending)
        data.sort(key=lambda x: (x.get('fiscal_year', 0), x.get('value', 0)), reverse=True)
        
        # Remove duplicates based on metric, fiscal year, and similar values
        seen = set()
        unique_data = []
        
        for item in data:
            key = (
                item.get('metric', ''),
                item.get('fiscal_year', 0),
                item.get('fiscal_quarter', ''),
                # Group similar values (within 5% range)
                round(item.get('value', 0) / 1000000)  # Round to millions for grouping
            )
            
            if key not in seen:
                seen.add(key)
                unique_data.append(item)
        
        return unique_data[:10]  # Limit to 10 most relevant entries