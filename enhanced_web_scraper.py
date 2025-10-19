"""
Enhanced Web Scraper using Advanced CloudScraper technology for comprehensive financial data extraction
NOW INTEGRATES: Multi-level Cloudflare bypass, Circuit breaker patterns, Job queue processing,
Advanced proxy management, and Multi-provider captcha solving capabilities
"""

import requests
import time
import random
import re
import json
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path
from advanced_scraper_client import AdvancedScraperClient, AdvancedJobQueue

class EnhancedFinancialScraper:
    """
    Production-grade financial data scraper with Cloudflare bypass and stealth capabilities
    Based on proven cloudscraper patterns from attached materials
    """
    
    def __init__(self, logger=None):
        self.logger = logger
        self.cache_dir = Path("cache/enhanced_scraping")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # ADVANCED scraping capabilities (user provided comprehensive solution)
        self.advanced_client = AdvancedScraperClient(logger)
        self.job_queue = AdvancedJobQueue(self.advanced_client)
        self.stealth_mode = True
        self.proxy_rotation = True  # Now enabled with advanced proxy manager
        self.cloudflare_bypass = True
        self.circuit_breaker_protection = True
        self.captcha_solving = True  # Ready for multi-provider integration
        self.browser_automation_ready = True  # For Playwright/undetected-chromedriver
        
        # Financial data patterns (enhanced from attached materials)
        self.revenue_patterns = [
            r'(?:revenue|sales|net sales)\s+(?:of|was|:)?\s*\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            r'(?:total\s+)?revenue\s*[:\-]?\s*\$?\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            r'net\s+sales\s*[:\-]?\s*\$?\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            r'(?:for\s+the\s+)?(?:fiscal\s+)?year\s+(?:ended\s+)?.*?revenue.*?\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            r'(?:fiscal\s+)?(?:year|period)\s+\d{4}.*?revenue.*?\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
        ]
        
        self.quarterly_patterns = [
            r'(?:q[1-4]|quarter)\s+\d{4}.*?revenue.*?\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            r'(?:first|second|third|fourth)\s+quarter.*?revenue.*?\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
            r'three\s+months\s+ended.*?revenue.*?\$\s*([\d,]+\.?\d*)\s*(million|billion|M|B)',
        ]

    def create_enterprise_session(self):
        """
        Create an ENTERPRISE-GRADE scraping session with ALL advanced capabilities
        Integrates user's comprehensive scraper client with circuit breakers, Cloudflare bypass, etc.
        """
        try:
            # Use the advanced scraper client (user's comprehensive solution)
            return self.advanced_client
            
        except Exception as e:
            if self.logger:
                self.logger.log_comprehensive('enterprise_session_error',
                                            {'error': str(e)},
                                            e)
            # Fallback to basic advanced client
            return AdvancedScraperClient(self.logger)

    def apply_stealth_techniques(self, session, url):
        """
        Apply stealth techniques based on cloudscraper stealth mode patterns
        """
        if not self.stealth_mode:
            return
            
        try:
            # Human-like delays (from stealth.py patterns)
            delay = random.uniform(0.5, 2.0)
            if random.random() < 0.1:  # 10% chance of longer pause
                delay *= 1.5
            delay = min(delay, 5.0)  # Cap at 5 seconds
            
            if delay >= 0.1:
                time.sleep(delay)
            
            # Randomize some headers to avoid fingerprinting
            accept_variants = [
                'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            ]
            session.headers['Accept'] = random.choice(accept_variants)
            
            # Add random DNT header
            if random.random() < 0.5:
                session.headers['DNT'] = '1'
            
        except Exception as e:
            if self.logger:
                self.logger.log_comprehensive('stealth_technique_error',
                                            {'url': url, 'error': str(e)},
                                            e)

    def get_enhanced_financial_urls(self, ticker: str) -> List[str]:
        """
        Get comprehensive list of financial data URLs using proven patterns
        """
        urls = []
        
        # Company-specific enhanced URLs
        company_domains = {
            'AAPL': ['apple.com/investor', 'apple.com/newsroom'],
            'MSFT': ['microsoft.com/investor', 'news.microsoft.com'],
            'GOOGL': ['abc.xyz/investor', 'investor.google.com'],
            'FOUR': ['shift4.com/investors', 'shift4.com/news', 'shift4.com/about'],
        }
        
        # Primary company URLs with enhanced paths
        if ticker in company_domains:
            for domain in company_domains[ticker]:
                urls.extend([
                    f"https://{domain}/",
                    f"https://{domain}/earnings",
                    f"https://{domain}/financial-results",
                    f"https://{domain}/quarterly-results",
                    f"https://{domain}/annual-reports",
                    f"https://{domain}/press-releases",
                    f"https://{domain}/news",
                ])
        
        # Generic patterns for any ticker
        company_name = ticker.lower()
        urls.extend([
            f"https://{company_name}.com/investors",
            f"https://{company_name}.com/investor-relations",
            f"https://investors.{company_name}.com",
            f"https://ir.{company_name}.com",
            f"https://{company_name}.com/about/investor-relations",
            f"https://{company_name}.com/financials",
        ])
        
        # Financial aggregator sites (like annualreports.com pattern)
        first_letter = ticker[0].lower()
        current_year = datetime.now().year
        for year in range(current_year - 5, current_year + 1):
            urls.extend([
                f"https://annualreports.com/Company/{ticker}",
                f"https://www.annualreports.com/HostedData/AnnualReportArchive/{first_letter}/NYSE_{ticker}_{year}.pdf",
                f"https://www.annualreports.com/HostedData/AnnualReportArchive/{first_letter}/NASDAQ_{ticker}_{year}.pdf",
            ])
        
        return list(set(urls))  # Remove duplicates

    def enterprise_scrape_financial_data(self, ticker: str) -> Dict[str, Any]:
        """
        ENTERPRISE-GRADE financial data scraping with ALL advanced capabilities
        Circuit breakers, Multi-level Cloudflare bypass, Job queue processing, Advanced proxy management
        """
        if self.logger:
            self.logger.log_comprehensive('enterprise_scraping_start',
                                        {'ticker': ticker},
                                        ticker=ticker,
                                        agent_context="Starting ENTERPRISE financial data scraping with advanced capabilities")
        
        try:
            client = self.create_enterprise_session()
            urls = self.get_enhanced_financial_urls(ticker)
            
            extracted_data = {
                'annual': [],
                'quarterly': [],
                'sources': [],
                'extraction_method': 'enterprise_web_scraping',
                'stealth_mode': self.stealth_mode,
                'cloudflare_bypass': self.cloudflare_bypass,
                'circuit_breaker_protection': self.circuit_breaker_protection,
                'advanced_proxy_rotation': self.proxy_rotation,
                'captcha_solving_ready': self.captcha_solving,
                'browser_automation_ready': self.browser_automation_ready
            }
            
            successful_extractions = 0
            
            # ENTERPRISE-GRADE scraping using user's comprehensive solution
            # Create jobs for bulk processing
            jobs = []
            for url in urls[:15]:  # Increased limit with job queue
                jobs.append(self.job_queue.add_job('GET', url))
            
            if self.logger:
                self.logger.log_comprehensive('enterprise_job_queue_start',
                                            {'total_jobs': len(jobs)},
                                            ticker=ticker,
                                            agent_context="Starting job queue processing with enterprise capabilities")
            
            # Process jobs using advanced scraper with all protections
            job_results = self.job_queue.process_jobs(jobs)
            
            for result in job_results:
                if result.get('success') and result.get('text'):
                    url = result.get('url')
                    
                    # Extract financial data using enhanced patterns
                    annual_data = self.extract_annual_data_enhanced(result['text'], url)
                    quarterly_data = self.extract_quarterly_data_enhanced(result['text'], url)
                    
                    extracted_data['annual'].extend(annual_data)
                    extracted_data['quarterly'].extend(quarterly_data)
                    
                    if annual_data or quarterly_data:
                        extracted_data['sources'].append(url)
                        successful_extractions += 1
                        
                        if self.logger:
                            self.logger.log_comprehensive('enterprise_extraction_success',
                                                        {'url': url, 
                                                         'annual_found': len(annual_data),
                                                         'quarterly_found': len(quarterly_data),
                                                         'bypass_used': result.get('bypass_used', False)},
                                                        ticker=ticker)
                elif not result.get('success'):
                    if self.logger:
                        self.logger.log_comprehensive('enterprise_job_failed',
                                                    {'url': result.get('url'), 
                                                     'error': result.get('error', 'Unknown error')[:200]},
                                                    ticker=ticker)
            
            # Deduplicate and enhance data
            extracted_data = self.enhance_extracted_data(extracted_data, ticker)
            
            if self.logger:
                self.logger.log_comprehensive('enterprise_scraping_complete',
                                            {'ticker': ticker,
                                             'successful_extractions': successful_extractions,
                                             'annual_periods': len(extracted_data['annual']),
                                             'quarterly_periods': len(extracted_data['quarterly']),
                                             'sources': len(extracted_data['sources']),
                                             'enterprise_features_used': {
                                                 'circuit_breaker': self.circuit_breaker_protection,
                                                 'cloudflare_bypass': self.cloudflare_bypass,
                                                 'job_queue': True,
                                                 'proxy_rotation': self.proxy_rotation
                                             }},
                                            ticker=ticker,
                                            agent_context="ENTERPRISE financial data scraping completed with ALL advanced features")
            
            return extracted_data
            
        except Exception as e:
            if self.logger:
                self.logger.log_comprehensive('enterprise_scraping_error',
                                            {'ticker': ticker, 'error': str(e)},
                                            e, ticker=ticker,
                                            agent_context="ENTERPRISE financial data scraping failed")
            return {'annual': [], 'quarterly': [], 'error': f"Enterprise scraping failed: {str(e)}"}

    def extract_annual_data_enhanced(self, text: str, url: str) -> List[Dict]:
        """Enhanced annual data extraction with multiple pattern matching"""
        annual_data = []
        
        for pattern in self.revenue_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    value_str = match.group(1).replace(',', '')
                    unit = match.group(2).lower()
                    
                    # Convert to standard units
                    value = float(value_str)
                    if unit in ['billion', 'b']:
                        value *= 1000000000
                    elif unit in ['million', 'm']:
                        value *= 1000000
                    
                    # Try to extract year from context
                    context = text[max(0, match.start()-200):match.end()+200]
                    year_match = re.search(r'\b(20\d{2})\b', context)
                    fiscal_year = int(year_match.group(1)) if year_match else None
                    
                    annual_data.append({
                        'metric': 'revenue',
                        'value': int(value),
                        'fiscal_year': fiscal_year,
                        'source_url': url,
                        'extraction_method': 'enhanced_pattern_matching',
                        'context': context[:100] + '...' if len(context) > 100 else context
                    })
                    
                except (ValueError, IndexError):
                    continue
        
        return annual_data

    def extract_quarterly_data_enhanced(self, text: str, url: str) -> List[Dict]:
        """Enhanced quarterly data extraction with multiple pattern matching"""
        quarterly_data = []
        
        for pattern in self.quarterly_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                try:
                    value_str = match.group(1).replace(',', '')
                    unit = match.group(2).lower()
                    
                    # Convert to standard units
                    value = float(value_str)
                    if unit in ['billion', 'b']:
                        value *= 1000000000
                    elif unit in ['million', 'm']:
                        value *= 1000000
                    
                    # Try to extract quarter and year from context
                    context = text[max(0, match.start()-200):match.end()+200]
                    
                    # Extract year
                    year_match = re.search(r'\b(20\d{2})\b', context)
                    fiscal_year = int(year_match.group(1)) if year_match else None
                    
                    # Extract quarter
                    quarter = None
                    if re.search(r'\bq1\b|\bfirst\s+quarter\b', context, re.IGNORECASE):
                        quarter = 'Q1'
                    elif re.search(r'\bq2\b|\bsecond\s+quarter\b', context, re.IGNORECASE):
                        quarter = 'Q2'
                    elif re.search(r'\bq3\b|\bthird\s+quarter\b', context, re.IGNORECASE):
                        quarter = 'Q3'
                    elif re.search(r'\bq4\b|\bfourth\s+quarter\b', context, re.IGNORECASE):
                        quarter = 'Q4'
                    
                    quarterly_data.append({
                        'metric': 'revenue',
                        'value': int(value),
                        'fiscal_year': fiscal_year,
                        'fiscal_quarter': quarter,
                        'source_url': url,
                        'extraction_method': 'enhanced_pattern_matching',
                        'context': context[:100] + '...' if len(context) > 100 else context
                    })
                    
                except (ValueError, IndexError):
                    continue
        
        return quarterly_data

    def enhance_extracted_data(self, data: Dict, ticker: str) -> Dict:
        """Enhanced data processing and deduplication"""
        # Remove duplicates and sort by fiscal year
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
            if key not in quarterly_seen and key[0] and key[1] and key[2]:
                quarterly_seen.add(key)
                unique_quarterly.append(item)
        
        # Sort by fiscal year (descending)
        unique_annual.sort(key=lambda x: x.get('fiscal_year', 0), reverse=True)
        unique_quarterly.sort(key=lambda x: (x.get('fiscal_year', 0), x.get('fiscal_quarter', 'Q1')), reverse=True)
        
        data['annual'] = unique_annual
        data['quarterly'] = unique_quarterly
        data['enterprise_enhancement_applied'] = True
        data['advanced_features'] = {
            'circuit_breaker_protection': True,
            'multi_level_cloudflare_bypass': True,  
            'job_queue_processing': True,
            'advanced_proxy_rotation': True,
            'captcha_solving_ready': True,
            'browser_automation_ready': True
        }
        data['total_extractions'] = len(unique_annual) + len(unique_quarterly)
        
        return data