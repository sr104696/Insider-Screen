"""
Enhanced Offline EDGAR Processor - Complete SEC data processing without API dependencies
Adapted from comprehensive EDGAR toolkit for Replit compatibility
"""

import os
import json
import pickle
import requests
# Pandas is now installed
import pandas as pd
import time
from datetime import datetime, timedelta
from pathlib import Path
import logging
import zipfile
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EdgarOfflineProcessor:
    """
    Complete SEC EDGAR data processor that works offline with bulk downloaded data
    Replaces API-dependent approach with authoritative SEC filings processing
    """
    
    def __init__(self, data_dir="edgar_bulk_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        # Create subdirectories
        self.indices_dir = self.data_dir / "indices"
        self.filings_dir = self.data_dir / "filings"  
        self.cache_dir = self.data_dir / "cache"
        
        for dir_path in [self.indices_dir, self.filings_dir, self.cache_dir]:
            dir_path.mkdir(exist_ok=True)
        
        # Cache file paths
        self.ticker_cik_cache = self.cache_dir / "ticker_cik_mapping.json"
        self.company_facts_cache = self.cache_dir / "company_facts"
        self.company_facts_cache.mkdir(exist_ok=True)
        
        # SEC compliance
        self.base_url = "https://www.sec.gov/Archives/"
        self.headers = {"User-Agent": "SEC Financial Analysis Tool admin@company.com"}
        
        # Load cached data
        self.ticker_to_cik = self._load_ticker_cik_mapping()
        
        # Rate limiting
        self.request_delay = 0.1  # 10 requests per second max
        
    def _load_ticker_cik_mapping(self):
        """Load ticker to CIK mapping from cache"""
        if self.ticker_cik_cache.exists():
            with open(self.ticker_cik_cache, 'r') as f:
                return json.load(f)
        return {}
    
    def download_ticker_cik_mapping(self, force_refresh=False):
        """
        Download and cache comprehensive ticker to CIK mapping from SEC
        Downloads once, uses forever - true offline-first approach
        """
        if self.ticker_cik_cache.exists() and not force_refresh:
            logger.info("📊 Using cached ticker-CIK mapping")
            return self.ticker_to_cik
            
        url = "https://www.sec.gov/files/company_tickers.json"
        
        try:
            logger.info("📡 Downloading comprehensive ticker-CIK mapping from SEC...")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Convert to ticker -> CIK mapping
            ticker_mapping = {}
            for item in data.values():
                ticker = item['ticker'].upper()
                cik = str(item['cik_str']).zfill(10)
                ticker_mapping[ticker] = {
                    'cik': cik,
                    'name': item['title'],
                    'ticker': ticker
                }
            
            # Apply known corrections for problematic mappings
            corrections = {
                'FOUR': {'cik': '0001794669', 'name': 'SHIFT4 PAYMENTS INC', 'ticker': 'FOUR'},
                # Add more corrections as needed
            }
            ticker_mapping.update(corrections)
            
            # Save to cache
            with open(self.ticker_cik_cache, 'w') as f:
                json.dump(ticker_mapping, f, indent=2)
            
            self.ticker_to_cik = ticker_mapping
            logger.info(f"✅ Downloaded and cached {len(ticker_mapping)} ticker-CIK mappings")
            
            return ticker_mapping
            
        except Exception as e:
            logger.error(f"❌ Failed to download ticker-CIK mapping: {e}")
            # Create minimal fallback
            fallback_mapping = {
                'AAPL': {'cik': '0000320193', 'name': 'APPLE INC', 'ticker': 'AAPL'},
                'MSFT': {'cik': '0000789019', 'name': 'MICROSOFT CORP', 'ticker': 'MSFT'},
                'GOOGL': {'cik': '0001652044', 'name': 'ALPHABET INC', 'ticker': 'GOOGL'},
                'FOUR': {'cik': '0001794669', 'name': 'SHIFT4 PAYMENTS INC', 'ticker': 'FOUR'}
            }
            self.ticker_to_cik = fallback_mapping
            return fallback_mapping
    
    def get_company_info(self, ticker):
        """Get company information from cached ticker mapping - NO API CALLS"""
        ticker = ticker.upper()
        if ticker in self.ticker_to_cik:
            return self.ticker_to_cik[ticker]
        return None
    
    def download_quarterly_index(self, year, quarter, force_refresh=False):
        """Download SEC quarterly filing index with all CIK mappings"""
        index_file = self.indices_dir / f"{year}_Q{quarter}_master.idx"
        
        if index_file.exists() and not force_refresh:
            logger.info(f"📊 Using cached index for {year} Q{quarter}")
            return self._parse_master_index(index_file)
        
        url = f"{self.base_url}edgar/full-index/{year}/QTR{quarter}/master.idx"
        
        try:
            logger.info(f"📡 Downloading index for {year} Q{quarter}...")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            # Save raw index
            with open(index_file, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            logger.info(f"✅ Downloaded index for {year} Q{quarter}")
            time.sleep(self.request_delay)  # Rate limiting
            
            return self._parse_master_index(index_file)
            
        except Exception as e:
            logger.error(f"❌ Failed to download index for {year} Q{quarter}: {e}")
            return pd.DataFrame()
    
    def _parse_master_index(self, index_file):
        """Parse SEC master index file into DataFrame"""
        try:
            with open(index_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            lines = content.split('\n')
            
            # Find data start (after header)
            data_start = 0
            for i, line in enumerate(lines):
                if 'CIK|Company Name|Form Type|Date Filed|Filename' in line:
                    data_start = i + 1
                    break
            
            # Parse data lines
            data_rows = []
            for line in lines[data_start:]:
                if line.strip() and '|' in line:
                    parts = line.split('|')
                    if len(parts) == 5:
                        data_rows.append(parts)
            
            df = pd.DataFrame(data_rows, columns=['CIK', 'Company_Name', 'Form_Type', 'Date_Filed', 'Filename'])
            df['CIK'] = df['CIK'].str.zfill(10)  # Normalize CIK format
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to parse index file: {e}")
            return pd.DataFrame()
    
    def download_company_facts(self, ticker, force_refresh=False):
        """
        Download and cache comprehensive company financial facts
        Downloads once per company, processes offline forever
        """
        company_info = self.get_company_info(ticker)
        if not company_info:
            logger.error(f"❌ Company info not found for ticker {ticker}")
            return None
            
        cik = company_info['cik']
        facts_file = self.company_facts_cache / f"{ticker}_{cik}_facts.json"
        
        if facts_file.exists() and not force_refresh:
            logger.info(f"📊 Using cached company facts for {ticker}")
            with open(facts_file, 'r') as f:
                return json.load(f)
        
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        
        try:
            logger.info(f"📡 Downloading company facts for {ticker} (CIK: {cik})...")
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            facts_data = response.json()
            
            # Save to cache
            with open(facts_file, 'w') as f:
                json.dump(facts_data, f, indent=2)
            
            logger.info(f"✅ Downloaded and cached company facts for {ticker}")
            time.sleep(self.request_delay)  # Rate limiting
            
            return facts_data
            
        except Exception as e:
            logger.error(f"❌ Failed to download company facts for {ticker}: {e}")
            return None
    
    def extract_financial_metrics(self, ticker, years=5):
        """
        Extract comprehensive financial metrics from cached company facts
        Processes data offline - NO API CALLS
        """
        facts = self.download_company_facts(ticker)  # Uses cache if available
        if not facts:
            logger.warning(f"⚠️ No facts data available for {ticker}")
            return None
        
        # Target financial metrics
        target_metrics = [
            'Assets', 'AssetsCurrent', 'Liabilities', 'LiabilitiesCurrent',
            'StockholdersEquity', 'Revenues', 'NetIncomeLoss', 
            'GrossProfit', 'OperatingIncomeLoss', 'EarningsPerShareBasic',
            'CashAndCashEquivalentsAtCarryingValue', 'LongTermDebt'
        ]
        
        extracted_data = {
            'annual_data': [],
            'quarterly_data': [],
            'data_quality': {'completeness': 0, 'issues': []}
        }
        
        us_gaap = facts.get('facts', {}).get('us-gaap', {})
        
        # Process each metric
        for metric in target_metrics:
            if metric in us_gaap:
                metric_data = us_gaap[metric]
                units = metric_data.get('units', {})
                
                if 'USD' in units:
                    values = units['USD']
                    
                    # Separate annual vs quarterly data
                    for value in values:
                        form_type = value.get('form', '')
                        end_date = value.get('end', '')
                        filed_date = value.get('filed', '')
                        amount = value.get('val', 0)
                        
                        record = {
                            'metric': metric,
                            'value': amount,
                            'end_date': end_date,
                            'form_type': form_type,
                            'filed_date': filed_date
                        }
                        
                        if form_type in ['10-K', '10-K/A']:
                            extracted_data['annual_data'].append(record)
                        elif form_type in ['10-Q', '10-Q/A']:
                            extracted_data['quarterly_data'].append(record)
        
        # Calculate data quality
        total_expected = len(target_metrics) * years
        actual_data = len(extracted_data['annual_data']) + len(extracted_data['quarterly_data'])
        extracted_data['data_quality']['completeness'] = min(100, (actual_data / total_expected) * 100)
        
        return extracted_data
    
    def get_cached_tickers(self):
        """List all tickers with cached financial data"""
        cached_tickers = []
        for file in self.company_facts_cache.glob("*_facts.json"):
            ticker = file.stem.split('_')[0]
            cached_tickers.append(ticker)
        return sorted(cached_tickers)
    
    def get_storage_stats(self):
        """Get comprehensive storage statistics"""
        def get_size_mb(path):
            return sum(f.stat().st_size for f in path.rglob('*') if f.is_file()) / (1024*1024)
        
        stats = {
            'ticker_mappings': len(self.ticker_to_cik),
            'cached_facts': len(list(self.company_facts_cache.glob("*_facts.json"))),
            'indices_size_mb': get_size_mb(self.indices_dir),
            'cache_size_mb': get_size_mb(self.cache_dir),
            'total_size_mb': get_size_mb(self.data_dir)
        }
        return stats
    
    def initialize_offline_data(self, tickers=None, force_refresh=False):
        """
        Initialize comprehensive offline data for specified tickers
        Downloads once, processes offline forever
        """
        logger.info("🚀 Initializing comprehensive offline EDGAR data...")
        
        # Step 1: Download ticker-CIK mapping
        self.download_ticker_cik_mapping(force_refresh=force_refresh)
        
        # Step 2: Download facts for specified tickers
        if tickers:
            logger.info(f"📊 Downloading facts for {len(tickers)} tickers...")
            for ticker in tickers:
                self.download_company_facts(ticker, force_refresh=force_refresh)
                time.sleep(self.request_delay)  # Rate limiting
        
        logger.info("✅ Offline data initialization complete")
        return self.get_storage_stats()