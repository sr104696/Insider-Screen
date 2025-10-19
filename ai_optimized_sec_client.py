"""
AI-Optimized SEC Client - Consolidated with validators and AI-safe patterns
Key Changes for Replit AI:

1. Merged TickerValidator into SECClient (fewer files = better AI context)
2. Added AI-safe circuit breaker patterns
3. Simplified retry logic to prevent infinite loops
4. Added explicit operation safeguards
5. Clear, single-responsibility methods for AI understanding
"""

import requests
import time
import re
from typing import Dict, Optional, Any, Tuple, List
from urllib.parse import quote
from datetime import datetime

class AIOptimizedSECClient:
    """
    Consolidated SEC client optimized for Replit AI collaboration
    
    CRITICAL AI SAFETY FEATURES:
    - Circuit breaker prevents infinite retry loops
    - Explicit operation limits protect against destructive behavior
    - Clear method boundaries for AI understanding
    - Consolidated validation reduces file complexity
    """
    
    def __init__(self):
        # SEC API Configuration
        self.base_url = "https://data.sec.gov"
        self.headers = {
            "User-Agent": "SEC Financial Analysis Tool admin@company.com",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate"
        }
        
        # AI-SAFE CONFIGURATION: Prevent infinite loops
        self.max_retries = 2  # Reduced from 3 - AI tends to get stuck on failures
        self.request_timeout = 20  # Reduced from 30 - faster failure for AI
        self.circuit_breaker_failures = 0
        self.circuit_breaker_limit = 5  # Stop after 5 failures to prevent AI loops
        self.last_request_time = 0
        self.min_interval = 0.1  # SEC rate limiting
        
        # Ticker validation patterns (consolidated from validators.py)
        self.valid_ticker_pattern = re.compile(r'^[A-Z]{1,5}(-[A-Z])?(\.[A-Z])?$')
        self.ticker_corrections = {
            'BRK.A': 'BRK-A', 'BRK.B': 'BRK-B',
            'BF.A': 'BF-A', 'BF.B': 'BF-B'
        }
        
        # Session tracking for AI transparency
        self.session_stats = {
            "requests_made": 0,
            "successful_requests": 0, 
            "failed_requests": 0,
            "circuit_breaker_trips": 0
        }

    def validate_and_normalize_ticker(self, raw_ticker: str) -> Dict[str, Any]:
        """
        CONSOLIDATED: Ticker validation merged from validators.py
        AI-FRIENDLY: Single method, clear return format, no exceptions in normal flow
        """
        if not raw_ticker or not raw_ticker.strip():
            return {
                'valid': False,
                'ticker': None,
                'message': 'Ticker symbol required',
                'suggestions': ['Try: AAPL, MSFT, GOOGL']
            }
        
        ticker = raw_ticker.strip().upper()
        
        # Handle obvious non-ticker input
        if len(ticker) > 10 or ' ' in ticker:
            return {
                'valid': False,
                'ticker': None,
                'message': f"'{raw_ticker}' doesn't look like a ticker symbol",
                'suggestions': ['Use ticker symbols like AAPL, not company names']
            }
        
        # Apply known corrections
        if ticker in self.ticker_corrections:
            ticker = self.ticker_corrections[ticker]
        
        # Validate format
        if not self.valid_ticker_pattern.match(ticker):
            return {
                'valid': False,
                'ticker': None,
                'message': f"'{ticker}' is not a valid ticker format",
                'suggestions': ['Most tickers are 1-4 letters (AAPL, MSFT, GOOGL)']
            }
        
        return {
            'valid': True,
            'ticker': ticker,
            'message': f"Analyzing {ticker}...",
            'suggestions': []
        }

    def _check_circuit_breaker(self) -> bool:
        """
        AI SAFETY: Prevent infinite retry loops that confuse Replit AI
        Returns False if circuit breaker is tripped
        """
        if self.circuit_breaker_failures >= self.circuit_breaker_limit:
            self.session_stats["circuit_breaker_trips"] += 1
            print(f"🚫 Circuit breaker tripped! {self.circuit_breaker_failures} failures. Stopping to prevent AI loops.")
            return False
        return True

    def _make_safe_request(self, url: str, operation: str) -> Optional[Dict]:
        """
        AI-SAFE REQUEST METHOD: Simplified retry logic with explicit safeguards
        
        Key changes for AI safety:
        1. Clear operation naming for AI understanding
        2. Circuit breaker prevents infinite loops
        3. Simplified retry logic (2 attempts max)
        4. Explicit logging for AI transparency
        """
        if not self._check_circuit_breaker():
            return None
        
        print(f"📡 {operation}: {url}")
        
        for attempt in range(1, self.max_retries + 1):
            try:
                # SEC rate limiting
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                if time_since_last < self.min_interval:
                    time.sleep(self.min_interval - time_since_last)
                
                self.last_request_time = time.time()
                self.session_stats["requests_made"] += 1
                
                response = requests.get(url, headers=self.headers, timeout=self.request_timeout)
                
                # Handle specific HTTP status codes
                if response.status_code == 403:
                    print(f"⚠️ Rate limited by SEC API (attempt {attempt})")
                    if attempt < self.max_retries:
                        time.sleep(10)  # Fixed wait instead of exponential backoff
                        continue
                    else:
                        self.circuit_breaker_failures += 1
                        return None
                
                elif response.status_code == 404:
                    print(f"❌ Not found: {operation}")
                    return None  # Don't retry 404s
                
                elif response.status_code >= 500:
                    print(f"🚨 Server error {response.status_code} (attempt {attempt})")
                    if attempt < self.max_retries:
                        time.sleep(5)
                        continue
                    else:
                        self.circuit_breaker_failures += 1
                        return None
                
                # Success
                response.raise_for_status()
                self.session_stats["successful_requests"] += 1
                self.circuit_breaker_failures = max(0, self.circuit_breaker_failures - 1)  # Recover
                
                print(f"✅ {operation} successful")
                return response.json()
                
            except requests.exceptions.Timeout:
                print(f"⏱️ Timeout on {operation} (attempt {attempt})")
                if attempt >= self.max_retries:
                    self.circuit_breaker_failures += 1
                    
            except Exception as e:
                print(f"💥 Error in {operation} (attempt {attempt}): {str(e)}")
                if attempt >= self.max_retries:
                    self.circuit_breaker_failures += 1
        
        # All retries failed
        self.session_stats["failed_requests"] += 1
        print(f"❌ {operation} failed after {self.max_retries} attempts")
        return None

    def get_company_info(self, ticker: str) -> Optional[Dict]:
        """
        AI-FRIENDLY: Clear method with explicit validation and error handling
        """
        # Validate ticker first
        validation = self.validate_and_normalize_ticker(ticker)
        if not validation['valid']:
            print(f"❌ Invalid ticker: {validation['message']}")
            return None
        
        normalized_ticker = validation['ticker']
        print(f"🔍 Looking up company info for {normalized_ticker}")
        
        # Get ticker to CIK mapping (FIXED: Use www.sec.gov not data.sec.gov)
        mapping_data = self._make_safe_request(
            "https://www.sec.gov/files/company_tickers.json",
            f"Ticker lookup for {normalized_ticker}"
        )
        
        if not mapping_data:
            return None
        
        # Find company by ticker
        for company_data in mapping_data.values():
            if company_data['ticker'].upper() == normalized_ticker:
                cik = str(company_data['cik_str']).zfill(10)
                print(f"📋 Found CIK {cik} for {normalized_ticker}")
                
                # Get detailed company info
                company_details = self._make_safe_request(
                    f"https://data.sec.gov/submissions/CIK{cik}.json",
                    f"Company details for {normalized_ticker}"
                )
                
                # Return basic info even if details fail
                result = {
                    'cik': cik,
                    'name': company_data['title'],
                    'ticker': normalized_ticker
                }
                
                if company_details:
                    result.update({
                        'name': company_details.get('name', company_data['title']),
                        'sic': company_details.get('sic'),
                        'sicDescription': company_details.get('sicDescription'),
                        'fiscalYearEnd': company_details.get('fiscalYearEnd')
                    })
                
                return result
        
        print(f"❌ Company not found for ticker {normalized_ticker}")
        return None

    def get_company_facts(self, cik: str) -> Optional[Dict]:
        """
        AI-FRIENDLY: Simple method with clear purpose and error handling
        """
        if not cik or len(cik) != 10:
            print(f"❌ Invalid CIK format: {cik}")
            return None
        
        print(f"📊 Fetching financial data for CIK {cik}")
        
        return self._make_safe_request(
            f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
            f"Financial facts for CIK {cik}"
        )

    def reset_circuit_breaker(self):
        """
        AI-SAFE: Allow manual reset of circuit breaker
        Useful if AI gets stuck and needs a clean slate
        """
        self.circuit_breaker_failures = 0
        print("🔄 Circuit breaker reset - ready for new operations")

    def get_status(self) -> Dict[str, Any]:
        """
        AI-TRANSPARENCY: Clear status for AI understanding
        """
        return {
            "operational": self.circuit_breaker_failures < self.circuit_breaker_limit,
            "circuit_breaker_failures": self.circuit_breaker_failures,
            "session_stats": self.session_stats,
            "ready_for_requests": self._check_circuit_breaker()
        }