"""
Offline-First SEC Client - Replaces api-dependent client with local data
Maintains identical interface while eliminating API dependency issues
"""

import requests
import time
import logging
from typing import Dict, Optional, Tuple
from datetime import datetime
import json
import os
from local_sec_data_manager import LocalSECDataManager

class OfflineFirstSECClient:
    """
    SEC Client that prioritizes local data over API calls
    
    This replaces the failing API-dependent client with a robust offline-first approach:
    1. Primary: Local ticker-to-CIK mapping (fast, reliable)
    2. Secondary: API calls with short timeout (fresh data when possible)
    3. Fallback: Cached responses and error handling
    
    Maintains exact same interface as original AI-optimized client
    """
    
    def __init__(self):
        # Initialize local data manager
        self.local_data = LocalSECDataManager()
        
        # API configuration (for fallback attempts)
        self.base_url = "https://data.sec.gov"
        self.headers = {
            "User-Agent": "SEC Financial Analysis Tool admin@company.com",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate"
        }
        
        # AI-safe configuration
        self.max_retries = 1  # Single retry for API calls
        self.api_timeout = 3   # Short timeout for API attempts
        self.circuit_breaker_failures = 0
        self.circuit_breaker_limit = 3
        
        # Session tracking (maintain compatibility)
        self.session_stats = {
            "requests_made": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "circuit_breaker_trips": 0,
            "local_data_used": 0,
            "api_calls_made": 0
        }
        
        # Initialize local data
        self._ensure_local_data_ready()
    
    def get_company_info(self, ticker: str) -> Optional[Dict]:
        """
        Get company information for a ticker symbol
        
        Strategy:
        1. Try local ticker mapping (primary - fast & reliable)
        2. If found locally, try to enhance with API data (optional)
        3. If not found locally, try API with timeout
        4. Return best available data or None
        
        Returns same format as original client:
        {
            'cik': 'XXXXXXXXXX',
            'name': 'Company Name',
            'ticker': 'TICKER',
            'sic': 'XXXX',
            'industry': 'Industry Name',
            'data_source': 'local|api|hybrid'
        }
        """
        if not self._validate_ticker(ticker):
            return None
            
        normalized_ticker = ticker.upper().strip()
        print(f"🔍 Looking up company info for {normalized_ticker}")
        
        # Step 1: Try local data first (primary path)
        local_result = self.local_data.lookup_ticker(normalized_ticker)
        
        if local_result:
            print(f"📋 Found {normalized_ticker} in local data: CIK {local_result['cik']}")
            self.session_stats["local_data_used"] += 1
            self.session_stats["successful_requests"] += 1
            
            # Try to enhance with API data if circuit breaker allows
            if self._should_attempt_api():
                enhanced_result = self._try_enhance_with_api(local_result)
                if enhanced_result:
                    enhanced_result['data_source'] = 'hybrid'
                    return enhanced_result
            
            # Return local data with source indication
            local_result['data_source'] = 'local'
            return local_result
        
        # Step 2: Not found locally, try API as fallback
        print(f"⚡ {normalized_ticker} not in local data, trying API fallback")
        api_result = self._try_api_lookup(normalized_ticker)
        
        if api_result:
            print(f"✅ Found {normalized_ticker} via API")
            api_result['data_source'] = 'api'
            return api_result
        
        # Step 3: All methods failed
        print(f"❌ Could not find company info for {normalized_ticker}")
        return None
    
    def get_company_facts(self, cik: str) -> Optional[Dict]:
        """
        Get financial facts for a company
        
        This method attempts API call for financial data since local storage
        of all financial facts would exceed Replit storage limits
        """
        if not cik:
            return None
            
        print(f"📊 Fetching financial data for CIK {cik}")
        
        if not self._should_attempt_api():
            print("⚠️ API calls disabled due to circuit breaker")
            return None
        
        try:
            url = f"{self.base_url}/api/xbrl/companyfacts/CIK{cik}.json"
            print(f"📡 Financial facts for CIK {cik}: {url}")
            
            self.session_stats["requests_made"] += 1
            self.session_stats["api_calls_made"] += 1
            
            response = requests.get(url, headers=self.headers, timeout=self.api_timeout)
            
            if response.status_code == 200:
                self.session_stats["successful_requests"] += 1
                print(f"✅ Financial facts for CIK {cik} successful")
                return response.json()
            elif response.status_code == 404:
                print(f"📄 No financial data found for CIK {cik}")
                return None
            else:
                print(f"⚠️ API returned status {response.status_code}")
                self._record_api_failure()
                return None
                
        except requests.exceptions.Timeout:
            print("⏱️ API request timed out")
            self._record_api_failure()
            return None
        except Exception as e:
            print(f"🚨 API request failed: {e}")
            self._record_api_failure()
            return None
    
    def reset_circuit_breaker(self):
        """Reset circuit breaker state"""
        self.circuit_breaker_failures = 0
        print("🔄 Circuit breaker reset")
    
    def get_status(self) -> Dict:
        """
        Get client operational status
        Maintains compatibility with AI-safe error handler
        """
        data_status = self.local_data.get_data_status()
        
        return {
            'operational': True,
            'circuit_breaker_failures': self.circuit_breaker_failures,
            'ready_for_requests': self.circuit_breaker_failures < self.circuit_breaker_limit,
            'session_stats': self.session_stats,
            'local_data_status': data_status,
            'api_available': self._should_attempt_api()
        }
    
    def _ensure_local_data_ready(self):
        """Initialize local data on startup"""
        try:
            success = self.local_data.ensure_data_ready()
            if not success:
                print("⚠️ Local data initialization had issues, but continuing...")
        except Exception as e:
            print(f"🚨 Failed to initialize local data: {e}")
            print("   Will attempt to continue with limited functionality")
    
    def _validate_ticker(self, ticker: str) -> bool:
        """Basic ticker validation"""
        if not ticker or not isinstance(ticker, str):
            return False
        
        ticker = ticker.strip()
        if not ticker or len(ticker) > 6:
            return False
        
        return True
    
    def _should_attempt_api(self) -> bool:
        """Check if API calls should be attempted (circuit breaker logic)"""
        return self.circuit_breaker_failures < self.circuit_breaker_limit
    
    def _record_api_failure(self):
        """Record API failure for circuit breaker"""
        self.circuit_breaker_failures += 1
        self.session_stats["failed_requests"] += 1
        
        if self.circuit_breaker_failures >= self.circuit_breaker_limit:
            self.session_stats["circuit_breaker_trips"] += 1
            print(f"🔌 Circuit breaker activated after {self.circuit_breaker_failures} API failures")
    
    def _try_enhance_with_api(self, local_result: Dict) -> Optional[Dict]:
        """
        Try to enhance local data with additional info from API
        Returns enhanced result or None if API fails
        """
        cik = local_result['cik']
        
        try:
            url = f"{self.base_url}/submissions/CIK{cik}.json"
            print(f"📡 Company details for {local_result['ticker']}: {url}")
            response = requests.get(url, headers=self.headers, timeout=self.api_timeout)
            
            if response.status_code == 200:
                api_data = response.json()
                print(f"✅ Company details for {local_result['ticker']} successful")
                
                # Enhance local result with API data
                enhanced = local_result.copy()
                enhanced.update({
                    'name': api_data.get('name', local_result['name']),
                    'sic': api_data.get('sic', ''),
                    'sicDescription': api_data.get('sicDescription', ''),
                    'industry': api_data.get('sicDescription', 'Unknown'),
                    'ein': api_data.get('ein', ''),
                    'category': api_data.get('category', ''),
                    'fiscalYearEnd': api_data.get('fiscalYearEnd', '')
                })
                
                self.session_stats["api_calls_made"] += 1
                self.session_stats["successful_requests"] += 1
                return enhanced
                
        except Exception as e:
            # Enhancement failed, but don't treat as major failure
            pass
        
        return None
    
    def _try_api_lookup(self, ticker: str) -> Optional[Dict]:
        """
        Attempt to look up ticker via API (fallback method)
        Returns company info or None
        """
        try:
            # Try the company_tickers.json endpoint (original failing endpoint)
            url = f"https://www.sec.gov/files/company_tickers.json"
            print(f"📡 Ticker lookup for {ticker}: {url}")
            response = requests.get(url, headers=self.headers, timeout=self.api_timeout)
            
            self.session_stats["requests_made"] += 1
            self.session_stats["api_calls_made"] += 1
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Ticker lookup for {ticker} successful")
                
                # Search for ticker in the data
                for company_data in data.values():
                    if company_data['ticker'].upper() == ticker:
                        result = {
                            'cik': str(company_data['cik_str']).zfill(10),
                            'name': company_data['title'],
                            'ticker': ticker
                        }
                        
                        self.session_stats["successful_requests"] += 1
                        return result
                
                # Ticker not found in API data
                self.session_stats["failed_requests"] += 1
                return None
            
            else:
                self._record_api_failure()
                return None
                
        except Exception as e:
            self._record_api_failure()
            return None

# Compatibility function for existing imports
def create_optimized_sec_client():
    """Factory function to maintain compatibility with existing code"""
    return OfflineFirstSECClient()

# For direct replacement in existing code
AIOptimizedSECClient = OfflineFirstSECClient