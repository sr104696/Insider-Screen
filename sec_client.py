import requests
import time
import logging
from typing import Dict, Optional, Any
from urllib.parse import quote
from error_logger import error_logger, ErrorCategory, ErrorLevel, DetailLevel
from replit_safe_monitor import replit_monitor
from session_manager import session_manager
import socket
from datetime import datetime
import sys

class SECRateLimiter:
    """SEC compliant rate limiter - max 10 requests per second"""
    
    def __init__(self):
        self.last_request_time = 0
        self.min_interval = 0.1  # 100ms between requests = max 10/sec
        self.request_count = 0
        self.start_time = time.time()
    
    def wait_if_needed(self):
        """Enforce SEC rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_interval:
            sleep_time = self.min_interval - time_since_last
            error_logger.log_error(
                f"Rate limiting: sleeping {sleep_time:.3f}s",
                ErrorCategory.SEC_API,
                ErrorLevel.DEBUG,
                DetailLevel.DETAILED,
                {"time_since_last": time_since_last, "min_interval": self.min_interval}
            )
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1

class SECClient:
    """Production-ready SEC EDGAR API client with comprehensive error handling"""
    
    def __init__(self):
        self.base_url = "https://data.sec.gov"
        self.headers = {
            "User-Agent": "SEC Financial Analysis Tool admin@company.com",
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov"
        }
        self.rate_limiter = SECRateLimiter()
        self.request_timeout = 30  # 30 second timeout for Replit
        self.max_retries = 3
        
        # Track session performance
        self.session_stats = {
            "requests_made": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_time": 0,
            "rate_limit_hits": 0
        }
        
    def _make_request_with_retries(self, url: str, description: str = "") -> Optional[Dict]:
        """Make SEC API request with comprehensive error handling and retries"""
        
        start_time = time.time()
        last_exception = None
        
        for attempt in range(1, self.max_retries + 1):
            try:
                # Apply SEC rate limiting
                self.rate_limiter.wait_if_needed()
                
                # Track request
                self.session_stats["requests_made"] += 1
                
                error_logger.log_error(
                    f"Making SEC API request - Attempt {attempt}/{self.max_retries}",
                    ErrorCategory.SEC_API,
                    ErrorLevel.INFO,
                    DetailLevel.DETAILED,
                    {
                        "url": url,
                        "description": description,
                        "attempt": attempt,
                        "headers": dict(self.headers),
                        "timeout": self.request_timeout
                    }
                )
                
                # Make request with timeout
                response = requests.get(
                    url, 
                    headers=self.headers, 
                    timeout=self.request_timeout
                )
                
                # Handle HTTP errors
                if response.status_code == 403:
                    self.session_stats["rate_limit_hits"] += 1
                    error_logger.log_sec_api_error(
                        url, 403, response.text[:500], 
                        detail_level=DetailLevel.FORENSIC
                    )
                    
                    if attempt < self.max_retries:
                        sleep_time = min(60, 10 * attempt)  # Exponential backoff up to 60s
                        error_logger.log_error(
                            f"Rate limited by SEC API, waiting {sleep_time}s before retry",
                            ErrorCategory.SEC_API,
                            ErrorLevel.WARNING,
                            DetailLevel.STANDARD,
                            {"sleep_time": sleep_time, "attempt": attempt}
                        )
                        time.sleep(sleep_time)
                        continue
                    else:
                        raise requests.exceptions.HTTPError(f"SEC API rate limit exceeded after {self.max_retries} attempts")
                
                elif response.status_code == 404:
                    error_logger.log_sec_api_error(
                        url, 404, response.text[:500],
                        detail_level=DetailLevel.DETAILED
                    )
                    return None  # Don't retry 404s
                
                elif response.status_code >= 500:
                    error_logger.log_sec_api_error(
                        url, response.status_code, response.text[:500],
                        detail_level=DetailLevel.DETAILED
                    )
                    
                    if attempt < self.max_retries:
                        sleep_time = 5 * attempt
                        time.sleep(sleep_time)
                        continue
                    else:
                        raise requests.exceptions.HTTPError(f"SEC API server error: {response.status_code}")
                
                # Successful response
                response.raise_for_status()
                
                # Track success
                request_time = time.time() - start_time
                self.session_stats["successful_requests"] += 1
                self.session_stats["total_time"] += int(request_time)
                
                error_logger.log_error(
                    f"SEC API request successful",
                    ErrorCategory.SEC_API,
                    ErrorLevel.INFO,
                    DetailLevel.STANDARD,
                    {
                        "url": url,
                        "status_code": response.status_code,
                        "response_size": len(response.content),
                        "request_time": request_time,
                        "attempt": attempt
                    }
                )
                
                return response.json()
                
            except requests.exceptions.Timeout as e:
                last_exception = e
                self.session_stats["failed_requests"] += 1
                
                error_logger.log_network_timeout(
                    url, self.request_timeout, attempt,
                    DetailLevel.DETAILED
                )
                
                if attempt < self.max_retries:
                    sleep_time = 2 * attempt
                    time.sleep(sleep_time)
                    continue
                    
            except requests.exceptions.ConnectionError as e:
                last_exception = e
                self.session_stats["failed_requests"] += 1
                
                error_logger.log_error(
                    f"Connection error on SEC API request",
                    ErrorCategory.NETWORK,
                    ErrorLevel.ERROR,
                    DetailLevel.DETAILED,
                    {
                        "url": url,
                        "attempt": attempt,
                        "error": str(e)
                    },
                    exception=e
                )
                
                if attempt < self.max_retries:
                    sleep_time = 5 * attempt
                    time.sleep(sleep_time)
                    continue
                    
            except Exception as e:
                last_exception = e
                self.session_stats["failed_requests"] += 1
                
                error_logger.log_error(
                    f"Unexpected error in SEC API request",
                    ErrorCategory.SEC_API,
                    ErrorLevel.ERROR,
                    DetailLevel.FORENSIC,
                    {
                        "url": url,
                        "attempt": attempt,
                        "error_type": type(e).__name__,
                        "error_message": str(e)
                    },
                    exception=e
                )
                
                if attempt < self.max_retries:
                    sleep_time = 3 * attempt
                    time.sleep(sleep_time)
                    continue
        
        # All retries failed
        total_time = time.time() - start_time
        error_logger.log_error(
            f"SEC API request failed after {self.max_retries} attempts",
            ErrorCategory.SEC_API,
            ErrorLevel.CRITICAL,
            DetailLevel.FORENSIC,
            {
                "url": url,
                "total_attempts": self.max_retries,
                "total_time": total_time,
                "last_exception": str(last_exception),
                "session_stats": self.session_stats
            },
            exception=last_exception
        )
        
        return None
    
    def _make_request(self, url: str, description: str = "") -> Optional[Dict]:
        """Legacy method - now uses new retry mechanism"""
        return self._make_request_with_retries(url, description)
    
    def get_company_info(self, ticker: str) -> Optional[Dict]:
        """Get company information by ticker with Replit optimizations"""
        try:
            # Check for cached data first (handles session restarts)
            cached_data = session_manager.get_cached_sec_data(ticker, "company_info")
            if cached_data:
                error_logger.log_error(
                    f"Using cached company info for {ticker}",
                    ErrorCategory.SEC_API,
                    ErrorLevel.INFO,
                    DetailLevel.STANDARD,
                    {"ticker": ticker, "cache_hit": True}
                )
                return cached_data
            
            # Optimize for Replit constraints before processing  
            if hasattr(replit_monitor, 'optimize_for_data_processing'):
                optimization = replit_monitor.optimize_for_data_processing(estimated_size_mb=1)
            else:
                optimization = replit_monitor.optimize_for_replit(data_size_estimate=1024*1024)
            if not optimization["ready"]:
                error_logger.log_error(
                    f"Resource constraints prevent processing {ticker}",
                    ErrorCategory.SYSTEM,
                    ErrorLevel.ERROR,
                    DetailLevel.FORENSIC,
                    optimization
                )
                return None
            
            # First, get the ticker to CIK mapping
            url = "https://www.sec.gov/files/company_tickers.json"
            data = self._make_request_with_retries(url, f"Fetching CIK for ticker: {ticker}")
            
            if not data:
                return None
            
            # Find the company by ticker
            ticker_upper = ticker.upper()
            for company_data in data.values():
                if company_data['ticker'].upper() == ticker_upper:
                    cik = str(company_data['cik_str']).zfill(10)
                    
                    # Get additional company details
                    company_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
                    company_details = self._make_request_with_retries(company_url, f"Fetching company details for CIK: {cik}")
                    
                    if company_details:
                        company_info = {
                            'cik': cik,
                            'name': company_details.get('name', company_data['title']),
                            'ticker': ticker_upper,
                            'sic': company_details.get('sic'),
                            'sicDescription': company_details.get('sicDescription'),
                            'fiscalYearEnd': company_details.get('fiscalYearEnd')
                        }
                    else:
                        company_info = {
                            'cik': cik,
                            'name': company_data['title'],
                            'ticker': ticker_upper
                        }
                    
                    # Cache the result for session recovery
                    session_manager.cache_sec_data(ticker, "company_info", company_info)
                    return company_info
            
            return None
            
        except Exception as e:
            error_logger.log_error(
                f"Critical error in get_company_info for ticker: {ticker}",
                ErrorCategory.SEC_API,
                ErrorLevel.CRITICAL,
                DetailLevel.FORENSIC,
                {"ticker": ticker, "error": str(e)},
                exception=e
            )
            return None
    
    def get_company_facts(self, cik: str) -> Optional[Dict]:
        """Get company facts (financial data) by CIK with memory optimization"""
        try:
            # Check cache first
            cached_data = session_manager.get_cached_sec_data(cik, "company_facts")
            if cached_data:
                error_logger.log_error(
                    f"Using cached company facts for CIK {cik}",
                    ErrorCategory.SEC_API,
                    ErrorLevel.INFO,
                    DetailLevel.STANDARD,
                    {"cik": cik, "cache_hit": True}
                )
                return cached_data
            
            # Check resources before large data operation
            if hasattr(replit_monitor, 'check_replit_limits'):
                resource_check = replit_monitor.check_replit_limits(estimated_memory_mb=5)
            else:
                resource_check = replit_monitor.check_resource_limits()
            if resource_check["status"] == "critical":
                error_logger.log_error(
                    f"Critical resource usage - cannot fetch company facts for CIK {cik}",
                    ErrorCategory.SYSTEM,
                    ErrorLevel.CRITICAL,
                    DetailLevel.FORENSIC,
                    resource_check
                )
                return None
            
            # Optimize for large financial data processing
            if hasattr(replit_monitor, 'optimize_for_data_processing'):
                optimization = replit_monitor.optimize_for_data_processing(estimated_size_mb=5)
            else:
                optimization = replit_monitor.optimize_for_replit(data_size_estimate=5*1024*1024)
            if not optimization["ready"]:
                return None
            
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
            data = self._make_request_with_retries(url, f"Fetching company facts for CIK: {cik}")
            
            if not data:
                return None
            
            # Check data size and optimize if needed
            data_size = sys.getsizeof(str(data))
            if data_size > 1024 * 1024:  # > 1MB
                error_logger.log_error(
                    f"Large SEC data received: {data_size / (1024*1024):.2f}MB",
                    ErrorCategory.SEC_API,
                    ErrorLevel.WARNING,
                    DetailLevel.DETAILED,
                    {"cik": cik, "data_size_mb": data_size / (1024*1024)}
                )
                
                # Force cleanup for large data
                replit_monitor.force_cleanup("Large SEC data processing")
            
            # Cache the financial data for recovery
            session_manager.cache_sec_data(cik, "company_facts", data)
            
            # Monitor resources after processing
            final_check = replit_monitor.check_replit_limits()
            if final_check["status"] != "ok":
                error_logger.log_error(
                    f"Resource usage elevated after processing CIK {cik}",
                    ErrorCategory.SYSTEM,
                    ErrorLevel.WARNING,
                    DetailLevel.DETAILED,
                    final_check
                )
                
            return data
            
        except Exception as e:
            error_logger.log_error(
                f"Critical error in get_company_facts for CIK: {cik}",
                ErrorCategory.SEC_API,
                ErrorLevel.CRITICAL,
                DetailLevel.FORENSIC,
                {"cik": cik, "error": str(e)},
                exception=e
            )
            return None
