# Advanced Scraper Client integrated with our financial data system
# Based on the comprehensive scraper_client architecture provided by user

import time
import json
import random
import requests
from typing import Optional, Dict, Any, List, Callable

# Circuit Breaker for fault tolerance
class CircuitBreaker:
    def __init__(self, fail_threshold: int = 5, reset_timeout: int = 60):
        self.fail_threshold = fail_threshold
        self.reset_timeout = reset_timeout
        self.fail_count = {}
        self.open_until = {}

    def allow(self, key: str) -> bool:
        until = self.open_until.get(key)
        if until is None:
            return True
        return time.time() > until

    def record_success(self, key: str):
        self.fail_count[key] = 0
        self.open_until.pop(key, None)

    def record_failure(self, key: str):
        c = self.fail_count.get(key, 0) + 1
        self.fail_count[key] = c
        if c >= self.fail_threshold:
            self.open_until[key] = time.time() + self.reset_timeout

# Advanced proxy manager
class AdvancedProxyManager:
    def __init__(self):
        self.proxies = []
        self.current_index = 0
        
    def add_proxy(self, proxy_url: str):
        self.proxies.append(proxy_url)
        
    def get_proxy(self) -> Optional[str]:
        if not self.proxies:
            return None
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy

# Cloudflare bypass strategies
class CloudflareBypassManager:
    def __init__(self, logger=None):
        self.logger = logger
        self.strategies = ['basic', 'stealth', 'advanced']
        
    def detect_cloudflare(self, response_text: str, headers: Dict) -> bool:
        """Detect if response contains Cloudflare protection"""
        cf_indicators = [
            'cloudflare',
            'cf-ray',
            'checking your browser',
            'ddos protection',
            'ray id',
            'performance & security by cloudflare'
        ]
        
        text_lower = response_text.lower() if response_text else ""
        header_str = str(headers).lower()
        
        return any(indicator in text_lower or indicator in header_str 
                  for indicator in cf_indicators)
    
    def bypass_cloudflare(self, url: str, session: requests.Session) -> Optional[Dict]:
        """Attempt Cloudflare bypass using multiple strategies"""
        
        for strategy in self.strategies:
            try:
                if strategy == 'basic':
                    return self._basic_bypass(url, session)
                elif strategy == 'stealth':
                    return self._stealth_bypass(url, session)
                elif strategy == 'advanced':
                    return self._advanced_bypass(url, session)
                    
            except Exception as e:
                if self.logger:
                    self.logger.log_comprehensive('cloudflare_bypass_attempt',
                                                {'strategy': strategy, 'error': str(e)[:200]},
                                                e)
                continue
                
        return None
    
    def _basic_bypass(self, url: str, session: requests.Session) -> Dict:
        """Basic bypass with enhanced headers"""
        enhanced_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        time.sleep(random.uniform(2, 5))  # Human-like delay
        response = session.get(url, headers=enhanced_headers, timeout=30)
        
        return {
            'status_code': response.status_code,
            'headers': dict(response.headers),
            'text': response.text
        }
    
    def _stealth_bypass(self, url: str, session: requests.Session) -> Dict:
        """Stealth bypass with randomized behavior"""
        stealth_headers = {
            'User-Agent': random.choice([
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
            ]),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': random.choice(['en-US,en;q=0.9', 'en-GB,en;q=0.9', 'en-US,en;q=0.8']),
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
        }
        
        # Random delay to mimic human behavior
        time.sleep(random.uniform(3, 8))
        
        response = session.get(url, headers=stealth_headers, timeout=30)
        
        return {
            'status_code': response.status_code,
            'headers': dict(response.headers),
            'text': response.text
        }
    
    def _advanced_bypass(self, url: str, session: requests.Session) -> Dict:
        """Advanced bypass with multiple request simulation"""
        # First request - simulate browsing behavior
        base_domain = '/'.join(url.split('/')[:3])
        
        # Simulate visiting home page first
        try:
            session.get(base_domain, timeout=15)
            time.sleep(random.uniform(1, 3))
        except:
            pass
        
        # Main request with full stealth
        advanced_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': base_domain,
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Chromium";v="117", "Not;A=Brand";v="8", "Google Chrome";v="117"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
        }
        
        time.sleep(random.uniform(2, 4))
        response = session.get(url, headers=advanced_headers, timeout=30)
        
        return {
            'status_code': response.status_code,
            'headers': dict(response.headers),
            'text': response.text
        }

# Main Advanced Scraper Client
class AdvancedScraperClient:
    def __init__(self, logger=None):
        self.logger = logger
        self.proxy_manager = AdvancedProxyManager()
        self.circuit_breaker = CircuitBreaker(fail_threshold=3, reset_timeout=60)
        self.cloudflare_bypass = CloudflareBypassManager(logger)
        self.max_retries = 3
        self.base_timeout = 30.0
        
    def _jitter_delay(self, base: float, factor: float = 0.5) -> float:
        """Add jitter to delays for more natural behavior"""
        lo = base * (1.0 - factor)
        hi = base * (1.0 + factor)
        return random.uniform(lo, hi)
    
    def request(self, method: str, url: str, headers: Optional[Dict[str, str]] = None, 
               data: Any = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Advanced request with circuit breaker, retries, and Cloudflare bypass"""
        
        target_key = url.split("/")[2] if "://" in url else url
        
        # Check circuit breaker
        if not self.circuit_breaker.allow(target_key):
            raise Exception(f"Circuit breaker open for {target_key}")
        
        session = requests.Session()
        attempt = 0
        last_exception = None
        
        while attempt <= self.max_retries:
            attempt += 1
            
            try:
                # Get proxy if available
                proxy = self.proxy_manager.get_proxy()
                if proxy:
                    session.proxies = {'http': proxy, 'https': proxy}
                
                # Merge headers
                request_headers = headers.copy() if headers else {}
                
                # Make request
                response = session.request(
                    method=method,
                    url=url,
                    headers=request_headers,
                    data=data,
                    params=params,
                    timeout=self.base_timeout,
                    allow_redirects=True
                )
                
                # Check if response is successful
                if response.status_code < 400:
                    self.circuit_breaker.record_success(target_key)
                    
                    return {
                        'status': response.status_code,
                        'headers': dict(response.headers),
                        'text': response.text,
                        'url': response.url
                    }
                
                # Check for Cloudflare protection
                if response.status_code in [403, 429, 503, 520, 521, 522, 523, 524]:
                    if self.cloudflare_bypass.detect_cloudflare(response.text, response.headers):
                        if self.logger:
                            self.logger.log_comprehensive('cloudflare_detected',
                                                        {'url': url, 'status': response.status_code},
                                                        url=url)
                        
                        # Attempt Cloudflare bypass
                        bypass_result = self.cloudflare_bypass.bypass_cloudflare(url, session)
                        if bypass_result and bypass_result.get('status_code', 0) < 400:
                            self.circuit_breaker.record_success(target_key)
                            return {
                                'status': bypass_result['status_code'],
                                'headers': bypass_result['headers'],
                                'text': bypass_result['text'],
                                'url': url,
                                'bypass_used': True
                            }
                
                # Record failure
                self.circuit_breaker.record_failure(target_key)
                
            except Exception as e:
                last_exception = e
                self.circuit_breaker.record_failure(target_key)
                
                if self.logger:
                    self.logger.log_comprehensive('advanced_scraper_error',
                                                {'url': url, 'attempt': attempt, 'error': str(e)[:200]},
                                                e)
            
            # Exponential backoff with jitter
            if attempt <= self.max_retries:
                delay = self._jitter_delay(2.0 ** attempt)
                time.sleep(min(delay, 10.0))  # Cap at 10 seconds
        
        # All retries exhausted
        if last_exception:
            raise last_exception
        raise Exception(f"Request failed after {self.max_retries} retries")

# Job Queue Manager for bulk operations
class AdvancedJobQueue:
    def __init__(self, scraper_client: AdvancedScraperClient, max_workers: int = 5):
        self.client = scraper_client
        self.max_workers = max_workers
        self.results = []
        
    def add_job(self, method: str, url: str, **kwargs):
        """Add a scraping job to the queue"""
        return {
            'method': method,
            'url': url,
            'kwargs': kwargs
        }
    
    def process_jobs(self, jobs: List[Dict]) -> List[Dict]:
        """Process jobs sequentially (can be enhanced with threading/async)"""
        results = []
        
        for i, job in enumerate(jobs):
            try:
                if self.client.logger:
                    self.client.logger.log_comprehensive('job_processing',
                                                       {'job_number': i+1, 'total_jobs': len(jobs),
                                                        'url': job['url']})
                
                result = self.client.request(
                    job['method'],
                    job['url'],
                    **job.get('kwargs', {})
                )
                
                result['job_id'] = i
                result['success'] = True
                results.append(result)
                
                # Rate limiting
                time.sleep(random.uniform(0.5, 2.0))
                
            except Exception as e:
                results.append({
                    'job_id': i,
                    'success': False,
                    'error': str(e),
                    'url': job['url']
                })
        
        return results