"""
Replit AI-Safe Error Handler for SEC Financial Analysis
Implements holistic debugging paradigms from user guidance document
"""

import os
import time
import json
import random
from typing import Dict, Any, List, Optional
from collections import Counter
import logging

# Replit DB fallback for environments without replit module
class LocalDBFallback:
    """Local storage fallback when Replit DB not available"""
    def __init__(self):
        self.storage = {}
        self.file_path = 'sec_pipeline_cache.json'
        self._load_from_file()
    
    def _load_from_file(self):
        try:
            if os.path.exists(self.file_path):
                with open(self.file_path, 'r') as f:
                    self.storage = json.load(f)
        except Exception:
            self.storage = {}
    
    def _save_to_file(self):
        try:
            with open(self.file_path, 'w') as f:
                json.dump(self.storage, f, indent=2)
        except Exception:
            pass
    
    def __getitem__(self, key):
        return self.storage[key]
    
    def __setitem__(self, key, value):
        self.storage[key] = value
        self._save_to_file()
    
    def get(self, key, default=None):
        return self.storage.get(key, default)
    
    def keys(self):
        return self.storage.keys()
    
    def __contains__(self, key):
        return key in self.storage

# Try to import replit db, fallback to local storage
try:
    from replit import db
except ImportError:
    db = LocalDBFallback()

class SafeChaosSimulator:
    """Prompt-driven error simulation without runtime overhead"""
    
    def __init__(self, enabled: bool = os.getenv('CHAOS_MODE', 'off') == 'on'):
        self.enabled = enabled
        self.failure_rate = 0.02  # Very low to avoid Agent loops
        
    def simulate_sec_data_failure(self, func, *args, **kwargs):
        """Simulate SEC data extraction failures safely"""
        if not self.enabled:
            return func(*args, **kwargs)
            
        if random.random() < self.failure_rate:
            error_type = random.choice(['missing_revenue', 'invalid_xbrl', 'network_timeout'])
            if error_type == 'missing_revenue':
                raise ValueError("Simulated: Revenue fact not found (check fact mappings)")
            elif error_type == 'invalid_xbrl':
                raise KeyError("Simulated: XBRL tag mismatch (use alternative facts)")
            else:
                raise IOError("Simulated: SEC API timeout (implement retry logic)")
        
        return func(*args, **kwargs)

class AgentEnforcedErrorBoundaries:
    """Simplified contracts with built-in validation for Agent collaboration"""
    
    @staticmethod
    def safe_extract_revenue(raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract revenue with Agent-friendly error boundaries"""
        try:
            facts = raw_data.get('facts', {}).get('us-gaap', {})
            assert facts, "Missing US-GAAP facts in SEC data"
            
            # Check for revenue facts in priority order
            revenue_facts = [
                'RevenueFromContractWithCustomerExcludingAssessedTax',
                'Revenues',
                'SalesRevenueNet',
                'ServiceRevenues'
            ]
            
            found_revenue = None
            for fact_name in revenue_facts:
                if fact_name in facts and facts[fact_name].get('units', {}).get('USD'):
                    found_revenue = fact_name
                    break
            
            assert found_revenue, f"No revenue facts found. Available facts: {list(facts.keys())[:10]}"
            
            revenue_data = facts[found_revenue]['units']['USD']
            assert revenue_data, f"No USD data for {found_revenue}"
            
            return {
                'success': True, 
                'result': {
                    'fact_name': found_revenue,
                    'data_points': len(revenue_data),
                    'revenue_entries': revenue_data
                }
            }
            
        except AssertionError as e:
            return {
                'success': False,
                'error': str(e),
                'stage': 'revenue_extraction',
                'suggestion': 'Check SEC fact mappings or try alternative XBRL tags'
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}",
                'stage': 'revenue_extraction',
                'suggestion': 'Review raw SEC data structure'
            }

class CheckpointTemporalLogger:
    """Persistent replay with Replit DB for cross-session debugging"""
    
    def __init__(self):
        self.session_id = str(int(time.time()))
        
    def log_pipeline_event(self, stage: str, ticker: str, data: Dict, error: str = None):
        """Log events to Replit DB for persistence across sessions"""
        event = {
            "timestamp": time.time(),
            "session_id": self.session_id,
            "stage": stage,
            "ticker": ticker,
            "data_summary": self._safe_data_summary(data),
            "error": error,
            "success": error is None
        }
        
        # Store in Replit DB
        events_key = f'sec_pipeline_events'
        if events_key not in db:
            db[events_key] = []
        
        events = db[events_key]
        events.append(event)
        
        # Keep only last 100 events to avoid storage limits
        if len(events) > 100:
            events = events[-100:]
        
        db[events_key] = events
        
    def _safe_data_summary(self, data: Dict) -> str:
        """Create safe summary of data for logging"""
        if isinstance(data, dict):
            return f"Dict with {len(data)} keys: {list(data.keys())[:5]}"
        return f"Type: {type(data)}, Length: {len(str(data)[:100])}"
    
    def replay_events(self, ticker: str = None, stage: str = None) -> List[Dict]:
        """Replay events for debugging"""
        events = db.get('sec_pipeline_events', [])
        
        filtered_events = []
        for event in events:
            if ticker and event['ticker'] != ticker:
                continue
            if stage and event['stage'] != stage:
                continue
            filtered_events.append(event)
            
        return filtered_events

class AgentAnomalyDetector:
    """Lightweight anomaly detection for SEC data quality issues"""
    
    def __init__(self):
        self.error_patterns = Counter()
        
    def detect_sec_data_anomalies(self, ticker: str, financial_data: Dict):
        """Detect common SEC data quality issues"""
        anomalies = []
        
        # Check for missing core metrics
        annual_data = financial_data.get('periods', {}).get('annual', [])
        quarterly_data = financial_data.get('periods', {}).get('quarterly', [])
        
        if not annual_data:
            anomalies.append({
                'type': 'missing_annual_data',
                'ticker': ticker,
                'severity': 'critical',
                'suggestion': 'Check SEC fact mappings for annual periods'
            })
            
        if not quarterly_data:
            anomalies.append({
                'type': 'missing_quarterly_data', 
                'ticker': ticker,
                'severity': 'warning',
                'suggestion': 'Check SEC fact mappings for quarterly periods'
            })
        
        # Check for revenue holes
        revenue_holes = 0
        for period in annual_data:
            if not period.get('revenue'):
                revenue_holes += 1
                
        if revenue_holes > len(annual_data) * 0.5:  # More than 50% missing
            anomalies.append({
                'type': 'revenue_extraction_failure',
                'ticker': ticker,
                'severity': 'critical',
                'missing_periods': revenue_holes,
                'suggestion': 'Revenue fact mapping likely incorrect for this company'
            })
            
        # Store patterns in Replit DB
        if anomalies:
            db[f'anomalies_{ticker}'] = {
                'timestamp': time.time(),
                'anomalies': anomalies
            }
            
        return anomalies

class ReplitEcosystemObservability:
    """Agent-traced pipeline stages with safety rails"""
    
    def __init__(self):
        self.traces = {}
        
    def trace_pipeline_stage(self, stage_name: str, ticker: str, func, *args, **kwargs):
        """Trace pipeline stages with automatic error recovery"""
        start_time = time.perf_counter()
        trace_key = f"{ticker}_{stage_name}"
        
        try:
            result = func(*args, **kwargs)
            duration = time.perf_counter() - start_time
            
            # Store successful trace
            trace_data = {
                'ticker': ticker,
                'stage': stage_name,
                'duration': duration,
                'status': 'success',
                'timestamp': time.time()
            }
            
            db[f'trace_{trace_key}'] = trace_data
            return result
            
        except Exception as e:
            duration = time.perf_counter() - start_time
            
            # Store failed trace
            trace_data = {
                'ticker': ticker,
                'stage': stage_name,
                'duration': duration,
                'status': 'failure',
                'error': str(e),
                'timestamp': time.time()
            }
            
            db[f'trace_{trace_key}'] = trace_data
            
            # Log for Agent analysis
            logging.error(f"Pipeline stage {stage_name} failed for {ticker}: {str(e)}")
            raise
            
    def get_pipeline_performance(self, ticker: str) -> Dict:
        """Get performance metrics for Agent optimization"""
        traces = []
        for key in db.keys():
            if key.startswith(f'trace_{ticker}_'):
                traces.append(db[key])
                
        return {
            'ticker': ticker,
            'total_stages': len(traces),
            'successful_stages': len([t for t in traces if t['status'] == 'success']),
            'failed_stages': len([t for t in traces if t['status'] == 'failure']),
            'avg_duration': sum(t['duration'] for t in traces) / len(traces) if traces else 0,
            'traces': traces
        }

# Integrated SEC Pipeline Safety Manager
class SECPipelineSafetyManager:
    """Holistic safety manager combining all paradigms"""
    
    def __init__(self):
        self.chaos_sim = SafeChaosSimulator()
        self.error_boundaries = AgentEnforcedErrorBoundaries()
        self.logger = CheckpointTemporalLogger()
        self.anomaly_detector = AgentAnomalyDetector()
        self.observability = ReplitEcosystemObservability()
        
    def safe_process_ticker(self, ticker: str, raw_sec_data: Dict) -> Dict[str, Any]:
        """Process ticker with full safety paradigms"""
        
        def _extract_and_process():
            # Stage 1: Revenue Extraction
            self.logger.log_pipeline_event("revenue_extraction_start", ticker, raw_sec_data)
            revenue_result = self.error_boundaries.safe_extract_revenue(raw_sec_data)
            
            if not revenue_result['success']:
                self.logger.log_pipeline_event("revenue_extraction_error", ticker, 
                                             revenue_result, revenue_result['error'])
                return revenue_result
                
            self.logger.log_pipeline_event("revenue_extraction_success", ticker, revenue_result)
            return revenue_result
        
        # Trace the entire process
        return self.observability.trace_pipeline_stage(
            "full_pipeline", ticker, _extract_and_process
        )