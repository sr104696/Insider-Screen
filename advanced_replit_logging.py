"""
Advanced Replit AI Error Logging - Comprehensive Implementation
Based on user's "Robust Error Logging for Replit AI" document
Implements all 5 distinct strategies for holistic debugging
"""

import os
import time
import json
import sys
import uuid
from hashlib import sha256
from collections import Counter
from typing import Dict, Any, List, Optional

# Local DB fallback (as established in previous implementation)
class LocalDBFallback:
    """Local storage fallback when Replit DB not available"""
    def __init__(self):
        self.storage = {}
        self.file_path = 'advanced_logging_cache.json'
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
            # Keep only last 1000 entries to manage storage
            for key in list(self.storage.keys()):
                if isinstance(self.storage[key], list) and len(self.storage[key]) > 1000:
                    self.storage[key] = self.storage[key][-1000:]
            
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

# Try to import replit db, fallback to local storage
try:
    from replit import db
except ImportError:
    db = LocalDBFallback()

class ContextualSnapshotLogger:
    """
    Strategy 1: Contextual Snapshot Logging
    Logs errors with full environment state for systemic issue detection
    """
    
    def __init__(self):
        self.session_id = str(uuid.uuid4())
    
    def log_snapshot(self, stage: str, data: Dict, error: Exception = None, agent_context: str = None):
        """Log comprehensive contextual snapshot"""
        try:
            snapshot = {
                'timestamp': time.time(),
                'session_id': self.session_id,
                'stage': stage,
                'input_hash': self._safe_hash(data),
                'env': {
                    'python_version': sys.version,
                    'repl_id': os.getenv('REPL_ID', 'unknown'),
                    'repl_env': os.getenv('REPL_ENV', 'unknown')
                },
                'agent_context': agent_context or db.get('agent_last_prompt', 'none'),
                'data_snippet': self._safe_data_snippet(data),
                'error': str(error) if error else None,
                'stack': self._safe_traceback() if error else None,
                'success': error is None
            }
            
            # Store in persistent DB
            logs = db.get('contextual_logs', [])
            logs.append(snapshot)
            db['contextual_logs'] = logs
            
            # Console output for immediate visibility
            print(f"📸 SNAPSHOT [{stage}]: {'✅' if not error else '❌'} - Hash: {snapshot['input_hash'][:8]}")
            if error:
                print(f"   Error: {str(error)[:100]}")
                
            return snapshot
            
        except Exception as logging_error:
            # Safe fallback - never crash due to logging
            print(f"⚠️ Logging error in snapshot: {logging_error}")
            return None
    
    def _safe_hash(self, data: Dict) -> str:
        """Create safe hash of input data"""
        try:
            clean_data = {k: str(v)[:200] for k, v in data.items() if not k.startswith('_')}
            return sha256(json.dumps(clean_data, sort_keys=True).encode()).hexdigest()
        except:
            return "hash_error"
    
    def _safe_data_snippet(self, data: Dict) -> Dict:
        """Create safe snippet of data for logging"""
        try:
            snippet = {}
            for k, v in data.items():
                if k in ['password', 'secret', 'key']:
                    snippet[k] = '[REDACTED]'
                else:
                    snippet[k] = str(v)[:100] if v is not None else None
            return snippet
        except:
            return {'error': 'snippet_creation_failed'}
    
    def _safe_traceback(self) -> str:
        """Safely get traceback"""
        try:
            import traceback
            return traceback.format_exc()[:1000]  # Limit size
        except:
            return "traceback_unavailable"

class PredictiveErrorClusterer:
    """
    Strategy 2: Predictive Error Clustering
    Groups similar errors to suggest root causes and refactoring opportunities
    """
    
    def __init__(self):
        self.alert_threshold = 3
    
    def cluster_error(self, stage: str, error: str, ticker: str = None) -> Dict:
        """Cluster errors and detect patterns"""
        try:
            # Create error signature
            error_key = f"{stage}:{error[:50]}"
            if ticker:
                error_key = f"{ticker}:{error_key}"
            
            # Update counts
            counts = db.get('error_counts', {})
            if isinstance(counts, list):  # Handle legacy format
                counts = {}
            counts[error_key] = counts.get(error_key, 0) + 1
            db['error_counts'] = counts
            
            # Check for pattern alerts
            count = counts[error_key]
            if count >= self.alert_threshold:
                alert = {
                    'type': 'pattern_alert',
                    'error_key': error_key,
                    'count': count,
                    'suggestion': self._generate_suggestion(stage, error),
                    'timestamp': time.time()
                }
                
                # Store alert
                alerts = db.get('pattern_alerts', [])
                alerts.append(alert)
                db['pattern_alerts'] = alerts
                
                print(f"🚨 PATTERN ALERT: '{error_key}' occurred {count} times!")
                print(f"💡 SUGGESTION: {alert['suggestion']}")
                
                return alert
            
            return {'type': 'normal', 'count': count}
            
        except Exception as e:
            print(f"⚠️ Error clustering failed: {e}")
            return {'type': 'error', 'error': str(e)}
    
    def _generate_suggestion(self, stage: str, error: str) -> str:
        """Generate Agent-friendly refactoring suggestions"""
        suggestions = {
            'revenue_extraction': 'Check SEC fact mappings - add alternative XBRL tags for revenue',
            'data_processing': 'Add null value handling and default fallbacks',
            'validation': 'Strengthen input validation with clear error messages',
            'calculation': 'Add zero-division and negative value handling',
            'template_render': 'Add template variable existence checks'
        }
        
        for key, suggestion in suggestions.items():
            if key in stage.lower() or key in error.lower():
                return suggestion
                
        return f"Refactor {stage} stage to handle recurring error pattern"

class AgentDrivenLogSummarizer:
    """
    Strategy 3: Agent-Driven Log Summaries
    Creates human-readable insights for Agent analysis and decision-making
    """
    
    def summarize_recent_logs(self, hours: int = 24) -> Dict:
        """Summarize logs from recent time period"""
        try:
            cutoff_time = time.time() - (hours * 3600)
            
            # Get recent contextual logs
            logs = db.get('contextual_logs', [])
            recent_logs = [log for log in logs if log.get('timestamp', 0) > cutoff_time]
            
            # Get recent alerts
            alerts = db.get('pattern_alerts', [])
            recent_alerts = [alert for alert in alerts if alert.get('timestamp', 0) > cutoff_time]
            
            # Create summary
            error_logs = [log for log in recent_logs if log.get('error')]
            success_logs = [log for log in recent_logs if not log.get('error')]
            
            stage_errors = Counter(log['stage'] for log in error_logs)
            error_types = Counter(log['error'][:50] for log in error_logs if log.get('error'))
            
            summary = {
                'period_hours': hours,
                'total_operations': len(recent_logs),
                'successful_operations': len(success_logs),
                'failed_operations': len(error_logs),
                'success_rate': len(success_logs) / len(recent_logs) * 100 if recent_logs else 0,
                'most_problematic_stages': stage_errors.most_common(3),
                'common_errors': error_types.most_common(3),
                'pattern_alerts': len(recent_alerts),
                'timestamp': time.time(),
                'agent_recommendation': self._generate_agent_recommendation(stage_errors, error_types, recent_alerts)
            }
            
            # Store summary
            db['last_summary'] = summary
            
            # Print for immediate visibility
            print("\n📊 AGENT LOG SUMMARY")
            print(f"   Success Rate: {summary['success_rate']:.1f}%")
            print(f"   Failed Operations: {summary['failed_operations']}")
            print(f"   Pattern Alerts: {summary['pattern_alerts']}")
            print(f"   🤖 Agent Recommendation: {summary['agent_recommendation']}")
            
            return summary
            
        except Exception as e:
            print(f"⚠️ Log summarization failed: {e}")
            return {'error': str(e), 'timestamp': time.time()}
    
    def _generate_agent_recommendation(self, stage_errors: Counter, error_types: Counter, alerts: List) -> str:
        """Generate specific recommendations for Agent action"""
        if not stage_errors and not error_types:
            return "System operating normally - no immediate action needed"
        
        if stage_errors:
            top_stage = stage_errors.most_common(1)[0][0]
            return f"Focus on fixing {top_stage} stage - highest error frequency"
        
        if alerts:
            return "Review pattern alerts and implement suggested refactoring"
        
        return "Investigate error types and add appropriate error handling"

class TraceLinkLogger:
    """
    Strategy 4: Trace-Linked Logging
    Links errors across pipeline stages with trace IDs for root cause analysis
    """
    
    def __init__(self):
        self.current_trace_id = None
    
    def start_trace(self, operation: str, ticker: str = None) -> str:
        """Start a new trace for pipeline operation"""
        self.current_trace_id = str(uuid.uuid4())
        
        trace_info = {
            'trace_id': self.current_trace_id,
            'operation': operation,
            'ticker': ticker,
            'start_time': time.time(),
            'stages': []
        }
        
        # Store trace info
        traces = db.get('active_traces', {})
        traces[self.current_trace_id] = trace_info
        db['active_traces'] = traces
        
        print(f"🔍 TRACE STARTED: {operation} - ID: {self.current_trace_id[:8]}")
        return self.current_trace_id
    
    def trace_stage(self, stage: str, data: Dict, error: Exception = None) -> str:
        """Log a stage within current trace"""
        if not self.current_trace_id:
            self.current_trace_id = self.start_trace("unknown_operation")
        
        stage_info = {
            'stage': stage,
            'timestamp': time.time(),
            'data_hash': self._create_hash(data),
            'error': str(error) if error else None,
            'success': error is None
        }
        
        # Update trace
        traces = db.get('active_traces', {})
        if self.current_trace_id in traces:
            traces[self.current_trace_id]['stages'].append(stage_info)
            db['active_traces'] = traces
        
        print(f"🔗 TRACE [{self.current_trace_id[:8]}] {stage}: {'✅' if not error else '❌'}")
        
        return self.current_trace_id
    
    def complete_trace(self, success: bool = True, final_data: Dict = None):
        """Complete current trace"""
        if not self.current_trace_id:
            return
        
        traces = db.get('active_traces', {})
        if self.current_trace_id in traces:
            traces[self.current_trace_id]['end_time'] = time.time()
            traces[self.current_trace_id]['success'] = success
            traces[self.current_trace_id]['duration'] = (
                traces[self.current_trace_id]['end_time'] - 
                traces[self.current_trace_id]['start_time']
            )
            
            # Move to completed traces
            completed_traces = db.get('completed_traces', {})
            completed_traces[self.current_trace_id] = traces[self.current_trace_id]
            db['completed_traces'] = completed_traces
            
            # Remove from active
            del traces[self.current_trace_id]
            db['active_traces'] = traces
        
        print(f"🏁 TRACE COMPLETED: {self.current_trace_id[:8]} - {'✅' if success else '❌'}")
        self.current_trace_id = None
    
    def _create_hash(self, data: Dict) -> str:
        """Create hash for trace correlation"""
        try:
            return sha256(str(sorted(data.items()))[:500].encode()).hexdigest()[:16]
        except:
            return "hash_error"

class SafeModeErrorBuffer:
    """
    Strategy 5: Safe-Mode Error Buffering
    Buffers errors to prevent production database overwrites
    """
    
    def __init__(self):
        self.buffer = []
        self.is_dev = os.getenv('REPL_ENV', 'dev') == 'dev'
        self.buffer_size = 50  # Flush after 50 entries
        
        print(f"🛡️ SafeMode Logger: {'DEV' if self.is_dev else 'PROD'} mode")
    
    def safe_log(self, stage: str, data: Dict, error: Exception = None, metadata: Dict = None):
        """Safely log with buffering"""
        entry = {
            'timestamp': time.time(),
            'stage': stage,
            'error': str(error) if error else None,
            'data_summary': self._safe_summary(data),
            'metadata': metadata or {},
            'success': error is None
        }
        
        self.buffer.append(entry)
        
        # Immediate console output
        print(f"🛡️ SAFE_LOG [{stage}]: {'✅' if not error else '❌'}")
        
        # Flush conditions
        if len(self.buffer) >= self.buffer_size:
            self.flush_buffer()
        
        return entry
    
    def flush_buffer(self, force: bool = False):
        """Flush buffer to persistent storage"""
        if not self.buffer:
            return
        
        if self.is_dev or force:
            try:
                # Add to persistent logs
                safe_logs = db.get('safe_mode_logs', [])
                safe_logs.extend(self.buffer)
                
                # Keep only recent logs to manage storage
                if len(safe_logs) > 2000:
                    safe_logs = safe_logs[-2000:]
                
                db['safe_mode_logs'] = safe_logs
                
                print(f"💾 Flushed {len(self.buffer)} entries to DB")
                self.buffer.clear()
                
            except Exception as e:
                print(f"⚠️ Buffer flush failed: {e}")
        else:
            print(f"🔒 PROD mode: Buffered {len(self.buffer)} entries (not persisted)")
    
    def _safe_summary(self, data: Dict) -> Dict:
        """Create safe summary of data"""
        try:
            return {k: f"{type(v).__name__}({len(str(v))} chars)" for k, v in data.items()}
        except:
            return {'summary_error': True}

# Integrated Advanced Logger Manager
class AdvancedReplitLogger:
    """
    Comprehensive logging manager integrating all 5 strategies
    """
    
    def __init__(self):
        self.contextual = ContextualSnapshotLogger()
        self.clusterer = PredictiveErrorClusterer() 
        self.summarizer = AgentDrivenLogSummarizer()
        self.tracer = TraceLinkLogger()
        self.safe_buffer = SafeModeErrorBuffer()
        
        print("🚀 Advanced Replit Logging System Initialized")
        print("   📸 Contextual Snapshots: Ready")
        print("   🔍 Error Clustering: Ready") 
        print("   📊 Agent Summaries: Ready")
        print("   🔗 Trace Linking: Ready")
        print("   🛡️ Safe Buffering: Ready")
    
    def log_comprehensive(self, stage: str, data: Dict, error: Exception = None, 
                         ticker: str = None, agent_context: str = None):
        """Comprehensive logging using all strategies"""
        
        # Strategy 1: Contextual snapshot
        snapshot = self.contextual.log_snapshot(stage, data, error, agent_context)
        
        # Strategy 2: Error clustering (if error)
        if error:
            cluster_result = self.clusterer.cluster_error(stage, str(error), ticker)
        
        # Strategy 3: Trace linking
        self.tracer.trace_stage(stage, data, error)
        
        # Strategy 4: Safe buffering
        self.safe_buffer.safe_log(stage, data, error, {
            'ticker': ticker,
            'trace_id': self.tracer.current_trace_id,
            'snapshot_id': snapshot.get('input_hash') if snapshot else None
        })
        
        return {
            'snapshot': snapshot,
            'cluster': cluster_result if error else None,
            'trace_id': self.tracer.current_trace_id
        }
    
    def start_operation_trace(self, operation: str, ticker: str = None) -> str:
        """Start comprehensive tracing for an operation"""
        return self.tracer.start_trace(operation, ticker)
    
    def complete_operation(self, success: bool = True):
        """Complete operation and generate summary"""
        self.tracer.complete_trace(success)
        self.safe_buffer.flush_buffer()
        
        # Generate summary if dev mode
        if self.safe_buffer.is_dev:
            return self.summarizer.summarize_recent_logs(1)  # Last hour
    
    def get_agent_insights(self) -> Dict:
        """Get comprehensive insights for Agent analysis"""
        return {
            'summary': db.get('last_summary', {}),
            'alerts': db.get('pattern_alerts', [])[-5:],  # Last 5 alerts
            'error_counts': db.get('error_counts', {}),
            'active_traces': db.get('active_traces', {})
        }