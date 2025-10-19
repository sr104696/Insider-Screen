"""
AI-Safe Error Handler - Optimized for Replit AI Collaboration

CRITICAL AI SAFETY CHANGES:
1. Simplified interface to prevent AI confusion
2. Built-in safeguards against infinite retry loops
3. Clear error categories that AI can understand and act on
4. Cannot trigger destructive operations
5. Transparent logging that helps AI debug issues

MAINTAINS EXCELLENT DEBUGGING CAPABILITIES:
- Multi-level error categorization preserved
- Comprehensive context capture
- Session tracking for analysis
- All sophisticated debugging features kept
"""

import logging
import traceback
import time
import json
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum

class ErrorSeverity(Enum):
    """Simplified severity levels - easy for AI to understand"""
    CRITICAL = "CRITICAL"  # App cannot continue - AI should stop and ask for help
    ERROR = "ERROR"        # Feature failed - AI should try alternative approach  
    WARNING = "WARNING"    # Unexpected but recoverable - AI can continue with caution
    INFO = "INFO"          # Normal operation - AI can proceed confidently

class ErrorType(Enum):
    """Clear error categories for AI decision making"""
    NETWORK = "NETWORK"           # API calls, connectivity issues
    SEC_API = "SEC_API"           # SEC EDGAR specific problems  
    DATA_PROCESSING = "DATA_PROCESSING"  # Data parsing, validation issues
    USER_INPUT = "USER_INPUT"     # Input validation, format problems
    RESOURCE = "RESOURCE"         # Memory, storage, performance issues
    UNKNOWN = "UNKNOWN"           # Uncategorized errors

class AISafeErrorHandler:
    """
    Simplified error handler optimized for AI collaboration
    
    KEY AI SAFETY FEATURES:
    - Simple, consistent interface
    - Clear error categories for AI decision making
    - Built-in operation limits to prevent loops
    - Cannot perform destructive operations
    - Transparent status reporting
    """
    
    def __init__(self, app_name: str = "SEC_Financial_Analysis"):
        self.app_name = app_name
        self.session_id = f"{app_name}_{int(time.time())}"
        
        # AI-SAFE CONFIGURATION
        self.max_errors_per_type = 10  # Prevent infinite error loops
        self.max_session_errors = 50   # Circuit breaker for entire session
        
        # Error tracking
        self.error_counts = {error_type.value: 0 for error_type in ErrorType}
        self.session_errors = []
        self.last_error_time = 0
        self.consecutive_errors = 0
        
        # Setup simple logging
        self.logger = logging.getLogger(f"{app_name}.ai_safe")
        self.logger.setLevel(logging.INFO)
        
        # Console handler for immediate feedback
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter(
                '%(asctime)s | %(levelname)s | %(message)s',
                datefmt='%H:%M:%S'
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        print(f"🛡️ AI-Safe Error Handler initialized for {app_name}")

    def log_error(self, 
                  message: str,
                  error_type: ErrorType = ErrorType.UNKNOWN,
                  severity: ErrorSeverity = ErrorSeverity.ERROR,
                  context: Optional[Dict[str, Any]] = None,
                  exception: Optional[Exception] = None) -> str:
        """
        MAIN ERROR LOGGING METHOD - AI Friendly
        
        Returns error_id for tracking, or 'CIRCUIT_BREAKER' if limits exceeded
        """
        
        # AI SAFETY: Circuit breaker to prevent infinite loops
        if self._should_trigger_circuit_breaker(error_type):
            return self._handle_circuit_breaker(error_type)
        
        # Generate error ID
        error_id = f"{error_type.value}_{len(self.session_errors):03d}"
        timestamp = datetime.now().isoformat()
        
        # Build error record
        error_record = {
            "id": error_id,
            "timestamp": timestamp,
            "session_id": self.session_id,
            "message": message,
            "type": error_type.value,
            "severity": severity.value,
            "context": context or {},
            "consecutive_count": self.consecutive_errors
        }
        
        # Add exception details if provided
        if exception:
            error_record["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "traceback": traceback.format_exception(type(exception), exception, exception.__traceback__)
            }
        
        # Track error
        self.session_errors.append(error_record)
        self.error_counts[error_type.value] += 1
        self.last_error_time = time.time()
        
        # Update consecutive count
        if len(self.session_errors) > 1 and self.session_errors[-2]["severity"] != "INFO":
            self.consecutive_errors += 1
        else:
            self.consecutive_errors = 1
        
        # Log to console
        log_method = getattr(self.logger, severity.value.lower())
        formatted_message = f"[{error_id}] {message}"
        if context:
            formatted_message += f" | Context: {json.dumps(context, default=str)}"
        
        log_method(formatted_message)
        
        return error_id
    
    def _should_trigger_circuit_breaker(self, error_type: ErrorType) -> bool:
        """Check if circuit breaker should trigger for this error type"""
        
        # Check type-specific limits
        if self.error_counts[error_type.value] >= self.max_errors_per_type:
            return True
        
        # Check session-wide limits
        if len(self.session_errors) >= self.max_session_errors:
            return True
        
        # Check consecutive error pattern
        if self.consecutive_errors >= 5:
            return True
        
        return False
    
    def _handle_circuit_breaker(self, error_type: ErrorType) -> str:
        """Handle circuit breaker trigger"""
        
        circuit_breaker_msg = f"🚨 CIRCUIT BREAKER TRIGGERED for {error_type.value}"
        
        # Log circuit breaker activation with traceback
        self.logger.exception(circuit_breaker_msg)
        
        print(f"\n{circuit_breaker_msg}")
        print(f"Error counts: {self.error_counts}")
        print(f"Consecutive errors: {self.consecutive_errors}")
        print(f"Total session errors: {len(self.session_errors)}")
        print("\n💡 RECOMMENDATION: Reset error handler or restart session")
        
        return "CIRCUIT_BREAKER"
    
    def reset_error_handler(self):
        """
        AI-SAFE: Reset error handler to clear circuit breaker
        Useful when AI gets stuck in error loops
        """
        self.error_counts = {error_type.value: 0 for error_type in ErrorType}
        self.consecutive_errors = 0
        print("🔄 Error handler reset - circuit breaker cleared")
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get current error status for AI transparency"""
        
        return {
            "session_id": self.session_id,
            "total_errors": len(self.session_errors),
            "error_counts": self.error_counts,
            "consecutive_errors": self.consecutive_errors,
            "last_error_time": self.last_error_time,
            "circuit_breaker_risk": {
                "high_risk_types": [
                    error_type for error_type, count in self.error_counts.items() 
                    if count >= self.max_errors_per_type * 0.8
                ],
                "session_limit_approached": len(self.session_errors) >= self.max_session_errors * 0.8,
                "consecutive_risk": self.consecutive_errors >= 3
            },
            "recent_errors": self.session_errors[-5:] if self.session_errors else []
        }
    
    def is_operational(self) -> bool:
        """
        AI-SIMPLE: Single boolean for operational status
        """
        return (
            len(self.session_errors) < self.max_session_errors and
            self.consecutive_errors < 5 and
            all(count < self.max_errors_per_type for count in self.error_counts.values())
        )
    
    def get_simple_status(self) -> str:
        """
        AI-FRIENDLY: Human readable status for AI understanding
        """
        if not self.is_operational():
            return f"🚨 ERROR HANDLER: Circuit breaker active - {self.consecutive_errors} consecutive errors"
        elif self.consecutive_errors >= 3:
            return f"⚠️ ERROR HANDLER: {self.consecutive_errors} consecutive errors - monitor closely"
        elif len(self.session_errors) > 0:
            return f"✅ ERROR HANDLER: {len(self.session_errors)} total errors - operational"
        else:
            return "✅ ERROR HANDLER: No errors - fully operational"

# Global instance for easy AI access
error_handler = AISafeErrorHandler()

# AI-FRIENDLY CONVENIENCE FUNCTIONS
def log_sec_error(message: str, context: Optional[Dict] = None) -> str:
    return error_handler.log_error(message, ErrorType.SEC_API, ErrorSeverity.ERROR, context)

def log_network_error(message: str, context: Optional[Dict] = None) -> str:
    return error_handler.log_error(message, ErrorType.NETWORK, ErrorSeverity.ERROR, context)

def log_user_error(message: str, context: Optional[Dict] = None) -> str:
    return error_handler.log_error(message, ErrorType.USER_INPUT, ErrorSeverity.INFO, context)

def log_critical(message: str, context: Optional[Dict] = None) -> str:
    return error_handler.log_error(message, ErrorType.UNKNOWN, ErrorSeverity.CRITICAL, context)