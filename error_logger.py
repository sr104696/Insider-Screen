"""
Comprehensive Error Logging System for SEC Financial Analysis Tool
Designed for Replit environment constraints and debugging needs
"""

import logging
import traceback
import time
import json
from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
import sys
import os

class ErrorLevel(Enum):
    """Error severity levels with specific meanings"""
    CRITICAL = "CRITICAL"  # App cannot continue
    ERROR = "ERROR"        # Feature fails but app continues
    WARNING = "WARNING"    # Unexpected but recoverable
    INFO = "INFO"          # Normal operation tracking
    DEBUG = "DEBUG"        # Detailed debugging info

class ErrorCategory(Enum):
    """Categorization of error types for filtering and analysis"""
    NETWORK = "NETWORK"           # External API calls, timeouts, connectivity
    SEC_API = "SEC_API"          # SEC EDGAR specific errors
    DATA_PROCESSING = "DATA_PROCESSING"  # Data parsing, validation, transformation
    USER_INPUT = "USER_INPUT"    # Validation, formatting issues
    SYSTEM = "SYSTEM"            # Replit platform, Flask, infrastructure
    CALCULATION = "CALCULATION"   # Financial calculations, growth metrics
    UNKNOWN = "UNKNOWN"          # Uncategorized errors

class DetailLevel(Enum):
    """Level of detail in error reporting"""
    MINIMAL = 1    # Error message only
    STANDARD = 2   # Error + context
    DETAILED = 3   # Error + context + request data
    FORENSIC = 4   # Everything + stack traces + environment

class SECFinancialErrorLogger:
    """
    Highly calibrated error logging system with multiple detail levels
    Designed specifically for SEC financial data analysis debugging
    """
    
    def __init__(self, app_name: str = "SEC_Financial_Analysis"):
        self.app_name = app_name
        self.session_id = f"{app_name}_{int(time.time())}"
        
        # Configure multiple loggers for different purposes
        self.setup_loggers()
        
        # Error tracking for analysis
        self.error_counts = {category.value: 0 for category in ErrorCategory}
        self.session_errors = []
        
        # Performance tracking
        self.request_times = []
        self.api_call_times = []
        
        # Environment info for debugging
        self.environment_info = self._get_environment_info()
        
    def setup_loggers(self):
        """Setup multiple specialized loggers"""
        
        # Main application logger
        self.main_logger = logging.getLogger(f"{self.app_name}.main")
        self.main_logger.setLevel(logging.DEBUG)
        
        # Network/API specific logger
        self.network_logger = logging.getLogger(f"{self.app_name}.network")
        self.network_logger.setLevel(logging.DEBUG)
        
        # Data processing logger
        self.data_logger = logging.getLogger(f"{self.app_name}.data")
        self.data_logger.setLevel(logging.DEBUG)
        
        # User interaction logger
        self.user_logger = logging.getLogger(f"{self.app_name}.user")
        self.user_logger.setLevel(logging.INFO)
        
        # Create formatters for different detail levels
        self.detailed_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s'
        )
        
        self.standard_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s'
        )
        
        # Setup console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(self.detailed_formatter)
        
        # Add handlers to all loggers
        for logger in [self.main_logger, self.network_logger, self.data_logger, self.user_logger]:
            logger.addHandler(console_handler)
    
    def _get_environment_info(self) -> Dict[str, Any]:
        """Collect environment information for debugging"""
        return {
            "platform": sys.platform,
            "python_version": sys.version,
            "is_replit": "REPL_ID" in os.environ,
            "replit_id": os.environ.get("REPL_ID", "N/A"),
            "timestamp": datetime.now().isoformat(),
            "working_directory": os.getcwd(),
            "environment_vars": {
                key: value for key, value in os.environ.items() 
                if key.startswith(('REPL_', 'FLASK_', 'SEC_'))
            }
        }
    
    def log_error(self, 
                  message: str,
                  category: ErrorCategory,
                  level: ErrorLevel = ErrorLevel.ERROR,
                  detail_level: DetailLevel = DetailLevel.STANDARD,
                  context: Optional[Dict[str, Any]] = None,
                  exception: Optional[Exception] = None,
                  request_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Primary error logging method with comprehensive detail levels
        
        Returns: Unique error ID for tracking
        """
        
        error_id = f"{category.value}_{int(time.time())}_{len(self.session_errors)}"
        timestamp = datetime.now().isoformat()
        
        # Build error record
        error_record = {
            "error_id": error_id,
            "timestamp": timestamp,
            "session_id": self.session_id,
            "level": level.value,
            "category": category.value,
            "message": message,
            "detail_level": detail_level.value
        }
        
        # Add context based on detail level
        if detail_level.value >= DetailLevel.STANDARD.value and context is not None:
            error_record["context"] = context
            
        if detail_level.value >= DetailLevel.DETAILED.value and request_data is not None:
            error_record["request_data"] = request_data
            
        if detail_level.value >= DetailLevel.FORENSIC.value:
            error_record["environment"] = self.environment_info
            error_record["stack_trace"] = traceback.format_stack()
            
        if exception:
            error_record["exception"] = {
                "type": type(exception).__name__,
                "message": str(exception),
                "traceback": traceback.format_exception(type(exception), exception, exception.__traceback__)
            }
        
        # Store for analysis
        self.session_errors.append(error_record)
        self.error_counts[category.value] += 1
        
        # Log to appropriate logger
        logger = self._get_logger_for_category(category)
        log_method = getattr(logger, level.value.lower())
        
        # Format message based on detail level
        formatted_message = self._format_error_message(error_record, detail_level)
        log_method(formatted_message)
        
        return error_id
    
    def _get_logger_for_category(self, category: ErrorCategory) -> logging.Logger:
        """Route errors to appropriate specialized logger"""
        if category in [ErrorCategory.NETWORK, ErrorCategory.SEC_API]:
            return self.network_logger
        elif category == ErrorCategory.DATA_PROCESSING:
            return self.data_logger
        elif category == ErrorCategory.USER_INPUT:
            return self.user_logger
        else:
            return self.main_logger
    
    def _format_error_message(self, error_record: Dict[str, Any], detail_level: DetailLevel) -> str:
        """Format error message based on detail level"""
        
        base_msg = f"[{error_record['error_id']}] {error_record['message']}"
        
        if detail_level == DetailLevel.MINIMAL:
            return base_msg
            
        elif detail_level == DetailLevel.STANDARD:
            context_str = ""
            if "context" in error_record:
                context_str = f" | Context: {json.dumps(error_record['context'], default=str)}"
            return f"{base_msg}{context_str}"
            
        elif detail_level == DetailLevel.DETAILED:
            details = []
            if "context" in error_record:
                details.append(f"Context: {json.dumps(error_record['context'], default=str)}")
            if "request_data" in error_record:
                details.append(f"Request: {json.dumps(error_record['request_data'], default=str)}")
            detail_str = " | ".join(details)
            return f"{base_msg} | {detail_str}"
            
        else:  # FORENSIC
            return f"{base_msg} | FULL_RECORD: {json.dumps(error_record, default=str, indent=2)}"
    
    # Specialized logging methods for common SEC analysis scenarios
    
    def log_sec_api_error(self, endpoint: str, status_code: int, response_text: str, 
                         ticker: str = None, cik: str = None, detail_level: DetailLevel = DetailLevel.DETAILED):
        """Log SEC API specific errors with relevant context"""
        context = {
            "endpoint": endpoint,
            "status_code": status_code,
            "response_preview": response_text[:500] if response_text else "No response",
            "ticker": ticker or "Unknown",
            "cik": cik or "Unknown"
        }
        
        if status_code == 403:
            message = f"SEC API Rate Limit Exceeded - Endpoint: {endpoint}"
            level = ErrorLevel.WARNING
        elif status_code == 404:
            message = f"SEC API Resource Not Found - Endpoint: {endpoint}"
            level = ErrorLevel.ERROR
        elif status_code >= 500:
            message = f"SEC API Server Error - Endpoint: {endpoint}"
            level = ErrorLevel.CRITICAL
        else:
            message = f"SEC API Error - Status: {status_code} - Endpoint: {endpoint}"
            level = ErrorLevel.ERROR
            
        return self.log_error(message, ErrorCategory.SEC_API, level, detail_level, context)
    
    def log_network_timeout(self, url: str, timeout_duration: float, attempt_number: int,
                           detail_level: DetailLevel = DetailLevel.DETAILED):
        """Log network timeout with retry context"""
        context = {
            "url": url,
            "timeout_duration": timeout_duration,
            "attempt_number": attempt_number,
            "total_attempts_in_session": len([e for e in self.session_errors if "timeout" in e.get("message", "").lower()])
        }
        
        message = f"Network Timeout - URL: {url} - Duration: {timeout_duration}s - Attempt: {attempt_number}"
        level = ErrorLevel.WARNING if attempt_number < 3 else ErrorLevel.ERROR
        
        return self.log_error(message, ErrorCategory.NETWORK, level, detail_level, context)
    
    def log_data_processing_error(self, step: str, data_type: str, error_details: str,
                                 sample_data: Any = None, detail_level: DetailLevel = DetailLevel.DETAILED):
        """Log data processing errors with data samples"""
        context = {
            "processing_step": step,
            "data_type": data_type,
            "error_details": error_details,
            "sample_data_preview": str(sample_data)[:200] if sample_data is not None else "No sample data"
        }
        
        message = f"Data Processing Error - Step: {step} - Type: {data_type} - Details: {error_details}"
        
        return self.log_error(message, ErrorCategory.DATA_PROCESSING, ErrorLevel.ERROR, detail_level, context)
    
    def log_calculation_error(self, metric: str, calculation_type: str, values: Dict[str, Any],
                             detail_level: DetailLevel = DetailLevel.DETAILED):
        """Log financial calculation errors with input values"""
        context = {
            "metric": metric,
            "calculation_type": calculation_type,
            "input_values": values,
            "values_summary": {
                "start_value": values.get("start_value"),
                "end_value": values.get("end_value"),
                "periods": values.get("periods")
            }
        }
        
        message = f"Calculation Error - Metric: {metric} - Type: {calculation_type}"
        
        return self.log_error(message, ErrorCategory.CALCULATION, ErrorLevel.ERROR, detail_level, context)
    
    def log_user_input_error(self, input_field: str, input_value: str, validation_error: str,
                           suggestions: List[str] = None, detail_level: DetailLevel = DetailLevel.STANDARD):
        """Log user input validation errors with suggestions"""
        context = {
            "field": input_field,
            "value": input_value,
            "validation_error": validation_error,
            "suggestions": suggestions if suggestions is not None else []
        }
        
        message = f"User Input Error - Field: {input_field} - Value: '{input_value}' - Error: {validation_error}"
        
        return self.log_error(message, ErrorCategory.USER_INPUT, ErrorLevel.INFO, detail_level, context)
    
    def log_performance_warning(self, operation: str, duration: float, threshold: float,
                               context_data: Dict[str, Any] = None):
        """Log performance issues that might indicate problems"""
        context = {
            "operation": operation,
            "actual_duration": duration,
            "threshold": threshold,
            "performance_ratio": duration / threshold,
            **(context_data if context_data is not None else {})
        }
        
        message = f"Performance Warning - Operation: {operation} - Duration: {duration:.2f}s (threshold: {threshold:.2f}s)"
        
        return self.log_error(message, ErrorCategory.SYSTEM, ErrorLevel.WARNING, DetailLevel.STANDARD, context)
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get summary of errors for debugging and analysis"""
        return {
            "session_id": self.session_id,
            "total_errors": len(self.session_errors),
            "error_counts_by_category": self.error_counts,
            "recent_errors": self.session_errors[-5:] if self.session_errors else [],
            "error_trends": self._analyze_error_trends(),
            "environment_info": self.environment_info
        }
    
    def _analyze_error_trends(self) -> Dict[str, Any]:
        """Analyze error patterns for insights"""
        if not self.session_errors:
            return {"status": "no_errors"}
            
        recent_errors = self.session_errors[-10:]
        
        # Check for repeated errors
        error_messages = [e.get("message", "") for e in recent_errors]
        repeated_errors = len(error_messages) - len(set(error_messages))
        
        # Check for escalating error levels
        error_levels = [e.get("level", "INFO") for e in recent_errors]
        critical_errors = sum(1 for level in error_levels if level == "CRITICAL")
        
        return {
            "repeated_errors": repeated_errors,
            "critical_errors_recent": critical_errors,
            "dominant_category": max(self.error_counts.items(), key=lambda x: x[1])[0],
            "error_frequency": len(self.session_errors) / max((time.time() - float(self.session_id.split('_')[-1])) / 60, 1)
        }
    
    def export_error_log(self, filepath: str = None) -> str:
        """Export complete error log for analysis"""
        if not filepath:
            filepath = f"error_log_{self.session_id}.json"
            
        export_data = {
            "export_timestamp": datetime.now().isoformat(),
            "session_summary": self.get_error_summary(),
            "all_errors": self.session_errors,
            "environment_info": self.environment_info
        }
        
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
            
        self.main_logger.info(f"Error log exported to: {filepath}")
        return filepath

# Global logger instance
error_logger = SECFinancialErrorLogger()

# Convenience functions for quick logging
def log_sec_error(message: str, context: Optional[Dict] = None, detail_level: DetailLevel = DetailLevel.STANDARD):
    return error_logger.log_error(message, ErrorCategory.SEC_API, ErrorLevel.ERROR, detail_level, context)

def log_network_error(message: str, context: Optional[Dict] = None, detail_level: DetailLevel = DetailLevel.STANDARD):
    return error_logger.log_error(message, ErrorCategory.NETWORK, ErrorLevel.ERROR, detail_level, context)

def log_user_error(message: str, context: Optional[Dict] = None, detail_level: DetailLevel = DetailLevel.STANDARD):
    return error_logger.log_error(message, ErrorCategory.USER_INPUT, ErrorLevel.INFO, detail_level, context)

def log_critical(message: str, category: ErrorCategory = ErrorCategory.SYSTEM, context: Optional[Dict] = None):
    return error_logger.log_error(message, category, ErrorLevel.CRITICAL, DetailLevel.FORENSIC, context)