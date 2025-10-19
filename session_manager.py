"""
Session and state management for Replit platform constraints
Handles memory loss between sessions and app restarts
"""

import os
import json
import hashlib
import time
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from error_logger import error_logger, ErrorCategory, ErrorLevel, DetailLevel

class ReplitSessionManager:
    """Manage session state and data persistence for Replit environment"""
    
    def __init__(self):
        self.cache_dir = ".session_cache"
        self.max_cache_age_hours = 2  # Cache SEC data for 2 hours
        self.max_cache_size_mb = 50   # Limit cache to 50MB total
        
        # Ensure cache directory exists
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
            
        # Track session info
        self.session_start = time.time()
        self.session_id = f"session_{int(self.session_start)}"
        
    def generate_cache_key(self, ticker: str, data_type: str) -> str:
        """Generate consistent cache key for ticker data"""
        key_string = f"{ticker.upper()}_{data_type}_{datetime.now().strftime('%Y%m%d')}"
        return hashlib.md5(key_string.encode()).hexdigest()[:12]
    
    def cache_sec_data(self, ticker: str, data_type: str, data: Dict[str, Any]) -> bool:
        """Cache SEC API data to survive session restarts"""
        try:
            cache_key = self.generate_cache_key(ticker, data_type)
            cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
            
            # Check cache size limits before writing
            if not self._check_cache_size_limits():
                self._cleanup_old_cache()
            
            cache_entry = {
                "ticker": ticker.upper(),
                "data_type": data_type,
                "timestamp": datetime.now().isoformat(),
                "session_id": self.session_id,
                "data": data
            }
            
            with open(cache_file, 'w') as f:
                json.dump(cache_entry, f, default=str)
            
            error_logger.log_error(
                f"Cached SEC data for {ticker}",
                ErrorCategory.SYSTEM,
                ErrorLevel.INFO,
                DetailLevel.STANDARD,
                {
                    "ticker": ticker,
                    "data_type": data_type,
                    "cache_key": cache_key,
                    "cache_file_size": os.path.getsize(cache_file)
                }
            )
            
            return True
            
        except Exception as e:
            error_logger.log_error(
                f"Failed to cache SEC data for {ticker}",
                ErrorCategory.SYSTEM,
                ErrorLevel.WARNING,
                DetailLevel.DETAILED,
                {
                    "ticker": ticker,
                    "data_type": data_type,
                    "error": str(e)
                },
                exception=e
            )
            return False
    
    def get_cached_sec_data(self, ticker: str, data_type: str) -> Optional[Dict[str, Any]]:
        """Retrieve cached SEC data if available and not expired"""
        try:
            cache_key = self.generate_cache_key(ticker, data_type)
            cache_file = os.path.join(self.cache_dir, f"{cache_key}.json")
            
            if not os.path.exists(cache_file):
                return None
                
            # Check file age
            file_age_hours = (time.time() - os.path.getmtime(cache_file)) / 3600
            if file_age_hours > self.max_cache_age_hours:
                os.remove(cache_file)  # Remove expired cache
                error_logger.log_error(
                    f"Removed expired cache for {ticker}",
                    ErrorCategory.SYSTEM,
                    ErrorLevel.INFO,
                    DetailLevel.STANDARD,
                    {"ticker": ticker, "age_hours": round(file_age_hours, 2)}
                )
                return None
            
            with open(cache_file, 'r') as f:
                cache_entry = json.load(f)
            
            error_logger.log_error(
                f"Retrieved cached SEC data for {ticker}",
                ErrorCategory.SYSTEM,
                ErrorLevel.INFO,
                DetailLevel.STANDARD,
                {
                    "ticker": ticker,
                    "data_type": data_type,
                    "cache_age_minutes": round(file_age_hours * 60, 2),
                    "cached_session": cache_entry.get("session_id", "unknown")
                }
            )
            
            return cache_entry["data"]
            
        except Exception as e:
            error_logger.log_error(
                f"Failed to retrieve cached data for {ticker}",
                ErrorCategory.SYSTEM,
                ErrorLevel.WARNING,
                DetailLevel.STANDARD,
                {
                    "ticker": ticker,
                    "data_type": data_type,
                    "error": str(e)
                },
                exception=e
            )
            return None
    
    def _check_cache_size_limits(self) -> bool:
        """Check if cache directory is within size limits"""
        try:
            total_size = 0
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                if os.path.isfile(file_path):
                    total_size += os.path.getsize(file_path)
            
            total_size_mb = total_size / (1024 * 1024)
            
            if total_size_mb > self.max_cache_size_mb:
                error_logger.log_error(
                    f"Cache size limit exceeded: {total_size_mb:.2f}MB > {self.max_cache_size_mb}MB",
                    ErrorCategory.SYSTEM,
                    ErrorLevel.WARNING,
                    DetailLevel.STANDARD,
                    {"current_size_mb": total_size_mb, "limit_mb": self.max_cache_size_mb}
                )
                return False
            
            return True
            
        except Exception as e:
            error_logger.log_error(
                "Failed to check cache size",
                ErrorCategory.SYSTEM,
                ErrorLevel.WARNING,
                DetailLevel.STANDARD,
                {"error": str(e)},
                exception=e
            )
            return True  # Assume OK if we can't check
    
    def _cleanup_old_cache(self):
        """Remove oldest cache files to free up space"""
        try:
            cache_files = []
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                if os.path.isfile(file_path) and filename.endswith('.json'):
                    cache_files.append({
                        'path': file_path,
                        'mtime': os.path.getmtime(file_path),
                        'size': os.path.getsize(file_path)
                    })
            
            # Sort by modification time (oldest first)
            cache_files.sort(key=lambda x: x['mtime'])
            
            # Remove oldest files until under limit
            removed_count = 0
            for file_info in cache_files[:len(cache_files)//2]:  # Remove half of files
                os.remove(file_info['path'])
                removed_count += 1
            
            error_logger.log_error(
                f"Cleaned up cache: removed {removed_count} old files",
                ErrorCategory.SYSTEM,
                ErrorLevel.INFO,
                DetailLevel.STANDARD,
                {"files_removed": removed_count, "total_files": len(cache_files)}
            )
            
        except Exception as e:
            error_logger.log_error(
                "Cache cleanup failed",
                ErrorCategory.SYSTEM,
                ErrorLevel.ERROR,
                DetailLevel.STANDARD,
                {"error": str(e)},
                exception=e
            )
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get current session information and recovery status"""
        uptime_minutes = (time.time() - self.session_start) / 60
        
        # Check cache status
        cache_files = []
        cache_size_mb = 0
        if os.path.exists(self.cache_dir):
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    cache_size_mb += file_size / (1024 * 1024)
                    cache_files.append(filename)
        
        return {
            "session_id": self.session_id,
            "uptime_minutes": round(uptime_minutes, 2),
            "platform": "replit",
            "memory_persistent": False,
            "cache_status": {
                "enabled": True,
                "files_count": len(cache_files),
                "total_size_mb": round(cache_size_mb, 3),
                "size_limit_mb": self.max_cache_size_mb,
                "age_limit_hours": self.max_cache_age_hours
            },
            "recovery_info": {
                "sec_data_cached": len([f for f in cache_files if f.endswith('.json')]),
                "can_recover_from_restart": len(cache_files) > 0,
                "recommendation": "Use cached data when available to reduce API calls"
            }
        }
    
    def handle_app_restart_recovery(self) -> Dict[str, Any]:
        """Handle recovery operations when app restarts (common on Replit)"""
        recovery_info = {
            "restart_detected": True,
            "recovery_actions": [],
            "available_cache": []
        }
        
        try:
            # Check for available cached data
            if os.path.exists(self.cache_dir):
                for filename in os.listdir(self.cache_dir):
                    if filename.endswith('.json'):
                        file_path = os.path.join(self.cache_dir, filename)
                        file_age_hours = (time.time() - os.path.getmtime(file_path)) / 3600
                        
                        if file_age_hours <= self.max_cache_age_hours:
                            recovery_info["available_cache"].append({
                                "file": filename,
                                "age_hours": round(file_age_hours, 2)
                            })
            
            recovery_info["recovery_actions"].append("Scanned cache directory")
            recovery_info["cache_files_available"] = len(recovery_info["available_cache"])
            
            # Log recovery status
            error_logger.log_error(
                f"App restart recovery: {len(recovery_info['available_cache'])} cached files available",
                ErrorCategory.SYSTEM,
                ErrorLevel.INFO,
                DetailLevel.DETAILED,
                recovery_info
            )
            
        except Exception as e:
            error_logger.log_error(
                "App restart recovery failed",
                ErrorCategory.SYSTEM,
                ErrorLevel.ERROR,
                DetailLevel.STANDARD,
                {"error": str(e)},
                exception=e
            )
            recovery_info["recovery_actions"].append(f"Recovery failed: {str(e)}")
        
        return recovery_info

# Global session manager instance
session_manager = ReplitSessionManager()