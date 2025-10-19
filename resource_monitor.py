"""
Resource monitoring and optimization for Replit platform constraints
Critical for staying within 1GB storage + memory limits
"""

import psutil
import os
import sys
import time
import gc
from typing import Dict, Any, Optional
from datetime import datetime
from error_logger import error_logger, ErrorCategory, ErrorLevel, DetailLevel

class ReplitResourceMonitor:
    """Monitor and manage resource usage within Replit's constraints"""
    
    def __init__(self):
        self.start_time = time.time()
        self.memory_alerts_sent = 0
        self.max_memory_seen = 0
        self.gc_collections = 0
        
        # Replit constraints
        self.MEMORY_LIMIT_MB = 512  # Conservative limit (actual may be higher)
        self.STORAGE_LIMIT_GB = 1.0
        self.MEMORY_WARNING_THRESHOLD = 0.8  # 80% of limit
        self.MEMORY_CRITICAL_THRESHOLD = 0.9  # 90% of limit
        
    def get_current_usage(self) -> Dict[str, Any]:
        """Get current resource usage with Replit-specific metrics"""
        try:
            # Memory usage
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
            memory_percent = (memory_mb / self.MEMORY_LIMIT_MB) * 100
            
            # CPU usage
            cpu_percent = process.cpu_percent(interval=0.1)
            
            # Disk usage (current directory - Repl storage)
            disk_usage = psutil.disk_usage('.')
            disk_used_gb = disk_usage.used / (1024**3)  # Convert to GB
            disk_percent = (disk_used_gb / self.STORAGE_LIMIT_GB) * 100
            
            # System info
            system_memory = psutil.virtual_memory()
            
            # Track maximum memory usage
            if memory_mb > self.max_memory_seen:
                self.max_memory_seen = memory_mb
            
            usage_stats = {
                "timestamp": datetime.now().isoformat(),
                "memory": {
                    "current_mb": round(memory_mb, 2),
                    "limit_mb": self.MEMORY_LIMIT_MB,
                    "percent_used": round(memory_percent, 2),
                    "max_seen_mb": round(self.max_memory_seen, 2),
                    "system_available_mb": round(system_memory.available / (1024*1024), 2)
                },
                "storage": {
                    "used_gb": round(disk_used_gb, 3),
                    "limit_gb": self.STORAGE_LIMIT_GB,
                    "percent_used": round(disk_percent, 2)
                },
                "cpu": {
                    "percent": round(cpu_percent, 2)
                },
                "uptime_minutes": round((time.time() - self.start_time) / 60, 2),
                "gc_collections": self.gc_collections
            }
            
            return usage_stats
            
        except Exception as e:
            error_logger.log_error(
                "Failed to get resource usage stats",
                ErrorCategory.SYSTEM,
                ErrorLevel.WARNING,
                DetailLevel.STANDARD,
                {"error": str(e)},
                exception=e
            )
            return {"error": "Failed to get stats", "timestamp": datetime.now().isoformat()}
    
    def check_resource_limits(self) -> Dict[str, Any]:
        """Check if approaching Replit resource limits"""
        usage = self.get_current_usage()
        
        if "error" in usage:
            return {"status": "error", "usage": usage}
        
        alerts = []
        status = "ok"
        
        # Check memory limits
        memory_percent = usage["memory"]["percent_used"]
        if memory_percent >= self.MEMORY_CRITICAL_THRESHOLD * 100:
            status = "critical"
            alerts.append({
                "type": "memory_critical",
                "message": f"Memory usage at {memory_percent:.1f}% (>{self.MEMORY_CRITICAL_THRESHOLD*100}% threshold)",
                "action": "Immediate garbage collection and data cleanup required"
            })
            self.force_garbage_collection("Critical memory usage")
            
        elif memory_percent >= self.MEMORY_WARNING_THRESHOLD * 100:
            status = "warning"
            alerts.append({
                "type": "memory_warning", 
                "message": f"Memory usage at {memory_percent:.1f}% (>{self.MEMORY_WARNING_THRESHOLD*100}% threshold)",
                "action": "Consider optimizing data structures"
            })
        
        # Check storage limits
        storage_percent = usage["storage"]["percent_used"]
        if storage_percent >= 90:
            status = "critical"
            alerts.append({
                "type": "storage_critical",
                "message": f"Storage usage at {storage_percent:.1f}% (>90% threshold)",
                "action": "Clean up temporary files and logs"
            })
        elif storage_percent >= 80:
            if status != "critical":
                status = "warning"
            alerts.append({
                "type": "storage_warning",
                "message": f"Storage usage at {storage_percent:.1f}% (>80% threshold)", 
                "action": "Monitor file growth"
            })
        
        # Log alerts
        if alerts:
            for alert in alerts:
                level = ErrorLevel.CRITICAL if alert["type"].endswith("critical") else ErrorLevel.WARNING
                error_logger.log_error(
                    f"Resource Alert: {alert['message']}",
                    ErrorCategory.SYSTEM,
                    level,
                    DetailLevel.DETAILED,
                    {
                        "alert_type": alert["type"],
                        "action_required": alert["action"],
                        "current_usage": usage,
                        "replit_constraints": {
                            "memory_limit_mb": self.MEMORY_LIMIT_MB,
                            "storage_limit_gb": self.STORAGE_LIMIT_GB
                        }
                    }
                )
        
        return {
            "status": status,
            "alerts": alerts,
            "usage": usage,
            "recommendations": self._get_optimization_recommendations(usage)
        }
    
    def force_garbage_collection(self, reason: str = "Manual trigger"):
        """Force garbage collection to free memory"""
        try:
            before_mb = psutil.Process().memory_info().rss / (1024 * 1024)
            
            # Multiple GC passes for thorough cleanup
            collected = 0
            for generation in range(3):
                collected += gc.collect()
            
            after_mb = psutil.Process().memory_info().rss / (1024 * 1024)
            freed_mb = before_mb - after_mb
            
            self.gc_collections += 1
            
            error_logger.log_error(
                f"Forced garbage collection completed",
                ErrorCategory.SYSTEM,
                ErrorLevel.INFO,
                DetailLevel.DETAILED,
                {
                    "reason": reason,
                    "objects_collected": collected,
                    "memory_before_mb": round(before_mb, 2),
                    "memory_after_mb": round(after_mb, 2),
                    "memory_freed_mb": round(freed_mb, 2),
                    "total_gc_runs": self.gc_collections
                }
            )
            
            return {
                "objects_collected": collected,
                "memory_freed_mb": round(freed_mb, 2),
                "success": True
            }
            
        except Exception as e:
            error_logger.log_error(
                "Garbage collection failed",
                ErrorCategory.SYSTEM,
                ErrorLevel.ERROR,
                DetailLevel.STANDARD,
                {"reason": reason, "error": str(e)},
                exception=e
            )
            return {"success": False, "error": str(e)}
    
    def optimize_for_replit(self, data_size_estimate: Optional[int] = None) -> Dict[str, Any]:
        """Optimize application for Replit constraints before processing large data"""
        
        # Check current state
        current_state = self.check_resource_limits()
        
        optimizations = []
        
        # Pre-emptive garbage collection
        gc_result = self.force_garbage_collection("Pre-processing optimization")
        optimizations.append({
            "action": "garbage_collection",
            "result": gc_result
        })
        
        # Estimate if data will fit
        if data_size_estimate:
            current_memory = current_state["usage"]["memory"]["current_mb"]
            estimated_total = current_memory + (data_size_estimate / (1024*1024))  # Convert bytes to MB
            
            if estimated_total > self.MEMORY_LIMIT_MB * 0.9:
                optimizations.append({
                    "action": "memory_warning",
                    "message": f"Estimated memory usage ({estimated_total:.1f}MB) may exceed limits",
                    "recommendation": "Process data in smaller chunks"
                })
        
        # Set aggressive GC thresholds for Replit
        gc.set_threshold(100, 10, 5)  # More frequent GC
        optimizations.append({
            "action": "gc_tuning",
            "message": "Set aggressive garbage collection thresholds for Replit"
        })
        
        return {
            "optimizations": optimizations,
            "resource_state": current_state,
            "ready_for_processing": current_state["status"] != "critical"
        }
    
    def _get_optimization_recommendations(self, usage: Dict[str, Any]) -> list:
        """Get specific optimization recommendations based on current usage"""
        recommendations = []
        
        memory_percent = usage["memory"]["percent_used"]
        storage_percent = usage["storage"]["percent_used"]
        
        if memory_percent > 70:
            recommendations.append("Run garbage collection more frequently")
            recommendations.append("Process SEC data in smaller chunks")
            recommendations.append("Clear processed data immediately after use")
        
        if storage_percent > 60:
            recommendations.append("Clean up temporary CSV exports")
            recommendations.append("Avoid storing large responses in memory")
            recommendations.append("Use streaming for large API responses")
        
        if memory_percent > 50:
            recommendations.append("Use generators instead of lists for large datasets")
            recommendations.append("Process financial data incrementally")
        
        return recommendations
    
    def get_session_recovery_info(self) -> Dict[str, Any]:
        """Get info for recovering from session restarts (Replit doesn't persist memory)"""
        return {
            "platform": "replit",
            "session_persistent": False,
            "memory_persistent": False, 
            "warning": "All in-memory data lost on app restart - store critical data externally",
            "recovery_strategy": "Re-fetch data from SEC APIs on each session",
            "cache_recommendation": "Use filesystem cache for expensive API calls",
            "uptime_minutes": round((time.time() - self.start_time) / 60, 2)
        }

# Global monitor instance  
resource_monitor = ReplitResourceMonitor()