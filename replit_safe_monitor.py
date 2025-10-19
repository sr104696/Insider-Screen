"""
Lightweight resource monitor for Replit - no external dependencies
Fallback version that works even without psutil
"""

import os
import gc
import sys
import time
from typing import Dict, Any, Optional
from datetime import datetime
from error_logger import error_logger, ErrorCategory, ErrorLevel, DetailLevel

class ReplitSafeMonitor:
    """Lightweight resource monitoring without external dependencies"""
    
    def __init__(self):
        self.start_time = time.time()
        self.gc_collections = 0
        self.memory_warnings = 0
        
        # Conservative Replit limits (safer estimates)
        self.MEMORY_WARNING_MB = 300  # Start warning at 300MB
        self.MEMORY_CRITICAL_MB = 400  # Critical at 400MB
        self.MAX_CACHE_SIZE_MB = 50   # Keep cache under 50MB
        
    def get_basic_stats(self) -> Dict[str, Any]:
        """Get basic stats without psutil"""
        try:
            # Get memory usage from sys.getsizeof on large objects
            import gc
            
            # Count objects
            object_counts = {
                "total_objects": len(gc.get_objects()),
                "dict_objects": len([obj for obj in gc.get_objects() if isinstance(obj, dict)]),
                "list_objects": len([obj for obj in gc.get_objects() if isinstance(obj, list)])
            }
            
            # Disk usage for current directory
            disk_usage = self._get_disk_usage()
            
            return {
                "timestamp": datetime.now().isoformat(),
                "uptime_minutes": round((time.time() - self.start_time) / 60, 2),
                "object_counts": object_counts,
                "disk_usage": disk_usage,
                "gc_collections": self.gc_collections,
                "memory_warnings": self.memory_warnings,
                "status": "basic_monitoring"
            }
            
        except Exception as e:
            error_logger.log_error(
                "Failed to get basic stats",
                ErrorCategory.SYSTEM,
                ErrorLevel.WARNING,
                DetailLevel.STANDARD,
                {"error": str(e)},
                exception=e
            )
            return {"error": str(e), "timestamp": datetime.now().isoformat()}
    
    def _get_disk_usage(self) -> Dict[str, Any]:
        """Get disk usage without psutil"""
        try:
            import shutil
            total, used, free = shutil.disk_usage('.')
            
            return {
                "total_gb": round(total / (1024**3), 3),
                "used_gb": round(used / (1024**3), 3),
                "free_gb": round(free / (1024**3), 3),
                "percent_used": round((used / total) * 100, 2)
            }
        except:
            return {"error": "Cannot determine disk usage"}
    
    def force_cleanup(self, reason: str = "Manual cleanup") -> Dict[str, Any]:
        """Force memory cleanup without psutil"""
        try:
            # Count objects before
            objects_before = len(gc.get_objects())
            
            # Multiple GC passes
            collected = 0
            for i in range(3):
                collected += gc.collect()
            
            objects_after = len(gc.get_objects())
            
            self.gc_collections += 1
            
            result = {
                "reason": reason,
                "objects_collected": collected,
                "objects_before": objects_before,
                "objects_after": objects_after,
                "objects_freed": objects_before - objects_after,
                "success": True
            }
            
            error_logger.log_error(
                f"Memory cleanup completed: freed {result['objects_freed']} objects",
                ErrorCategory.SYSTEM,
                ErrorLevel.INFO,
                DetailLevel.STANDARD,
                result
            )
            
            return result
            
        except Exception as e:
            error_logger.log_error(
                "Memory cleanup failed",
                ErrorCategory.SYSTEM,
                ErrorLevel.ERROR,
                DetailLevel.STANDARD,
                {"reason": reason, "error": str(e)},
                exception=e
            )
            return {"success": False, "error": str(e)}
    
    def check_replit_limits(self, estimated_memory_mb: int = 0) -> Dict[str, Any]:
        """Check if we're likely approaching Replit limits"""
        stats = self.get_basic_stats()
        
        alerts = []
        status = "ok"
        
        # Check object count as proxy for memory usage
        object_count = stats.get("object_counts", {}).get("total_objects", 0)
        
        # Rough heuristics for Replit memory usage
        if object_count > 100000:  # High object count
            status = "warning"
            self.memory_warnings += 1
            alerts.append({
                "type": "high_object_count",
                "message": f"High object count: {object_count:,}",
                "action": "Consider running garbage collection"
            })
        
        if object_count > 200000:  # Very high object count
            status = "critical"
            alerts.append({
                "type": "critical_object_count", 
                "message": f"Critical object count: {object_count:,}",
                "action": "Immediate cleanup required"
            })
            # Auto-cleanup
            self.force_cleanup("Critical object count")
        
        # Check disk usage
        disk_usage = stats.get("disk_usage", {})
        if isinstance(disk_usage, dict) and "percent_used" in disk_usage:
            disk_percent = disk_usage["percent_used"]
            if disk_percent > 80:
                if status != "critical":
                    status = "warning"
                alerts.append({
                    "type": "disk_usage_high",
                    "message": f"Disk usage at {disk_percent:.1f}%",
                    "action": "Clean up cache files"
                })
        
        # Log alerts
        if alerts:
            for alert in alerts:
                level = ErrorLevel.CRITICAL if "critical" in alert["type"] else ErrorLevel.WARNING
                error_logger.log_error(
                    f"Replit Resource Alert: {alert['message']}",
                    ErrorCategory.SYSTEM,
                    level,
                    DetailLevel.DETAILED,
                    {
                        "alert": alert,
                        "stats": stats,
                        "estimated_memory_mb": estimated_memory_mb
                    }
                )
        
        return {
            "status": status,
            "alerts": alerts,
            "stats": stats,
            "ready_for_processing": status != "critical",
            "recommendations": self._get_recommendations(stats, status)
        }
    
    def _get_recommendations(self, stats: Dict[str, Any], status: str) -> list:
        """Get optimization recommendations"""
        recommendations = []
        
        if status in ["warning", "critical"]:
            recommendations.append("Run garbage collection more frequently")
            recommendations.append("Process data in smaller chunks")
            recommendations.append("Clear variables immediately after use")
        
        object_count = stats.get("object_counts", {}).get("total_objects", 0)
        if object_count > 50000:
            recommendations.append("Reduce object creation in loops")
            recommendations.append("Use generators instead of lists")
        
        disk_usage = stats.get("disk_usage", {})
        if isinstance(disk_usage, dict) and disk_usage.get("percent_used", 0) > 50:
            recommendations.append("Clear cache files regularly")
            recommendations.append("Avoid storing large responses")
        
        return recommendations
    
    def optimize_for_data_processing(self, estimated_size_mb: int = 0) -> Dict[str, Any]:
        """Prepare for data processing on Replit"""
        
        # Pre-emptive cleanup
        cleanup_result = self.force_cleanup("Pre-processing optimization")
        
        # Check limits
        limit_check = self.check_replit_limits(estimated_size_mb)
        
        # Set aggressive GC for Replit
        gc.set_threshold(50, 10, 5)  # Very frequent GC
        
        return {
            "cleanup": cleanup_result,
            "limits": limit_check,
            "ready": limit_check["ready_for_processing"],
            "gc_tuned": True,
            "estimated_memory_mb": estimated_size_mb
        }

# Create global instance
try:
    # Try to use full resource monitor if psutil is available
    from resource_monitor import resource_monitor as full_monitor
    replit_monitor = full_monitor
    monitor_type = "full"
except ImportError:
    # Fall back to safe monitor
    replit_monitor = ReplitSafeMonitor()
    monitor_type = "safe"

# Log which monitor we're using
error_logger.log_error(
    f"Using {monitor_type} resource monitoring for Replit",
    ErrorCategory.SYSTEM,
    ErrorLevel.INFO,
    DetailLevel.STANDARD,
    {"monitor_type": monitor_type}
)