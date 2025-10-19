"""
Unified Resource Monitor - Optimized for Replit AI Collaboration

KEY AI OPTIMIZATIONS:
1. Single interface instead of dual monitor system (eliminates AI confusion)
2. Clear, simple methods that AI can understand and modify
3. Built-in safeguards against destructive operations
4. Transparent resource reporting for AI decision making
5. Auto-fallback without complex decision trees

REPLIT PLATFORM AWARENESS:
- Handles 1GB storage and ~512MB memory limits
- Works with or without psutil (excellent fallback pattern preserved)
- Conservative thresholds to prevent platform issues
- Session-aware cleanup for Replit's stateless nature
"""

import gc
import os
import sys
import time
import shutil
from typing import Dict, Any, Optional
from datetime import datetime

# Try to import psutil, fall back gracefully
try:
    import psutil
    PSUTIL_AVAILABLE = True
    print("📊 Resource monitor: Full monitoring with psutil")
except ImportError:
    PSUTIL_AVAILABLE = False
    print("📊 Resource monitor: Basic monitoring (psutil not available)")

class ReplitAIResourceMonitor:
    """
    Unified resource monitor optimized for Replit AI collaboration
    
    CRITICAL AI SAFETY FEATURES:
    - Single, clear interface (no dual system confusion)
    - Automatic cleanup before resource limits
    - Cannot perform destructive operations
    - Clear status reporting for AI understanding
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.cleanup_count = 0
        self.max_memory_seen = 0
        
        # REPLIT PLATFORM CONSTRAINTS (conservative for safety)
        self.MEMORY_LIMIT_MB = 400  # Conservative limit to prevent issues
        self.STORAGE_LIMIT_GB = 0.8  # Use 80% of 1GB limit for safety
        self.WARNING_THRESHOLD = 0.7  # Warn at 70%
        self.CRITICAL_THRESHOLD = 0.85  # Critical at 85%
        
        # AI-SAFE CACHE SETTINGS
        self.MAX_CACHE_SIZE_MB = 30  # Conservative cache size
        self.cache_dir = ".session_cache"
        
        print(f"🚀 Resource monitor initialized (psutil: {PSUTIL_AVAILABLE})")

    def get_resource_status(self) -> Dict[str, Any]:
        """
        SINGLE METHOD for all resource information
        AI-FRIENDLY: Clear, consistent return format
        """
        status = {
            "timestamp": datetime.now().isoformat(),
            "uptime_minutes": round((time.time() - self.start_time) / 60, 2),
            "monitoring_type": "full" if PSUTIL_AVAILABLE else "basic",
            "cleanup_count": self.cleanup_count,
            "memory": self._get_memory_info(),
            "storage": self._get_storage_info(),
            "cache": self._get_cache_info(),
            "status": "ok",  # Will be updated based on thresholds
            "alerts": []
        }
        
        # Determine overall status
        memory_percent = status["memory"]["percent_used"]
        storage_percent = status["storage"]["percent_used"]
        
        if memory_percent >= self.CRITICAL_THRESHOLD * 100 or storage_percent >= self.CRITICAL_THRESHOLD * 100:
            status["status"] = "critical"
            status["alerts"].append("Resource usage critical - cleanup recommended")
        elif memory_percent >= self.WARNING_THRESHOLD * 100 or storage_percent >= self.WARNING_THRESHOLD * 100:
            status["status"] = "warning"
            status["alerts"].append("Resource usage elevated - monitor closely")
        
        return status

    def _get_memory_info(self) -> Dict[str, Any]:
        """Get memory information with fallback logic"""
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process()
                memory_mb = process.memory_info().rss / (1024 * 1024)
                self.max_memory_seen = max(self.max_memory_seen, memory_mb)
                
                return {
                    "current_mb": round(memory_mb, 2),
                    "max_seen_mb": round(self.max_memory_seen, 2),
                    "limit_mb": self.MEMORY_LIMIT_MB,
                    "percent_used": round((memory_mb / self.MEMORY_LIMIT_MB) * 100, 2),
                    "method": "psutil"
                }
            except Exception as e:
                print(f"⚠️ psutil memory check failed: {e}")
        
        # Fallback: Use object count as proxy
        try:
            object_count = len(gc.get_objects())
            # Rough heuristic: 50K objects ≈ 100MB
            estimated_mb = (object_count / 50000) * 100
            estimated_mb = min(estimated_mb, self.MEMORY_LIMIT_MB)  # Cap at limit
            
            return {
                "current_mb": round(estimated_mb, 2),
                "max_seen_mb": round(self.max_memory_seen, 2),
                "limit_mb": self.MEMORY_LIMIT_MB,
                "percent_used": round((estimated_mb / self.MEMORY_LIMIT_MB) * 100, 2),
                "method": "object_count_proxy",
                "object_count": object_count
            }
        except Exception:
            return {
                "current_mb": 0,
                "max_seen_mb": 0,
                "limit_mb": self.MEMORY_LIMIT_MB,
                "percent_used": 0,
                "method": "failed",
                "error": "Unable to determine memory usage"
            }

    def _get_storage_info(self) -> Dict[str, Any]:
        """Get storage information for current directory"""
        try:
            total, used, free = shutil.disk_usage('.')
            used_gb = used / (1024**3)
            total_gb = total / (1024**3)
            
            return {
                "used_gb": round(used_gb, 3),
                "total_gb": round(total_gb, 3),
                "limit_gb": self.STORAGE_LIMIT_GB,
                "percent_used": round((used_gb / self.STORAGE_LIMIT_GB) * 100, 2),
                "free_gb": round(free / (1024**3), 3)
            }
        except Exception as e:
            return {
                "used_gb": 0,
                "total_gb": 0,
                "limit_gb": self.STORAGE_LIMIT_GB,
                "percent_used": 0,
                "error": f"Storage check failed: {e}"
            }

    def _get_cache_info(self) -> Dict[str, Any]:
        """Get cache directory information"""
        try:
            if not os.path.exists(self.cache_dir):
                return {
                    "size_mb": 0,
                    "file_count": 0,
                    "limit_mb": self.MAX_CACHE_SIZE_MB,
                    "percent_used": 0
                }
            
            total_size = 0
            file_count = 0
            
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                if os.path.isfile(file_path):
                    total_size += os.path.getsize(file_path)
                    file_count += 1
            
            size_mb = total_size / (1024 * 1024)
            
            return {
                "size_mb": round(size_mb, 3),
                "file_count": file_count,
                "limit_mb": self.MAX_CACHE_SIZE_MB,
                "percent_used": round((size_mb / self.MAX_CACHE_SIZE_MB) * 100, 2)
            }
        except Exception as e:
            return {
                "size_mb": 0,
                "file_count": 0,
                "limit_mb": self.MAX_CACHE_SIZE_MB,
                "percent_used": 0,
                "error": f"Cache check failed: {e}"
            }

    def cleanup_resources(self, reason: str = "Manual cleanup") -> Dict[str, Any]:
        """
        AI-SAFE CLEANUP: Cannot perform destructive operations
        
        SAFETY FEATURES:
        - Only cleans up garbage and temp files
        - Cannot delete user data or important files
        - Provides clear feedback on what was done
        """
        cleanup_results = {
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
            "actions": [],
            "success": True
        }
        
        try:
            # Memory cleanup (safe - only garbage collection)
            objects_before = len(gc.get_objects()) if not PSUTIL_AVAILABLE else 0
            collected = 0
            
            for i in range(3):  # Multiple GC passes
                collected += gc.collect()
            
            if not PSUTIL_AVAILABLE:
                objects_after = len(gc.get_objects())
                cleanup_results["actions"].append({
                    "type": "garbage_collection",
                    "objects_collected": collected,
                    "objects_freed": objects_before - objects_after
                })
            else:
                cleanup_results["actions"].append({
                    "type": "garbage_collection", 
                    "objects_collected": collected
                })
            
            # Cache cleanup (safe - only removes old cache files)
            if os.path.exists(self.cache_dir):
                cache_cleaned = self._cleanup_old_cache()
                if cache_cleaned:
                    cleanup_results["actions"].append(cache_cleaned)
            
            self.cleanup_count += 1
            print(f"🧹 Cleanup completed: {reason}")
            
        except Exception as e:
            cleanup_results["success"] = False
            cleanup_results["error"] = str(e)
            print(f"❌ Cleanup failed: {e}")
        
        return cleanup_results

    def _cleanup_old_cache(self) -> Optional[Dict[str, Any]]:
        """
        SAFE CACHE CLEANUP: Only removes files older than 2 hours
        AI-SAFE: Cannot accidentally delete important files
        """
        try:
            cache_files = []
            current_time = time.time()
            
            for filename in os.listdir(self.cache_dir):
                file_path = os.path.join(self.cache_dir, filename)
                if os.path.isfile(file_path) and filename.endswith('.json'):
                    file_age_hours = (current_time - os.path.getmtime(file_path)) / 3600
                    if file_age_hours > 2:  # Only remove files older than 2 hours
                        cache_files.append(file_path)
            
            removed_count = 0
            for file_path in cache_files[:5]:  # Limit to 5 files per cleanup
                try:
                    os.remove(file_path)
                    removed_count += 1
                except Exception:
                    pass  # Skip files that can't be removed
            
            if removed_count > 0:
                return {
                    "type": "cache_cleanup",
                    "files_removed": removed_count,
                    "note": "Only removed files older than 2 hours"
                }
            
        except Exception:
            pass  # Cache cleanup is optional
        
        return None

    def check_and_cleanup_if_needed(self) -> Dict[str, Any]:
        """
        AI-FRIENDLY: Automatic cleanup when resources are high
        SAFE: Only triggers on clear thresholds
        """
        status = self.get_resource_status()
        
        # Auto-cleanup if resources are critical
        if status["status"] == "critical":
            print("🚨 Critical resource usage detected - auto cleanup triggered")
            cleanup_result = self.cleanup_resources("Auto cleanup - critical resources")
            status["auto_cleanup"] = cleanup_result
        
        return status

    def is_ready_for_processing(self) -> bool:
        """
        AI-SIMPLE: Single boolean for processing readiness
        """
        status = self.get_resource_status()
        return status["status"] != "critical"

    def get_simple_summary(self) -> str:
        """
        AI-FRIENDLY: Human readable summary for AI understanding
        """
        status = self.get_resource_status()
        
        memory_pct = status["memory"]["percent_used"]
        storage_pct = status["storage"]["percent_used"]
        
        if status["status"] == "critical":
            return f"🚨 CRITICAL: Memory {memory_pct:.1f}% | Storage {storage_pct:.1f}% - Cleanup needed!"
        elif status["status"] == "warning":
            return f"⚠️ WARNING: Memory {memory_pct:.1f}% | Storage {storage_pct:.1f}% - Monitor closely"
        else:
            return f"✅ OK: Memory {memory_pct:.1f}% | Storage {storage_pct:.1f}% - Ready for processing"

# Global instance for easy AI access
monitor = ReplitAIResourceMonitor()