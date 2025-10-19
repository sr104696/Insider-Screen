# Production Circuit Breaker with sliding window - based on user's WindowBreaker
import time
import random
from collections import deque
from typing import Dict, Any, Optional

class ProductionWindowBreaker:
    """
    Advanced circuit breaker with Closed/Open/Half-Open states and sliding window
    Based on user's comprehensive WindowBreaker implementation
    """
    def __init__(self, fail_threshold: int = 5, window_sec: int = 60, open_sec: int = 60, halfopen_max: int = 1):
        self.fail_threshold = fail_threshold
        self.window_sec = window_sec
        self.open_sec = open_sec
        self.halfopen_max = halfopen_max
        self.fail_events = {}  # key -> deque of failure timestamps
        self.state = {}        # key -> "closed"/"open"/"half-open"
        self.open_until = {}   # key -> timestamp when can transition to half-open
        self.halfopen_inflight = {}  # key -> count of inflight half-open requests

    def _now(self):
        return time.time()

    def _get_failure_deque(self, key):
        """Get failure events deque for a key"""
        if key not in self.fail_events:
            self.fail_events[key] = deque()
        return self.fail_events[key]

    def _prune_old_failures(self, key):
        """Remove failure events outside the sliding window"""
        cutoff = self._now() - self.window_sec
        dq = self._get_failure_deque(key)
        while dq and dq[0] < cutoff:
            dq.popleft()

    def allow(self, key: str) -> bool:
        """Check if requests are allowed for this key"""
        current_state = self.state.get(key, "closed")
        
        if current_state == "open":
            # Check if we can transition to half-open
            return self._now() > self.open_until.get(key, 0)
        elif current_state == "half-open":
            # Allow limited concurrent requests in half-open
            return self.halfopen_inflight.get(key, 0) < self.halfopen_max
        else:  # closed
            return True

    def record_success(self, key: str):
        """Record successful request - reset to closed state"""
        self.state[key] = "closed"
        self.halfopen_inflight[key] = 0
        # Clear failure history on success
        dq = self._get_failure_deque(key)
        dq.clear()

    def record_failure(self, key: str):
        """Record failed request - may trigger state transitions"""
        self._prune_old_failures(key)
        dq = self._get_failure_deque(key)
        dq.append(self._now())
        
        # Check if we should open the circuit
        if len(dq) >= self.fail_threshold:
            self.state[key] = "open"
            self.open_until[key] = self._now() + self.open_sec

    def on_attempt(self, key: str):
        """Called when starting an attempt - manages state transitions"""
        current_state = self.state.get(key, "closed")
        
        if current_state == "open" and self._now() > self.open_until.get(key, 0):
            # Transition from open to half-open
            self.state[key] = "half-open"
            self.halfopen_inflight[key] = self.halfopen_inflight.get(key, 0)
        
        if self.state.get(key) == "half-open":
            # Increment inflight counter for half-open requests
            self.halfopen_inflight[key] = self.halfopen_inflight.get(key, 0) + 1

    def on_attempt_done(self, key: str):
        """Called when attempt is complete - decrement inflight counter"""
        if self.state.get(key) == "half-open":
            self.halfopen_inflight[key] = max(0, self.halfopen_inflight.get(key, 0) - 1)
    
    def get_state(self, key: str) -> str:
        """Get current circuit state for debugging"""
        return self.state.get(key, "closed")
    
    def get_failure_count(self, key: str) -> int:
        """Get current failure count in window"""
        self._prune_old_failures(key)
        return len(self._get_failure_deque(key))