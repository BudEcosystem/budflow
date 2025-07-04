"""Simple metrics module for monitoring."""

from typing import Dict, Any, Optional, List


class Metrics:
    """Simple metrics collector."""
    
    def __init__(self):
        self._counters: Dict[str, int] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = {}
    
    def increment(self, name: str, value: int = 1, tags: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter metric."""
        key = self._make_key(name, tags)
        self._counters[key] = self._counters.get(key, 0) + value
    
    def gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge metric."""
        key = self._make_key(name, tags)
        self._gauges[key] = value
    
    def histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram metric."""
        key = self._make_key(name, tags)
        if key not in self._histograms:
            self._histograms[key] = []
        self._histograms[key].append(value)
    
    def _make_key(self, name: str, tags: Optional[Dict[str, str]] = None) -> str:
        """Create a metric key with optional tags."""
        if not tags:
            return name
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name},{tag_str}"
    
    def get_counter(self, name: str, tags: Optional[Dict[str, str]] = None) -> int:
        """Get counter value."""
        key = self._make_key(name, tags)
        return self._counters.get(key, 0)
    
    def get_gauge(self, name: str, tags: Optional[Dict[str, str]] = None) -> Optional[float]:
        """Get gauge value."""
        key = self._make_key(name, tags)
        return self._gauges.get(key)
    
    def reset(self) -> None:
        """Reset all metrics."""
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()


# Global metrics instance
metrics = Metrics()