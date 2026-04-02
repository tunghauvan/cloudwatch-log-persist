"""Warehouse metrics collector"""

import threading
import time
from typing import Dict, Any


class WarehouseMetrics:
    """Collect metrics for warehouse operations"""

    def __init__(self):
        self._lock = threading.Lock()

        # Query metrics
        self.queries_total = 0
        self.query_errors_total = 0
        self.query_duration_seconds_total = 0.0
        self.query_logs_returned_total = 0
        self.query_last_timestamp = 0

        # Insert metrics
        self.inserts_total = 0
        self.insert_errors_total = 0
        self.insert_duration_seconds_total = 0.0
        self.insert_logs_total = 0
        self.insert_last_timestamp = 0

        # Maintenance metrics
        self.compactions_total = 0
        self.retention_runs_total = 0
        self.retention_removed_partitions_total = 0

        # Cache for stats
        self._cached_log_groups_count = 0
        self._cached_log_streams_count = 0
        self._cache_timestamp = 0
        self._cache_ttl_seconds = 60  # Cache stats for 60 seconds

    def record_query(
        self, logs_returned: int = 0, duration_seconds: float = 0.0, error: bool = False
    ):
        """Record a query operation"""
        with self._lock:
            self.queries_total += 1
            self.query_logs_returned_total += logs_returned
            self.query_duration_seconds_total += duration_seconds
            if error:
                self.query_errors_total += 1
            self.query_last_timestamp = int(time.time())

    def record_insert(
        self, logs_count: int = 0, duration_seconds: float = 0.0, error: bool = False
    ):
        """Record an insert operation"""
        with self._lock:
            self.inserts_total += 1
            self.insert_logs_total += logs_count
            self.insert_duration_seconds_total += duration_seconds
            if error:
                self.insert_errors_total += 1
            self.insert_last_timestamp = int(time.time())

    def record_compaction(self):
        """Record a compaction operation"""
        with self._lock:
            self.compactions_total += 1

    def record_retention(self, removed_partitions: int = 0):
        """Record a retention enforcement run"""
        with self._lock:
            self.retention_runs_total += 1
            self.retention_removed_partitions_total += removed_partitions

    def update_stats_cache(self, log_groups: int = 0, log_streams: int = 0):
        """Update cached stats (called periodically)"""
        with self._lock:
            self._cached_log_groups_count = log_groups
            self._cached_log_streams_count = log_streams
            self._cache_timestamp = int(time.time())

    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics stats"""
        with self._lock:
            # Calculate averages
            avg_query_duration = self.query_duration_seconds_total / max(
                self.queries_total, 1
            )
            avg_insert_duration = self.insert_duration_seconds_total / max(
                self.inserts_total, 1
            )

            return {
                # Query metrics
                "queries_total": self.queries_total,
                "query_errors_total": self.query_errors_total,
                "query_duration_seconds_total": round(
                    self.query_duration_seconds_total, 3
                ),
                "query_avg_duration_seconds": round(avg_query_duration, 6),
                "query_logs_returned_total": self.query_logs_returned_total,
                "query_last_timestamp": self.query_last_timestamp,
                # Insert metrics
                "inserts_total": self.inserts_total,
                "insert_errors_total": self.insert_errors_total,
                "insert_duration_seconds_total": round(
                    self.insert_duration_seconds_total, 3
                ),
                "insert_avg_duration_seconds": round(avg_insert_duration, 6),
                "insert_logs_total": self.insert_logs_total,
                "insert_last_timestamp": self.insert_last_timestamp,
                # Maintenance metrics
                "compactions_total": self.compactions_total,
                "retention_runs_total": self.retention_runs_total,
                "retention_removed_partitions_total": self.retention_removed_partitions_total,
                # Cached stats
                "log_groups_count": self._cached_log_groups_count,
                "log_streams_count": self._cached_log_streams_count,
                "stats_cache_age_seconds": int(time.time()) - self._cache_timestamp,
            }


# Global metrics instance
warehouse_metrics = WarehouseMetrics()
