"""Loki API metrics collector"""

import threading
from typing import Dict, Any


class LokiMetrics:
    """Collect metrics for Loki API endpoints"""

    def __init__(self):
        self._lock = threading.Lock()

        # Push metrics
        self.push_requests_total = 0
        self.push_logs_total = 0
        self.push_streams_total = 0
        self.push_errors_total = 0
        self.push_last_timestamp = 0

        # Query metrics
        self.query_requests_total = 0
        self.query_errors_total = 0
        self.query_logs_returned_total = 0
        self.query_last_timestamp = 0

        # Query range metrics
        self.query_range_requests_total = 0
        self.query_range_errors_total = 0
        self.query_range_logs_returned_total = 0

        # Label metrics
        self.label_requests_total = 0
        self.label_values_requests_total = 0

        # Series metrics
        self.series_requests_total = 0
        self.series_errors_total = 0

        # Index stats metrics
        self.index_stats_requests_total = 0
        self.index_volume_requests_total = 0
        self.index_volume_range_requests_total = 0

    def record_push(self, logs_count: int, streams_count: int, error: bool = False):
        """Record a push request"""
        with self._lock:
            self.push_requests_total += 1
            self.push_logs_total += logs_count
            self.push_streams_total += streams_count
            if error:
                self.push_errors_total += 1
            import time

            self.push_last_timestamp = int(time.time())

    def record_query(self, logs_returned: int = 0, error: bool = False):
        """Record a query request"""
        with self._lock:
            self.query_requests_total += 1
            self.query_logs_returned_total += logs_returned
            if error:
                self.query_errors_total += 1
            import time

            self.query_last_timestamp = int(time.time())

    def record_query_range(self, logs_returned: int = 0, error: bool = False):
        """Record a query_range request"""
        with self._lock:
            self.query_range_requests_total += 1
            self.query_range_logs_returned_total += logs_returned
            if error:
                self.query_range_errors_total += 1

    def record_label_request(self):
        """Record a label request"""
        with self._lock:
            self.label_requests_total += 1

    def record_label_values_request(self):
        """Record a label values request"""
        with self._lock:
            self.label_values_requests_total += 1

    def record_series_request(self, error: bool = False):
        """Record a series request"""
        with self._lock:
            self.series_requests_total += 1
            if error:
                self.series_errors_total += 1

    def record_index_stats_request(self):
        """Record an index stats request"""
        with self._lock:
            self.index_stats_requests_total += 1

    def record_index_volume_request(self):
        """Record an index volume request"""
        with self._lock:
            self.index_volume_requests_total += 1

    def record_index_volume_range_request(self):
        """Record an index volume range request"""
        with self._lock:
            self.index_volume_range_requests_total += 1

    def get_stats(self) -> Dict[str, Any]:
        """Get current metrics stats"""
        with self._lock:
            return {
                # Push metrics
                "push_requests_total": self.push_requests_total,
                "push_logs_total": self.push_logs_total,
                "push_streams_total": self.push_streams_total,
                "push_errors_total": self.push_errors_total,
                "push_last_timestamp": self.push_last_timestamp,
                # Query metrics
                "query_requests_total": self.query_requests_total,
                "query_errors_total": self.query_errors_total,
                "query_logs_returned_total": self.query_logs_returned_total,
                "query_last_timestamp": self.query_last_timestamp,
                # Query range metrics
                "query_range_requests_total": self.query_range_requests_total,
                "query_range_errors_total": self.query_range_errors_total,
                "query_range_logs_returned_total": self.query_range_logs_returned_total,
                # Label metrics
                "label_requests_total": self.label_requests_total,
                "label_values_requests_total": self.label_values_requests_total,
                # Series metrics
                "series_requests_total": self.series_requests_total,
                "series_errors_total": self.series_errors_total,
                # Index metrics
                "index_stats_requests_total": self.index_stats_requests_total,
                "index_volume_requests_total": self.index_volume_requests_total,
                "index_volume_range_requests_total": self.index_volume_range_requests_total,
            }


# Global metrics instance
loki_metrics = LokiMetrics()
