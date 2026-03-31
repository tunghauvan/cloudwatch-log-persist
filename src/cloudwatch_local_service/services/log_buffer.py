import os
import sys
import threading
import time
import queue
from pathlib import Path
from typing import Dict, Any, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime


class LogBuffer:
    def __init__(
        self,
        max_size: int = 10000,
        flush_interval_seconds: int = 5,
    ):
        self.max_size = max_size
        self.flush_interval_seconds = flush_interval_seconds
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush_time = time.time()
        self._flush_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._warehouse = None
        self._last_error: Optional[str] = None

    def set_warehouse(self, warehouse):
        self._warehouse = warehouse

    def start(self):
        if self._flush_thread is None:
            self._stop_event.clear()
            self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
            self._flush_thread.start()

    def stop(self):
        self._stop_event.set()
        if self._flush_thread:
            self._flush_thread.join(timeout=10)
        self.flush()

    def add(self, logs: List[Dict[str, Any]]):
        with self._lock:
            self._buffer.extend(logs)
            self._last_flush_time = time.time()

        if len(self._buffer) >= self.max_size:
            try:
                self.flush()
            except Exception as e:
                self._last_error = str(e)

    def flush(self) -> int:
        with self._lock:
            if not self._buffer:
                return 0

            logs_to_write = self._buffer.copy()
            self._buffer.clear()
            self._last_flush_time = time.time()

        if self._warehouse and logs_to_write:
            try:
                self._warehouse.insert_logs(logs_to_write)
                return len(logs_to_write)
            except Exception as e:
                self._last_error = str(e)
                with self._lock:
                    self._buffer.extend(logs_to_write)
                return 0
        return 0

    def _flush_loop(self):
        while not self._stop_event.is_set():
            time.sleep(1)
            if time.time() - self._last_flush_time >= self.flush_interval_seconds:
                try:
                    self.flush()
                except Exception as e:
                    self._last_error = str(e)
                    print(f"[LogBuffer] Flush error: {e}")

    def size(self) -> int:
        with self._lock:
            return len(self._buffer)

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "buffer_size": len(self._buffer),
                "max_size": self.max_size,
                "flush_interval_seconds": self.flush_interval_seconds,
                "last_flush_age_seconds": time.time() - self._last_flush_time,
                "last_error": self._last_error,
            }


log_buffer = LogBuffer()
