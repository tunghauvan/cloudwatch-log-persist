import os
import sys
import threading
import time
import queue
import json
import uuid
from pathlib import Path
from typing import Dict, Any, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from datetime import datetime


class LogBuffer:
    def __init__(
        self,
        max_size: int = 10000,
        flush_interval_seconds: int = 5,
        wal_enabled: bool = False,
        wal_dir: str = "wal",
    ):
        self.max_size = max_size
        self.flush_interval_seconds = flush_interval_seconds
        self.wal_enabled = wal_enabled
        self.wal_dir = Path(wal_dir)
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush_time = time.time()
        self._flush_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._warehouse = None
        self._last_error: Optional[str] = None
        self._current_wal_file: Optional[Path] = None

        if self.wal_enabled:
            self.wal_dir.mkdir(parents=True, exist_ok=True)
            self._recover_wal()

    def _get_active_wal_file(self) -> Path:
        """Get or create the current active WAL file for appending."""
        if self._current_wal_file and self._current_wal_file.exists():
            return self._current_wal_file
        
        wal_id = f"active_{int(time.time())}.wal"
        self._current_wal_file = self.wal_dir / wal_id
        return self._current_wal_file

    def _recover_wal(self):
        """Load pending logs from WAL files on startup."""
        print(f"[LogBuffer] Recovering WAL from {self.wal_dir}...")
        wal_files = sorted(list(self.wal_dir.glob("*.wal")))
        recovered_count = 0
        for wal_file in wal_files:
            try:
                with open(wal_file, "r") as f:
                    for line in f:
                        if line.strip():
                            self._buffer.append(json.loads(line))
                            recovered_count += 1
                # We don't delete yet, flush will handle cleanup if successful
                # Or we can rename them to .recovered to avoid double recovery if crash during recovery
                wal_file.rename(wal_file.with_suffix(".wal.pending"))
            except Exception as e:
                print(f"[LogBuffer] Failed to recover WAL file {wal_file}: {e}")
        
        if recovered_count > 0:
            print(f"[LogBuffer] Recovered {recovered_count} logs from WAL.")

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

    def _write_to_wal(self, logs: List[Dict[str, Any]]) -> Optional[Path]:
        if not self.wal_enabled:
            return None
        
        wal_file = self._get_active_wal_file()
        
        try:
            with open(wal_file, "a") as f: # Append mode
                for log in logs:
                    f.write(json.dumps(log) + "\n")
            return wal_file
        except Exception as e:
            print(f"[LogBuffer] WAL Write Error: {e}")
            self._last_error = f"WAL Write Error: {e}"
            return None

    def add(self, logs: List[Dict[str, Any]]):
        wal_file = self._write_to_wal(logs)
        
        with self._lock:
            self._buffer.extend(logs)

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
                # Remove any table metadata
                for log in logs_to_write:
                    log.pop("_warehouse_table", None)
                
                # Insert all logs into the configured table
                self._warehouse.insert_logs(logs_to_write)
                
                # Cleanup WAL only after successful write to Iceberg
                if self.wal_enabled:
                    self._cleanup_wal()
                    
                return len(logs_to_write)
            except Exception as e:
                self._last_error = str(e)
                print(f"[LogBuffer] Flush failed, keeping logs in buffer: {e}")
                with self._lock:
                    # Prepend failed logs back to buffer
                    self._buffer = logs_to_write + self._buffer
                return 0
        return 0

    def _cleanup_wal(self):
        """Remove processed WAL files and reset active file."""
        try:
            # First, check files that were pending or older
            for wal_file in self.wal_dir.glob("*.wal*"):
                if self._current_wal_file and wal_file == self._current_wal_file:
                    continue # Keep appending to the current active file until flush
                wal_file.unlink()
            
            # If we just flushed everything, we can safely rotate the current active file
            if self._current_wal_file and self._current_wal_file.exists():
                 self._current_wal_file.unlink()
                 self._current_wal_file = None # Will create a new one on next add
        except Exception as e:
            print(f"[LogBuffer] WAL Cleanup Error: {e}")

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
