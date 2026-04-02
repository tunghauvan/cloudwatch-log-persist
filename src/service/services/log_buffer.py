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

import logging

logger = logging.getLogger("service.buffer")

class LogBuffer:
    def __init__(
        self,
        max_size: int = 10000,
        flush_interval_seconds: int = 5,
        wal_enabled: bool = False,
        wal_dir: str = "wal",
        worker_threads: int = 2,
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

        # Background worker pool for flushing
        self._flush_queue = queue.Queue()
        self._worker_threads: List[threading.Thread] = []
        self._num_workers = worker_threads

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
        logger.info(f"Recovering WAL from {self.wal_dir}...")
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
                logger.error(f"Failed to recover WAL file {wal_file}: {e}")
        
        if recovered_count > 0:
            logger.info(f"Recovered {recovered_count} logs from WAL.")

    def set_warehouse(self, warehouse):
        self._warehouse = warehouse

    def start(self):
        if self._flush_thread is None:
            self._stop_event.clear()
            self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
            self._flush_thread.start()
        
        # Start worker threads
        if not self._worker_threads:
            for i in range(self._num_workers):
                t = threading.Thread(target=self._worker_loop, name=f"LogWorker-{i}", daemon=True)
                t.start()
                self._worker_threads.append(t)
            logger.info(f"Started {self._num_workers} worker threads.")

    def stop(self):
        self._stop_event.set()
        
        # Wait for workers to finish current tasks
        logger.info("Stopping workers and flushing remaining logs...")
        for t in self._worker_threads:
            t.join(timeout=5)
            
        if self._flush_thread:
            self._flush_thread.join(timeout=10)
        
        # Final explicit flush of whatever is in memory buffer
        self.flush()

    def _worker_loop(self):
        """Background worker consuming logs from the queue and writing to warehouse."""
        while not self._stop_event.is_set() or not self._flush_queue.empty():
            try:
                # Use a small timeout to allow checking stop_event periodically
                logs_to_write = self._flush_queue.get(timeout=1.0)
                if logs_to_write:
                    self._perform_flush(logs_to_write)
                self._flush_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Worker error: {e}")
                self._last_error = f"Worker Error: {e}"

    def _perform_flush(self, logs_to_write: List[Dict[str, Any]]):
        """Actual logic to write logs to warehouse."""
        if self._warehouse and logs_to_write:
            try:
                # Group logs by table
                table_groups = {}
                for log in logs_to_write:
                    table_name = log.pop("_warehouse_table", None)
                    if table_name not in table_groups:
                        table_groups[table_name] = []
                    table_groups[table_name].append(log)
                
                # Insert logs for each table group
                for table_name, logs in table_groups.items():
                    # If table_name is None, it will use default cloudwatch_logs table
                    self._warehouse.insert_logs(logs, table_name=table_name)
                
                # Cleanup WAL only after successful write to Iceberg
                if self.wal_enabled:
                    self._cleanup_wal()
                    
                return len(logs_to_write)
            except Exception as e:
                self._last_error = str(e)
                logger.error(f"Flush failed, keeping logs in buffer: {e}")
                with self._lock:
                    # Prepend failed logs back to buffer (need to restore metadata if possible)
                    # But for now, we just prepend them back
                    self._buffer = logs_to_write + self._buffer
                return 0
        return 0

    def _write_to_wal(self, logs: List[Dict[str, Any]]):
        """Write logs to the active WAL file."""
        if not self.wal_enabled:
            return None
            
        try:
            wal_file = self._get_active_wal_file()
            with open(wal_file, "a") as f:
                for log in logs:
                    f.write(json.dumps(log) + "\n")
            return wal_file
        except Exception as e:
            logger.error(f"WAL Write Error: {e}")
            return None

    def add(self, logs: List[Dict[str, Any]]):
        wal_file = self._write_to_wal(logs)
        
        with self._lock:
            self._buffer.extend(logs)

        if len(self._buffer) >= self.max_size:
            self.flush()

    def flush(self) -> int:
        with self._lock:
            if not self._buffer:
                return 0

            logs_to_write = self._buffer.copy()
            self._buffer.clear()
            self._last_flush_time = time.time()

        # Enqueue for background worker instead of blocking
        self._flush_queue.put(logs_to_write)
        return len(logs_to_write)

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
                "queue_size": self._flush_queue.qsize(),
                "num_workers": len(self._worker_threads),
            }


log_buffer = LogBuffer()
