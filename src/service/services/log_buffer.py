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
        self._wal_file_handle = None
        self._wal_lock = threading.Lock()
        self._max_wal_size_mb = 10  # Rotate WAL after 10MB

        # Background worker pool for flushing
        self._flush_queue = queue.Queue()
        self._worker_threads: List[threading.Thread] = []
        self._num_workers = worker_threads

        if self.wal_enabled:
            self.wal_dir.mkdir(parents=True, exist_ok=True)
            self._recover_wal()

    def _get_active_wal_file(self) -> Path:
        """Get or create the current active WAL file for appending."""
        with self._wal_lock:
            # Check if current handle needs rotation
            if self._current_wal_file and self._current_wal_file.exists():
                if self._current_wal_file.stat().st_size >= self._max_wal_size_mb * 1024 * 1024:
                    if self._wal_file_handle:
                        self._wal_file_handle.close()
                        self._wal_file_handle = None
                    # Rename to mark as ready for flush/archive
                    self._current_wal_file.rename(self._current_wal_file.with_suffix(".wal.full"))
                    self._current_wal_file = None

            if self._current_wal_file is None:
                wal_id = f"segment_{int(time.time() * 1000)}.wal"
                self._current_wal_file = self.wal_dir / wal_id
                self._wal_file_handle = open(self._current_wal_file, "a", buffering=1) # Line buffered
            
            return self._current_wal_file

    def _recover_wal(self):
        """Load pending logs from WAL files on startup."""
        logger.info(f"Recovering WAL segments from {self.wal_dir}...")
        # Recover both .wal and .wal.full files
        wal_files = sorted(list(self.wal_dir.glob("*.wal*")))
        recovered_count = 0
        for wal_file in wal_files:
            if ".pending" in wal_file.suffixes:
                continue
            try:
                with open(wal_file, "r") as f:
                    for line in f:
                        if line.strip():
                            self._buffer.append(json.loads(line))
                            recovered_count += 1
                wal_file.rename(wal_file.with_suffix(".wal.pending"))
            except Exception as e:
                logger.error(f"Failed to recover WAL segment {wal_file}: {e}")
        
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
        """Write logs to the active WAL segment with persistent handle."""
        if not self.wal_enabled:
            return None
            
        try:
            self._get_active_wal_file() # Ensure handle is open
            with self._wal_lock:
                if self._wal_file_handle:
                    for log in logs:
                        self._wal_file_handle.write(json.dumps(log) + "\n")
                    self._wal_file_handle.flush() # Ensure it's on disk
            return self._current_wal_file
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
        """Remove processed WAL segments."""
        with self._wal_lock:
            try:
                # Cleanup .pending and .full segments that are done
                for wal_file in self.wal_dir.glob("*.wal*"):
                    # NEVER delete the currently active file handle's file
                    if self._current_wal_file and wal_file == self._current_wal_file:
                        continue
                    
                    try:
                        wal_file.unlink()
                    except OSError:
                        pass # Might be open by another process
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

    def get_logs(
        self,
        log_group_name: str,
        log_stream_name: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Search logs in the current hot tier (memory buffer)."""
        with self._lock:
            results = []
            for log in self._buffer:
                # Cần xử lý data mapping tương ứng với metadata format
                if log.get("log_group_name") != log_group_name:
                    continue
                
                if log_stream_name and log.get("log_stream_name") != log_stream_name:
                    continue
                
                ts = log.get("timestamp")
                if start_time and ts < start_time:
                    continue
                if end_time and ts > end_time:
                    continue
                
                results.append(log)
                
            results.sort(key=lambda x: x.get("timestamp", 0))
            return results[:limit]

    def get_log_groups(self) -> Dict[str, Dict[str, Any]]:
        """Get distinct log groups from buffer and their metadata."""
        with self._lock:
            groups = {}
            for log in self._buffer:
                group_name = log.get("log_group_name")
                if group_name and group_name not in groups:
                    groups[group_name] = {
                        "logGroupName": group_name,
                        "creationTime": log.get("ingestion_time", 0),
                        "storedBytes": 0,
                    }
            return groups

    def get_log_streams(self, log_group_name: str) -> Dict[str, Dict[str, Any]]:
        """Get distinct log streams for a specific group from buffer."""
        with self._lock:
            streams = {}
            for log in self._buffer:
                if log.get("log_group_name") == log_group_name:
                    stream_name = log.get("log_stream_name")
                    if stream_name and stream_name not in streams:
                        streams[stream_name] = {
                            "logStreamName": stream_name,
                            "creationTime": log.get("ingestion_time", 0),
                            "storedBytes": 0,
                        }
            return streams

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
