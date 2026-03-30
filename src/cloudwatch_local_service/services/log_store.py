import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import threading
import time
from typing import Dict, List, Any, Optional


class LogStore:
    def __init__(self):
        self._store: Dict[str, Dict[str, Any]] = {}
        self._sequence_tokens: Dict[str, int] = {}
        self._lock = threading.Lock()

    def add_log_group(
        self,
        log_group_name: str,
        log_stream_name: str,
        events: List[Dict],
        ingestion_time: int,
    ) -> int:
        key = f"{log_group_name}/{log_stream_name}"
        with self._lock:
            if key not in self._store:
                self._store[key] = {
                    "logGroupName": log_group_name,
                    "logStreamName": log_stream_name,
                    "events": [],
                    "created_at": ingestion_time,
                }
                self._sequence_tokens[key] = 1

            for event in events:
                self._store[key]["events"].append(
                    {
                        "timestamp": event.get("timestamp"),
                        "message": event.get("message"),
                        "ingestionTime": ingestion_time,
                    }
                )

            new_sequence = self._sequence_tokens[key] + len(events)
            self._sequence_tokens[key] = new_sequence
            return new_sequence

    def get_sequence_token(self, key: str) -> str:
        with self._lock:
            return str(self._sequence_tokens.get(key, 1))

    def get_events(
        self,
        log_group_name: str,
        log_stream_name: Optional[str] = None,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict]:
        key_prefix = f"{log_group_name}/"
        with self._lock:
            events = []
            for key, data in self._store.items():
                if log_stream_name:
                    full_key = f"{log_group_name}/{log_stream_name}"
                    if key != full_key:
                        continue
                elif not key.startswith(key_prefix):
                    continue

                entry_events = data.get("events", [])
                filtered = entry_events

                if start_time:
                    filtered = [
                        e for e in filtered if e.get("timestamp", 0) >= start_time
                    ]
                if end_time:
                    filtered = [
                        e for e in filtered if e.get("timestamp", 0) <= end_time
                    ]

                events.extend(filtered)

            events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            return events[:limit]

    def filter_events(
        self,
        log_group_name: str,
        log_stream_name_prefix: Optional[str] = None,
        filter_pattern: str = "",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict]:
        from cloudwatch_local_service.utils.helpers import parse_filter_pattern

        key_prefix = f"{log_group_name}/"
        with self._lock:
            events = []
            for key, data in self._store.items():
                if not key.startswith(key_prefix):
                    continue

                if log_stream_name_prefix:
                    stream_name = key.replace(key_prefix, "")
                    if not stream_name.startswith(log_stream_name_prefix):
                        continue

                entry_events = data.get("events", [])
                filtered = entry_events

                if start_time:
                    filtered = [
                        e for e in filtered if e.get("timestamp", 0) >= start_time
                    ]
                if end_time:
                    filtered = [
                        e for e in filtered if e.get("timestamp", 0) <= end_time
                    ]

                if filter_pattern:
                    filtered = [
                        e
                        for e in filtered
                        if parse_filter_pattern(filter_pattern, e.get("message", ""))
                    ]

                events.extend(filtered)

            events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
            return events[:limit]

    def get_all_log_groups(self) -> Dict[str, Dict]:
        with self._lock:
            groups = {}
            for key, data in self._store.items():
                group_name = data["logGroupName"]
                if group_name not in groups:
                    groups[group_name] = {
                        "logGroupName": group_name,
                        "creationTime": data["created_at"],
                        "metricFilterCount": 0,
                        "arn": f"arn:aws:logs:us-east-1:123456789012:log-group:{group_name}",
                        "storedBytes": 0,
                    }
            return groups

    def get_log_streams(self, log_group_name: str) -> List[Dict]:
        prefix = f"{log_group_name}/"
        with self._lock:
            streams = []
            for key, data in self._store.items():
                if key.startswith(prefix):
                    streams.append(
                        {
                            "logStreamName": data["logStreamName"],
                            "creationTime": data["created_at"],
                            "arn": f"arn:aws:logs:us-east-1:123456789012:log-stream:{key}",
                            "storedBytes": 0,
                        }
                    )
            return streams

    def get_all(self) -> Dict:
        with self._lock:
            return dict(self._store)

    def clear(self):
        with self._lock:
            self._store.clear()
            self._sequence_tokens.clear()


log_store = LogStore()
