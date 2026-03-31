import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional


class FileStorage:
    def __init__(self, logs_dir: Path):
        self.logs_dir = logs_dir
        self.logs_dir.mkdir(parents=True, exist_ok=True)

    def save_logs(
        self,
        log_group_name: str,
        log_stream_name: str,
        log_events: List[Dict],
        ingestion_time: int,
    ) -> None:
        file_path = self._get_file_path(log_group_name, log_stream_name)
        try:
            with open(file_path, "a") as f:
                for event in log_events:
                    log_entry = {
                        "timestamp": event.get("timestamp"),
                        "message": event.get("message"),
                        "ingestionTime": ingestion_time,
                        "logGroup": log_group_name,
                        "logStream": log_stream_name,
                    }
                    f.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            print(f"Error saving logs to file: {e}")

    def load_logs(
        self,
        log_group_name: str,
        log_stream_name: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict]:
        file_path = self._get_file_path(log_group_name, log_stream_name)
        events = []
        try:
            if file_path.exists():
                with open(file_path, "r") as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line)
                            ts = entry.get("timestamp", 0)

                            if start_time and ts < start_time:
                                continue
                            if end_time and ts > end_time:
                                continue

                            events.append(
                                {
                                    "timestamp": entry.get("timestamp"),
                                    "message": entry.get("message"),
                                    "ingestionTime": entry.get("ingestionTime"),
                                }
                            )
        except Exception as e:
            print(f"Error loading logs from file: {e}")

        events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return events[:limit]

    def get_log_groups(self) -> Dict[str, Dict]:
        groups = {}
        try:
            for group_dir in self.logs_dir.iterdir():
                if group_dir.is_dir():
                    group_name = group_dir.name.replace("_", "/")
                    has_streams = False
                    for log_file in group_dir.glob("*.jsonl"):
                        has_streams = True
                        break

                    if has_streams:
                        groups[group_name] = {
                            "logGroupName": group_name,
                            "creationTime": int(group_dir.stat().st_ctime * 1000),
                            "metricFilterCount": 0,
                            "arn": f"arn:aws:logs:us-east-1:123456789012:log-group:{group_name}",
                            "storedBytes": sum(
                                f.stat().st_size for f in group_dir.glob("*.jsonl")
                            ),
                        }
        except Exception as e:
            print(f"Error getting log groups: {e}")
        return groups

    def get_log_streams(self, log_group_name: str) -> List[Dict]:
        streams = []
        safe_group_name = log_group_name.replace("/", "_")
        group_dir = self.logs_dir / safe_group_name

        try:
            if group_dir.exists():
                for log_file in group_dir.glob("*.jsonl"):
                    stream_name = log_file.stem.replace("_", "/")
                    streams.append(
                        {
                            "logStreamName": stream_name,
                            "creationTime": int(log_file.stat().st_ctime * 1000),
                            "arn": f"arn:aws:logs:us-east-1:123456789012:log-stream:{log_group_name}/{stream_name}",
                            "storedBytes": log_file.stat().st_size,
                        }
                    )
        except Exception as e:
            print(f"Error getting log streams: {e}")
        return streams

    def filter_logs(
        self,
        log_group_name: str,
        log_stream_name_prefix: Optional[str] = None,
        filter_pattern: str = "",
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict]:
        safe_group_name = log_group_name.replace("/", "_")
        group_dir = self.logs_dir / safe_group_name
        events = []

        try:
            if not group_dir.exists():
                return events

            for log_file in group_dir.glob("*.jsonl"):
                stream_name = log_file.stem.replace("_", "/")

                if log_stream_name_prefix and not stream_name.startswith(
                    log_stream_name_prefix
                ):
                    continue

                with open(log_file, "r") as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line)
                            ts = entry.get("timestamp", 0)

                            if start_time and ts < start_time:
                                continue
                            if end_time and ts > end_time:
                                continue

                            if filter_pattern:
                                from cloudwatch_local_service.utils.helpers import (
                                    parse_filter_pattern,
                                )

                                if not parse_filter_pattern(
                                    filter_pattern, entry.get("message", "")
                                ):
                                    continue

                            events.append(
                                {
                                    "timestamp": entry.get("timestamp"),
                                    "message": entry.get("message"),
                                    "ingestionTime": entry.get("ingestionTime"),
                                }
                            )
        except Exception as e:
            print(f"Error filtering logs: {e}")

        events.sort(key=lambda x: x.get("timestamp", 0), reverse=True)
        return events[:limit]

    def get_all_logs_from_files(self) -> Dict[str, Dict[str, List[Dict]]]:
        result = {}
        try:
            for group_dir in self.logs_dir.iterdir():
                if group_dir.is_dir():
                    group_name = group_dir.name.replace("_", "/")
                    result[group_name] = {}
                    for log_file in group_dir.glob("*.jsonl"):
                        stream_name = log_file.stem.replace("_", "/")
                        with open(log_file, "r") as f:
                            events = []
                            for line in f:
                                if line.strip():
                                    events.append(json.loads(line))
                            result[group_name][stream_name] = events
        except Exception as e:
            print(f"Error reading logs from files: {e}")
        return result

    def _get_file_path(self, log_group_name: str, log_stream_name: str) -> Path:
        safe_group_name = log_group_name.replace("/", "_")
        safe_stream_name = log_stream_name.replace("/", "_")
        group_dir = self.logs_dir / safe_group_name
        group_dir.mkdir(parents=True, exist_ok=True)
        return group_dir / f"{safe_stream_name}.jsonl"
