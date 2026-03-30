import json
import os
from pathlib import Path
from typing import List, Dict, Any


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

    def load_logs(self, log_group_name: str, log_stream_name: str) -> List[Dict]:
        file_path = self._get_file_path(log_group_name, log_stream_name)
        events = []
        try:
            if file_path.exists():
                with open(file_path, "r") as f:
                    for line in f:
                        if line.strip():
                            events.append(json.loads(line))
        except Exception as e:
            print(f"Error loading logs from file: {e}")
        return events

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
