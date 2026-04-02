import sys
from pathlib import Path

from flask import Blueprint, request, jsonify
import json
import time

ingest_bp = Blueprint("ingest", __name__)


def get_warehouse():
    from cloudwatch_local_service.server import warehouse

    return warehouse


def get_log_buffer():
    from cloudwatch_local_service.server import log_buffer

    return log_buffer


def parse_labels_from_headers():
    labels = {}
    prefix = "X-Log-Label-"
    for header, value in request.headers:
        if header.startswith(prefix):
            label_name = header[len(prefix) :].lower()
            labels[label_name] = value
    return labels


def parse_labels_from_config():
    from cloudwatch_local_service.server import config

    ingest_config = config.get("ingest", {})
    labels_config = ingest_config.get("labels", {})
    return labels_config.get("columns", [])


@ingest_bp.route("/ingest/logs", methods=["POST"])
def ingest_logs():
    log_buffer = get_log_buffer()
    warehouse = get_warehouse()

    if not log_buffer or not warehouse:
        return jsonify({"error": "service not available"}), 503

    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "invalid JSON body"}), 400

    messages = data.get("messages", [])
    if isinstance(messages, str):
        messages = [messages]

    log_group = data.get("log_group") or request.headers.get("X-Log-Group", "default")
    log_stream = data.get("log_stream") or request.headers.get(
        "X-Log-Stream", "default"
    )

    header_labels = parse_labels_from_headers()
    label_columns = parse_labels_from_config()

    if not messages:
        return jsonify({"error": "no messages provided"}), 400

    ingestion_time = int(time.time() * 1000)
    timestamp = data.get("timestamp")
    if timestamp:
        if isinstance(timestamp, int):
            timestamp_ms = timestamp
        else:
            timestamp_ms = int(timestamp)
    else:
        timestamp_ms = ingestion_time

    warehouse_logs = []
    for msg in messages:
        # Detect if message is JSON and extract common labels
        auto_labels = {}
        if isinstance(msg, str) and (msg.startswith("{") and msg.endswith("}")):
            try:
                msg_json = json.loads(msg)
                # Auto labels from common fields
                for k in ["app", "env", "service", "namespace", "pod", "container", "level"]:
                    if k in msg_json:
                        auto_labels[k] = str(msg_json[k])
                
                # Nested kubernetes labels
                if "kubernetes" in msg_json and isinstance(msg_json["kubernetes"], dict):
                    k8s = msg_json["kubernetes"]
                    for k in ["pod_name", "container_name", "namespace"]:
                        if k in k8s:
                            mapped_k = k.replace("_name", "")
                            auto_labels[mapped_k] = str(k8s[k])
                    if "labels" in k8s and isinstance(k8s["labels"], dict):
                        for k, v in k8s["labels"].items():
                            auto_labels[k] = str(v)
            except Exception:
                pass
        elif isinstance(msg, dict):
            # Already a dict, extract labels
            for k in ["app", "env", "service", "namespace", "pod", "container", "level"]:
                if k in msg:
                    auto_labels[k] = str(msg[k])
            if "kubernetes" in msg and isinstance(msg["kubernetes"], dict):
                k8s = msg["kubernetes"]
                for k in ["pod_name", "container_name", "namespace"]:
                    if k in k8s:
                        mapped_k = k.replace("_name", "")
                        auto_labels[mapped_k] = str(k8s[k])
                if "labels" in k8s and isinstance(k8s["labels"], dict):
                    for k, v in k8s["labels"].items():
                        auto_labels[k] = str(v)

        log_entry = {
            "logGroupName": log_group,
            "logStreamName": log_stream,
            "timestamp": timestamp_ms * 1000, # ms to us
            "message": msg if isinstance(msg, str) else json.dumps(msg),
            "ingestionTime": timestamp_ms * 1000, # Use event timestamp for ingestion time
            "sequenceToken": 0,
        }

        for label_col in label_columns:
            # Priority: Header -> Auto-extracted from JSON -> Empty
            label_value = header_labels.get(label_col) or auto_labels.get(label_col, "")
            log_entry[f"label_{label_col}"] = label_value

        warehouse_logs.append(log_entry)

    try:
        log_buffer.add(warehouse_logs)
        return jsonify(
            {
                "status": "ok",
                "accepted": len(warehouse_logs),
                "labels": header_labels,
                "log_group": log_group,
                "log_stream": log_stream,
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@ingest_bp.route("/ingest/logs/batch", methods=["POST"])
def ingest_logs_batch():
    log_buffer = get_log_buffer()
    warehouse = get_warehouse()

    if not log_buffer or not warehouse:
        return jsonify({"error": "service not available"}), 503

    try:
        data = request.get_json(force=True) or {}
    except Exception:
        return jsonify({"error": "invalid JSON body"}), 400

    logs = data.get("logs", [])
    if not isinstance(logs, list):
        return jsonify({"error": "logs must be an array"}), 400

    label_columns = parse_labels_from_config()
    ingestion_time = int(time.time() * 1000)

    warehouse_logs = []
    for log_entry in logs:
        log_group = (
            log_entry.get("log_group") or log_entry.get("logGroupName") or "default"
        )
        log_stream = (
            log_entry.get("log_stream") or log_entry.get("logStreamName") or "default"
        )
        message = log_entry.get("message", "")
        timestamp = log_entry.get("timestamp")

        if timestamp:
            if isinstance(timestamp, int):
                timestamp_ms = timestamp
            else:
                timestamp_ms = int(timestamp)
        else:
            timestamp_ms = ingestion_time

        header_labels = parse_labels_from_headers()

        processed_entry = {
            "logGroupName": log_group,
            "logStreamName": log_stream,
            "timestamp": timestamp_ms,
            "message": message if isinstance(message, str) else str(message),
            "ingestionTime": ingestion_time,
            "sequenceToken": 0,
        }

        for label_col in label_columns:
            label_value = (
                log_entry.get(f"label_{label_col}")
                or log_entry.get(f"label-{label_col}")
                or header_labels.get(label_col, "")
            )
            processed_entry[f"label_{label_col}"] = label_value

        warehouse_logs.append(processed_entry)

    try:
        log_buffer.add(warehouse_logs)
        return jsonify(
            {
                "status": "ok",
                "accepted": len(warehouse_logs),
            }
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@ingest_bp.route("/ingest/health", methods=["GET"])
def ingest_health():
    log_buffer = get_log_buffer()
    if log_buffer:
        return jsonify(
            {
                "status": "ok",
                "buffer": log_buffer.stats(),
            }
        )
    return jsonify({"status": "unavailable"}), 503
