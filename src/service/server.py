import os
import sys
import time
from pathlib import Path
from flask import Flask, request, jsonify

sys.path.insert(0, str(Path(__file__).parent.parent))

from service.routes.logs import logs_bp
from service.routes.groups import groups_bp
from service.routes.streams import streams_bp
from service.routes.query import query_bp
from service.routes.store import store_bp
from service.routes.ingest import ingest_bp
from service.routes.loki import loki_bp
from service.routes.metrics import metrics_bp
from service.routes.alb import alb_bp
from service.utils.logging_config import setup_logging

logger = setup_logging()

app = Flask(__name__)

config_path = Path(os.getenv("CONFIG_PATH", "/app/config.yaml"))
warehouse = None
log_buffer = None
startup_time = time.time()

try:
    import yaml

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
except Exception as e:
    logger.error(f"[Config] Failed to load from {config_path}: {e}")
    config = {}

buffer_config = config.get("buffer", {})
buffer_max_size = buffer_config.get("max_size", 50000)
buffer_flush_interval = buffer_config.get("flush_interval_seconds", 10)
buffer_worker_threads = buffer_config.get("worker_threads", 2)
wal_enabled = buffer_config.get("wal_enabled", False)
wal_dir = buffer_config.get("wal_dir", "wal")

try:
    from warehouse.warehouse import WarehouseManager
    from service.services.log_buffer import LogBuffer

    warehouse = WarehouseManager(str(config_path))
    warehouse.ensure_warehouse()
    warehouse.start_maintenance()

    log_buffer = LogBuffer(
        max_size=buffer_max_size,
        flush_interval_seconds=buffer_flush_interval,
        wal_enabled=wal_enabled,
        wal_dir=wal_dir,
        worker_threads=buffer_worker_threads,
    )
    log_buffer.set_warehouse(warehouse)
    log_buffer.start()

    stats = warehouse.get_stats()
    logger.info(f"Warehouse initialized: {warehouse.catalog_name} at {warehouse.warehouse_path}")
    logger.debug(f"Warehouse stats: {stats}")
    logger.info(f"Buffer config: max_size={buffer_max_size}, flush_interval={buffer_flush_interval}s, workers={buffer_worker_threads}, wal_enabled={wal_enabled}")

    # Start SQS consumer if queue URL is configured
    alb_cfg = config.get("alb", {})
    if alb_cfg.get("enabled", True) and alb_cfg.get("sqs_queue_url", "").strip():
        import atexit
        from service.services.sqs_consumer import SQSConsumer
        _sqs_consumer = SQSConsumer(warehouse, config)
        _sqs_consumer.start()
        atexit.register(_sqs_consumer.stop)
except Exception as e:
    logger.error(f"Failed to initialize warehouse/buffer: {e}")

app.register_blueprint(logs_bp)
app.register_blueprint(groups_bp)
app.register_blueprint(streams_bp)
app.register_blueprint(query_bp)
app.register_blueprint(store_bp)
app.register_blueprint(ingest_bp)
app.register_blueprint(loki_bp)
app.register_blueprint(metrics_bp)
app.register_blueprint(alb_bp)


@app.route("/", methods=["GET"])
def health():
    response = {"status": "ok", "service": "cloudwatch-logs-local"}
    if log_buffer:
        response["buffer"] = log_buffer.stats()
    return jsonify(response)


@app.route("/", methods=["GET", "POST", "PUT"])
def handle_request():
    logger.debug(f"Request: {request.method} {request.path}")
    if request.method == "POST":
        try:
            body = request.get_json(force=True)
            logger.debug(f"Request body: {body}")
        except:
            pass

    amz_target = request.headers.get("X-Amz-Target", "")

    if amz_target:
        action = amz_target.split(".")[-1] if "." in amz_target else amz_target
    else:
        action = request.args.get("Action")
        if not action and request.method == "POST":
            data = request.get_json(force=True) or {}
            action = data.get("Action")

    if action == "DescribeLogGroups":
        return describe_log_groups()
    elif action == "DescribeLogStreams":
        return describe_log_streams()
    elif action == "GetLogEvents":
        return get_log_events()
    elif action == "PutLogEvents":
        return put_log_events()
    elif action == "FilterLogEvents":
        return filter_log_events()
    elif action == "StartQueryExecution":
        return start_query_execution()
    elif action == "StartQuery":
        return start_query_execution()
    elif action == "GetQueryResults":
        return get_query_execution_results()
    elif action == "GetQuery":
        return get_query_execution_results()

    if request.method == "POST":
        data = request.get_json(force=True) or {}
        if "logEvents" in data:
            return put_log_events()
        else:
            return get_log_events()

    if action == "GetLogEvents" or (not action and request.method == "GET"):
        return get_log_events()
    else:
        return put_log_events()


def get_warehouse():
    return warehouse


def describe_log_groups():
    from service.services.log_store import log_store
    groups = log_store.get_all_log_groups()
    return jsonify({"logGroups": list(groups.values())})


def describe_log_streams():
    from service.services.log_store import log_store
    
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        log_group_name = data.get("logGroupName")
    else:
        log_group_name = request.args.get("logGroupName")

    if not log_group_name:
        return jsonify(
            {"__type": "InvalidParameterException", "message": "Missing logGroupName"}
        ), 400

    streams = log_store.get_log_streams(log_group_name)
    return jsonify({"logStreams": streams})


def put_log_events():
    from service.services.log_store import log_store
    import time

    data = request.get_json(force=True)
    log_group_name = data.get("logGroupName")
    log_stream_name = data.get("logStreamName")
    log_events = data.get("logEvents", [])

    if not log_group_name or not log_stream_name:
        return jsonify(
            {
                "__type": "InvalidParameterException",
                "message": "Missing required parameters",
            }
        ), 400

    key = f"{log_group_name}/{log_stream_name}"
    ingestion_time = int(time.time() * 1000)

    existing_token = request.headers.get("X-Amz-Sequence-Token")
    expected_token = log_store.get_sequence_token(key)

    if existing_token and existing_token != expected_token:
        return jsonify(
            {
                "__type": "InvalidSequenceTokenException",
                "message": "Sequence token is invalid",
                "expectedSequenceToken": expected_token,
            }
        ), 400

    new_sequence = log_store.add_log_group(
        log_group_name, log_stream_name, log_events, ingestion_time
    )
    next_token = str(new_sequence)

    if warehouse and log_buffer:
        try:
            warehouse_logs = [
                {
                    "logGroupName": log_group_name,
                    "logStreamName": log_stream_name,
                    "timestamp": event.get("timestamp") * 1000,
                    "message": event.get("message"),
                    "ingestionTime": (event.get("timestamp") or ingestion_time) * 1000,
                    "sequenceToken": new_sequence,
                }
                for event in log_events
            ]
            log_buffer.add(warehouse_logs)
        except Exception as e:
            print(f"[Warehouse] Failed to buffer logs: {e}")

    return jsonify(
        {
            "nextSequenceToken": next_token,
            "rejectedLogEventsInfo": {
                "tooOldLogEventEndIndex": [],
                "tooNewLogEventStartIndex": [],
                "matchedLogEventsNotInserted": [],
            },
        }
    )


def get_log_events():
    from service.services.log_store import log_store
    
    if request.method == "POST":
        data = request.get_json(force=True)
        log_group_name = data.get("logGroupName")
        log_stream_name = data.get("logStreamName")
        start_time = data.get("startTime")
        end_time = data.get("endTime")
        limit = data.get("limit", 100)
    else:
        log_group_name = request.args.get("logGroupName")
        log_stream_name = request.args.get("logStreamName")
        start_time = request.args.get("startTime")
        end_time = request.args.get("endTime")
        limit = request.args.get("limit", 100)

    if start_time:
        start_time = int(start_time)
    if end_time:
        end_time = int(end_time)
    if limit:
        limit = int(limit)

    if not log_group_name or not log_stream_name:
        return jsonify(
            {
                "__type": "InvalidParameterException",
                "message": "Missing required parameters",
            }
        ), 400

    events = log_store.get_events(
        log_group_name=log_group_name,
        log_stream_name=log_stream_name,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )

    return jsonify(
        {
            "events": [
                {
                    "timestamp": e["timestamp"],
                    "message": e["message"],
                    "ingestionTime": e["ingestionTime"],
                }
                for e in events
            ],
            "nextForwardToken": "end",
            "nextBackwardToken": "end",
        }
    )


def filter_log_events():
    from service.services.log_store import log_store
    
    data = request.get_json(force=True) or {}
    log_group_name = data.get("logGroupName")
    log_stream_name_prefix = data.get("logStreamNamePrefix")
    filter_pattern = data.get("filterPattern", "")
    start_time = data.get("startTime")
    end_time = data.get("endTime")
    limit = data.get("limit", 100)

    if start_time:
        start_time = int(start_time)
    if end_time:
        end_time = int(end_time)

    if not log_group_name:
        return jsonify(
            {
                "__type": "InvalidParameterException",
                "message": "Missing required parameters",
            }
        ), 400

    events = log_store.filter_events(
        log_group_name=log_group_name,
        log_stream_name_prefix=log_stream_name_prefix,
        filter_pattern=filter_pattern,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )

    return jsonify(
        {
            "events": [
                {
                    "timestamp": e["timestamp"],
                    "message": e["message"],
                    "ingestionTime": e["ingestionTime"],
                }
                for e in events
            ]
        }
    )


def start_query_execution():
    from service.routes.query import execute_query_internal

    data = request.get_json(force=True) or {}

    # Handle both logGroupName (string) and logGroupNames (array from Grafana)
    log_group_names = data.get("logGroupNames")
    if log_group_names and isinstance(log_group_names, list):
        log_group_name = log_group_names[0]
    else:
        log_group_name = data.get("logGroupName")

    query_string = data.get("queryString", "")
    start_time = data.get("startTime", int((time.time() - 3600) * 1000))
    end_time = data.get("endTime", int(time.time() * 1000))

    print(f"[Query] Using log_group_name: {log_group_name}")

    return jsonify(
        execute_query_internal(log_group_name, query_string, start_time, end_time)
    )


def get_query_execution_results():
    from service.routes.query import get_query_results_internal

    data = request.get_json(force=True) or {}
    query_id = data.get("queryId")
    result = get_query_results_internal(query_id)
    if "error" in result or result.get("__type") == "ResourceNotFoundException":
        return jsonify(result), 400
    return jsonify(result)


@app.route("/flush", methods=["POST"])
def flush_buffer():
    if log_buffer:
        count = log_buffer.flush()
        # Wait for flush queue to be empty (with timeout)
        try:
            log_buffer._flush_queue.join()
        except Exception:
            pass
    else:
        count = 0

    # Move hot WAL → cold Iceberg so queries see data with full filter push-down
    warehouse = get_warehouse()
    if warehouse:
        try:
            loki_table = warehouse.config.get("loki", {}).get("table_name", "loki_logs")
            warehouse.flush_wal(warehouse.table_name)
            warehouse.flush_wal(loki_table)
        except Exception as e:
            logger.warning(f"[Flush] WAL flush error: {e}")

    return jsonify({"status": "ok", "flushed": count})


@app.route("/buffer/stats", methods=["GET"])
def buffer_stats():
    if log_buffer:
        return jsonify(log_buffer.stats())
    return jsonify({"error": "buffer not initialized"})


@app.route("/debug/memory", methods=["GET"])
def debug_memory():
    """
    Memory breakdown endpoint — zero overhead when not called.

    Query params:
      top=N        - top N tracemalloc lines (default 20); pass 0 to skip
      trace=1      - start tracemalloc NOW and take snapshot (adds ~50 MB overhead while active)
      trace=stop   - stop tracemalloc and free its memory
      reset=1      - clear tracemalloc traces after reading
    """
    import gc
    import tracemalloc
    import collections
    import platform

    top_n = int(request.args.get("top", 20))
    trace = request.args.get("trace", "")
    reset = request.args.get("reset", "0") == "1"

    # --- 1. OS RSS via /proc (Linux) or resource module -------------------
    rss_bytes  = 0
    hwm_bytes  = 0
    vmsize_bytes = 0
    try:
        with open("/proc/1/status") as f:  # PID 1 = the Flask process inside container
            for line in f:
                k, _, v = line.partition(":")
                v = v.strip()
                if k == "VmRSS":
                    rss_bytes  = int(v.split()[0]) * 1024
                elif k == "VmHWM":       # peak RSS ever reached
                    hwm_bytes  = int(v.split()[0]) * 1024
                elif k == "VmSize":
                    vmsize_bytes = int(v.split()[0]) * 1024
    except FileNotFoundError:
        import resource, platform
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        rss_bytes = rss_kb if platform.system() == "Linux" else rss_kb * 1024

    # --- 2. tracemalloc (opt-in, no overhead unless requested) -----------
    tm_info: dict = {"active": tracemalloc.is_tracing()}
    top_allocators: list = []

    if trace == "1" and not tracemalloc.is_tracing():
        tracemalloc.start(10)  # 10 frames is enough, less overhead than 25
        tm_info["started"] = True

    if trace == "stop" and tracemalloc.is_tracing():
        tracemalloc.stop()
        tm_info["stopped"] = True

    if tracemalloc.is_tracing() and top_n > 0:
        snapshot = tracemalloc.take_snapshot()
        if reset:
            tracemalloc.clear_traces()
        cur, peak = tracemalloc.get_traced_memory()
        tm_info["current_mb"] = round(cur  / 1024 / 1024, 1)
        tm_info["peak_mb"]    = round(peak / 1024 / 1024, 1)

        for s in snapshot.statistics("filename")[:top_n]:
            top_allocators.append({
                "size_kb":  round(s.size / 1024, 1),
                "count":    s.count,
                "location": s.traceback.format()[0].strip(),
            })

    # --- 3. GC object counts (cheap — no size traversal) -----------------
    gc.collect()
    type_counts: dict = collections.Counter()
    for obj in gc.get_objects():
        type_counts[type(obj).__name__] += 1
    top_types = [{"type": t, "count": c} for t, c in type_counts.most_common(20)]

    # --- 4. Application-level memory consumers ---------------------------
    app_stats: dict = {}
    if log_buffer:
        app_stats["log_buffer_items"] = log_buffer.size()
        app_stats["log_buffer_queue"] = log_buffer._flush_queue.qsize()

    try:
        from service.services.log_store import log_store
        all_data = log_store.get_all()
        total_events = sum(len(v.get("events", [])) for v in all_data.values())
        app_stats["log_store_streams"] = len(all_data)
        app_stats["log_store_events"]  = total_events
    except Exception:
        pass

    if warehouse:
        try:
            ws = warehouse.get_stats()
            app_stats["warehouse_wal"]         = ws.get("hot_wal", {})
            app_stats["warehouse_cold"]        = ws.get("cold", {})
            app_stats["warehouse_backpressure"] = ws.get("backpressure", {})
        except Exception:
            pass

    return jsonify({
        "process": {
            "rss_mb":         round(rss_bytes    / 1024 / 1024, 1),
            "peak_rss_mb":    round(hwm_bytes    / 1024 / 1024, 1),
            "virtual_mb":     round(vmsize_bytes / 1024 / 1024, 1),
            "uptime_seconds": round(time.time() - startup_time, 0),
        },
        "tracemalloc": tm_info,
        "top_allocators": top_allocators,
        "top_gc_types":   top_types,
        "app":            app_stats,
    })


@app.route("/warehouse/stats", methods=["GET"])
def warehouse_stats():
    """3-tier health: hot WAL, cold Iceberg, archive S3."""
    warehouse = get_warehouse()
    if not warehouse:
        return jsonify({"error": "warehouse not available"}), 503
    return jsonify(warehouse.get_stats())


def shutdown():
    print("[Server] Shutting down...")
    if log_buffer:
        print(f"[Server] Flushing buffer ({log_buffer.size()} logs)...")
        log_buffer.stop()
        print("[Server] Buffer flushed")


if __name__ == "__main__":
    import atexit

    atexit.register(shutdown)
    app.run(host="0.0.0.0", port=4588, debug=True)
