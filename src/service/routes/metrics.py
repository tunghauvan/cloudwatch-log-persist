import sys
from pathlib import Path
from flask import Blueprint, Response
import time

sys.path.insert(0, str(Path(__file__).parent.parent))

metrics_bp = Blueprint("metrics", __name__)


def get_warehouse():
    from service.server import warehouse

    return warehouse


def get_log_buffer():
    from service.server import log_buffer

    return log_buffer


def get_startup_time():
    from service.server import startup_time

    return startup_time


def get_loki_metrics():
    from service.services.loki_metrics import loki_metrics

    return loki_metrics


def get_warehouse_metrics():
    from service.services.warehouse_metrics import warehouse_metrics

    return warehouse_metrics


def format_prometheus_metric(
    name: str, value, help_text: str = None, metric_type: str = "gauge"
) -> str:
    """Format a single metric in Prometheus exposition format"""
    lines = []
    if help_text:
        lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} {metric_type}")
    lines.append(f"{name} {value}")
    return "\n".join(lines)


@metrics_bp.route("/metrics", methods=["GET"])
def prometheus_metrics():
    """Export metrics in Prometheus exposition format"""
    metrics_lines = []

    # Add application info
    metrics_lines.append(
        format_prometheus_metric(
            "cloudwatch_logs_info",
            1,
            "CloudWatch Log Persist service information",
            "gauge",
        )
    )

    # Uptime metric
    try:
        startup = get_startup_time()
        uptime_seconds = time.time() - startup
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_uptime_seconds",
                int(uptime_seconds),
                "Application uptime in seconds",
                "counter",
            )
        )
    except:
        pass

    # Buffer metrics
    log_buffer = get_log_buffer()
    if log_buffer:
        buffer_stats = log_buffer.stats()

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_buffer_size",
                buffer_stats.get("buffer_size", 0),
                "Current number of logs in buffer",
                "gauge",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_buffer_max_size",
                buffer_stats.get("max_size", 0),
                "Maximum buffer size",
                "gauge",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_buffer_last_flush_age_seconds",
                int(buffer_stats.get("last_flush_age_seconds", 0)),
                "Seconds since last buffer flush",
                "gauge",
            )
        )

        # Error metric - 1 if there's an error, 0 otherwise
        last_error = buffer_stats.get("last_error")
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_buffer_error",
                1 if last_error else 0,
                "Buffer error state (1 if error, 0 if ok)",
                "gauge",
            )
        )
    else:
        # Buffer not initialized
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_buffer_initialized",
                0,
                "Buffer initialization state (1 if initialized, 0 if not)",
                "gauge",
            )
        )

    # Warehouse metrics
    warehouse = get_warehouse()
    if warehouse:
        try:
            wh_stats = warehouse.get_stats()

            if "error" not in wh_stats:
                metrics_lines.append(
                    format_prometheus_metric(
                        "cloudwatch_logs_warehouse_data_files",
                        wh_stats.get("data_files", 0),
                        "Total number of data files in warehouse",
                        "gauge",
                    )
                )

                metrics_lines.append(
                    format_prometheus_metric(
                        "cloudwatch_logs_warehouse_size_bytes",
                        wh_stats.get("total_size_bytes", 0),
                        "Total size of warehouse data in bytes",
                        "gauge",
                    )
                )

                metrics_lines.append(
                    format_prometheus_metric(
                        "cloudwatch_logs_warehouse_size_mb",
                        wh_stats.get("total_size_mb", 0),
                        "Total size of warehouse data in megabytes",
                        "gauge",
                    )
                )

                # Partition count
                partitions = wh_stats.get("partitions", {})
                metrics_lines.append(
                    format_prometheus_metric(
                        "cloudwatch_logs_warehouse_partitions",
                        len(partitions),
                        "Number of partitions in warehouse",
                        "gauge",
                    )
                )

                # Retention and compaction settings
                metrics_lines.append(
                    format_prometheus_metric(
                        "cloudwatch_logs_warehouse_retention_enabled",
                        1 if wh_stats.get("retention_enabled", False) else 0,
                        "Retention policy enabled (1 if enabled, 0 if disabled)",
                        "gauge",
                    )
                )

                metrics_lines.append(
                    format_prometheus_metric(
                        "cloudwatch_logs_warehouse_retention_days",
                        wh_stats.get("retention_days", 0),
                        "Data retention period in days",
                        "gauge",
                    )
                )

                metrics_lines.append(
                    format_prometheus_metric(
                        "cloudwatch_logs_warehouse_compaction_enabled",
                        1 if wh_stats.get("compaction_enabled", False) else 0,
                        "Compaction enabled (1 if enabled, 0 if disabled)",
                        "gauge",
                    )
                )

                metrics_lines.append(
                    format_prometheus_metric(
                        "cloudwatch_logs_warehouse_spark_available",
                        1 if wh_stats.get("spark_available", False) else 0,
                        "Spark availability (1 if available, 0 if not)",
                        "gauge",
                    )
                )
            else:
                # Warehouse error state
                metrics_lines.append(
                    format_prometheus_metric(
                        "cloudwatch_logs_warehouse_error",
                        1,
                        "Warehouse error state (1 if error, 0 if ok)",
                        "gauge",
                    )
                )
        except Exception as e:
            metrics_lines.append(
                format_prometheus_metric(
                    "cloudwatch_logs_warehouse_error",
                    1,
                    "Warehouse error state (1 if error, 0 if ok)",
                    "gauge",
                )
            )
    else:
        # Warehouse not initialized
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_initialized",
                0,
                "Warehouse initialization state (1 if initialized, 0 if not)",
                "gauge",
            )
        )

    # Loki API metrics
    loki = get_loki_metrics()
    if loki:
        loki_stats = loki.get_stats()

        # Push metrics
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_push_requests_total",
                loki_stats.get("push_requests_total", 0),
                "Total number of Loki push requests",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_push_logs_total",
                loki_stats.get("push_logs_total", 0),
                "Total number of logs received via Loki push",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_push_streams_total",
                loki_stats.get("push_streams_total", 0),
                "Total number of streams received via Loki push",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_push_errors_total",
                loki_stats.get("push_errors_total", 0),
                "Total number of Loki push errors",
                "counter",
            )
        )

        # Query metrics
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_query_requests_total",
                loki_stats.get("query_requests_total", 0),
                "Total number of Loki query requests",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_query_errors_total",
                loki_stats.get("query_errors_total", 0),
                "Total number of Loki query errors",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_query_logs_returned_total",
                loki_stats.get("query_logs_returned_total", 0),
                "Total number of logs returned by Loki queries",
                "counter",
            )
        )

        # Query range metrics
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_query_range_requests_total",
                loki_stats.get("query_range_requests_total", 0),
                "Total number of Loki query_range requests",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_query_range_errors_total",
                loki_stats.get("query_range_errors_total", 0),
                "Total number of Loki query_range errors",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_query_range_logs_returned_total",
                loki_stats.get("query_range_logs_returned_total", 0),
                "Total number of logs returned by Loki query_range",
                "counter",
            )
        )

        # Label metrics
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_label_requests_total",
                loki_stats.get("label_requests_total", 0),
                "Total number of Loki label requests",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_label_values_requests_total",
                loki_stats.get("label_values_requests_total", 0),
                "Total number of Loki label values requests",
                "counter",
            )
        )

        # Series metrics
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_series_requests_total",
                loki_stats.get("series_requests_total", 0),
                "Total number of Loki series requests",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_series_errors_total",
                loki_stats.get("series_errors_total", 0),
                "Total number of Loki series request errors",
                "counter",
            )
        )

        # Index metrics
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_index_stats_requests_total",
                loki_stats.get("index_stats_requests_total", 0),
                "Total number of Loki index stats requests",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_index_volume_requests_total",
                loki_stats.get("index_volume_requests_total", 0),
                "Total number of Loki index volume requests",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_loki_index_volume_range_requests_total",
                loki_stats.get("index_volume_range_requests_total", 0),
                "Total number of Loki index volume range requests",
                "counter",
            )
        )

    # Warehouse operation metrics
    wh_metrics = get_warehouse_metrics()
    if wh_metrics:
        wh_stats = wh_metrics.get_stats()

        # Query metrics
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_queries_total",
                wh_stats.get("queries_total", 0),
                "Total number of warehouse queries",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_query_errors_total",
                wh_stats.get("query_errors_total", 0),
                "Total number of warehouse query errors",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_query_avg_duration_seconds",
                wh_stats.get("query_avg_duration_seconds", 0),
                "Average query duration in seconds",
                "gauge",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_query_logs_returned_total",
                wh_stats.get("query_logs_returned_total", 0),
                "Total number of logs returned by warehouse queries",
                "counter",
            )
        )

        # Insert metrics
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_inserts_total",
                wh_stats.get("inserts_total", 0),
                "Total number of warehouse insert operations",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_insert_errors_total",
                wh_stats.get("insert_errors_total", 0),
                "Total number of warehouse insert errors",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_insert_avg_duration_seconds",
                wh_stats.get("insert_avg_duration_seconds", 0),
                "Average insert duration in seconds",
                "gauge",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_insert_logs_total",
                wh_stats.get("insert_logs_total", 0),
                "Total number of logs inserted into warehouse",
                "counter",
            )
        )

        # Maintenance metrics
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_compactions_total",
                wh_stats.get("compactions_total", 0),
                "Total number of compaction runs",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_retention_runs_total",
                wh_stats.get("retention_runs_total", 0),
                "Total number of retention enforcement runs",
                "counter",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_retention_removed_partitions_total",
                wh_stats.get("retention_removed_partitions_total", 0),
                "Total number of partitions removed by retention",
                "counter",
            )
        )

        # Cached stats
        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_log_groups_total",
                wh_stats.get("log_groups_count", 0),
                "Total number of log groups in warehouse",
                "gauge",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_log_streams_total",
                wh_stats.get("log_streams_count", 0),
                "Total number of log streams in warehouse",
                "gauge",
            )
        )

        metrics_lines.append(
            format_prometheus_metric(
                "cloudwatch_logs_warehouse_stats_cache_age_seconds",
                wh_stats.get("stats_cache_age_seconds", 0),
                "Age of cached warehouse stats in seconds",
                "gauge",
            )
        )

    # Add timestamp
    metrics_lines.append(f"\n# Collection timestamp: {int(time.time())}")

    # Join all metrics with newlines
    output = "\n\n".join(metrics_lines)

    return Response(output, mimetype="text/plain; version=0.0.4; charset=utf-8")


@metrics_bp.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint for monitoring"""
    warehouse = get_warehouse()
    log_buffer = get_log_buffer()

    status = {
        "status": "ok",
        "timestamp": int(time.time()),
        "components": {
            "warehouse": "ok" if warehouse else "not_initialized",
            "buffer": "ok" if log_buffer else "not_initialized",
        },
    }

    # Check buffer for errors
    if log_buffer:
        buffer_stats = log_buffer.stats()
        if buffer_stats.get("last_error"):
            status["components"]["buffer"] = "error"
            status["status"] = "degraded"

    # Check warehouse
    if warehouse:
        try:
            wh_stats = warehouse.get_stats()
            if "error" in wh_stats:
                status["components"]["warehouse"] = "error"
                status["status"] = "degraded"
        except:
            status["components"]["warehouse"] = "error"
            status["status"] = "degraded"

    from flask import jsonify

    return jsonify(status), 200 if status["status"] == "ok" else 503
