"""
ALB access log parser.

ALB log format (space-delimited, quoted strings for some fields):
  type time elb client:port target:port
  request_processing_time target_processing_time response_processing_time
  elb_status_code target_status_code received_bytes sent_bytes
  "request" "user-agent" ssl_cipher ssl_protocol
  target_group_arn trace_id domain_name chosen_cert_arn
  matched_rule_priority request_creation_time "actions_executed"
  "redirect_url" "error_reason" "target:port_list" "target_status_code_list"
  classification classification_reason

Reference:
  https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html
"""

import gzip
import logging
import shlex
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger("service.alb_parser")

# Field indices after shlex.split()
_F_TYPE = 0
_F_TIME = 1
_F_ELB = 2
_F_CLIENT = 3
_F_TARGET = 4
_F_REQ_PROC_TIME = 5
_F_TGT_PROC_TIME = 6
_F_RESP_PROC_TIME = 7
_F_ELB_STATUS = 8
_F_TGT_STATUS = 9
_F_RECV_BYTES = 10
_F_SENT_BYTES = 11
_F_REQUEST = 12
_F_UA = 13
_F_SSL_CIPHER = 14
_F_SSL_PROTOCOL = 15
_F_TGT_GROUP_ARN = 16
_F_TRACE_ID = 17
_F_DOMAIN = 18
_F_CERT_ARN = 19
_F_RULE_PRIORITY = 20
_F_REQ_CREATION_TIME = 21
_F_ACTIONS = 22
_F_REDIRECT_URL = 23
_F_ERROR_REASON = 24


def _field(fields: List[str], idx: int) -> str:
    return fields[idx] if idx < len(fields) else "-"


def _or_none(val: str) -> Optional[str]:
    return None if (not val or val == "-") else val


def _parse_endpoint(endpoint: str):
    """Split 'ip:port' into (ip, port). Handles IPv6 addresses like '[::1]:443'."""
    if not endpoint or endpoint == "-":
        return None, None
    if endpoint.startswith("["):
        close = endpoint.rfind("]")
        ip = endpoint[1:close] if close > 0 else None
        port_part = endpoint[close + 1:] if close > 0 else ""
        port = int(port_part.lstrip(":")) if port_part.startswith(":") else None
    elif ":" in endpoint:
        parts = endpoint.rsplit(":", 1)
        ip = parts[0] or None
        try:
            port = int(parts[1])
        except (ValueError, IndexError):
            port = None
    else:
        ip = endpoint or None
        port = None
    return ip, port


def _parse_float(val: str) -> Optional[float]:
    if not val or val == "-":
        return None
    try:
        f = float(val)
        return None if f < 0 else f  # -1 means connection error / not available
    except ValueError:
        return None


def _parse_int(val: str) -> Optional[int]:
    if not val or val == "-":
        return None
    try:
        return int(val)
    except ValueError:
        return None


def _parse_time(val: str) -> Optional[datetime]:
    """Parse ISO-8601 like '2026-04-06T10:30:00.123456Z' → UTC datetime."""
    if not val or val == "-":
        return None
    try:
        val = val.rstrip("Z")
        return datetime.fromisoformat(val).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_request(request_str: str):
    """Split 'METHOD /url HTTP/1.x' into (method, url, protocol, path)."""
    method = url = protocol = path = None
    if not request_str or request_str == "-":
        return method, url, protocol, path
    parts = request_str.split(" ", 2)
    if len(parts) == 3:
        method, url, protocol = parts
    elif len(parts) == 2:
        method, url = parts
    if url and url != "-":
        try:
            parsed = urlparse(url)
            path = parsed.path or url
        except Exception:
            path = url.split("?")[0] if "?" in url else url
    return method, _or_none(url), _or_none(protocol), _or_none(path)


def parse_alb_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single ALB access log line.

    Returns a dict ready for insertion, or None if the line should be skipped
    (blank lines, comment lines starting with '#', or malformed lines).
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    try:
        fields = shlex.split(line)
    except ValueError:
        logger.debug(f"[ALBParser] shlex.split failed on line: {line[:80]}")
        return None
    if len(fields) < 12:
        return None

    client_ip, client_port = _parse_endpoint(_field(fields, _F_CLIENT))
    target_ip, target_port = _parse_endpoint(_field(fields, _F_TARGET))
    method, url, protocol, path = _parse_request(_field(fields, _F_REQUEST))
    now = datetime.now(tz=timezone.utc)

    return {
        "time":                     _parse_time(_field(fields, _F_TIME)),
        "type":                     _or_none(_field(fields, _F_TYPE)),
        "elb":                      _or_none(_field(fields, _F_ELB)),
        "client_ip":                client_ip,
        "client_port":              client_port,
        "target_ip":                target_ip,
        "target_port":              target_port,
        "request_processing_time":  _parse_float(_field(fields, _F_REQ_PROC_TIME)),
        "target_processing_time":   _parse_float(_field(fields, _F_TGT_PROC_TIME)),
        "response_processing_time": _parse_float(_field(fields, _F_RESP_PROC_TIME)),
        "elb_status_code":          _parse_int(_field(fields, _F_ELB_STATUS)),
        "target_status_code":       _parse_int(_field(fields, _F_TGT_STATUS)),
        "received_bytes":           _parse_int(_field(fields, _F_RECV_BYTES)),
        "sent_bytes":               _parse_int(_field(fields, _F_SENT_BYTES)),
        "request_method":           _or_none(method),
        "request_url":              url,
        "request_path":             path,
        "request_protocol":         protocol,
        "user_agent":               _or_none(_field(fields, _F_UA)),
        "ssl_cipher":               _or_none(_field(fields, _F_SSL_CIPHER)),
        "ssl_protocol":             _or_none(_field(fields, _F_SSL_PROTOCOL)),
        "domain_name":              _or_none(_field(fields, _F_DOMAIN)),
        "actions_executed":         _or_none(_field(fields, _F_ACTIONS)),
        "redirect_url":             _or_none(_field(fields, _F_REDIRECT_URL)),
        "error_reason":             _or_none(_field(fields, _F_ERROR_REASON)),
        "trace_id":                 _or_none(_field(fields, _F_TRACE_ID)),
        "ingestion_time":           now,
    }


def parse_alb_content(content: bytes) -> List[Dict[str, Any]]:
    """
    Parse the raw content of an ALB log file (gzip-compressed or plain text).
    Returns a list of parsed row dicts, skipping unparseable lines.
    """
    try:
        text = gzip.decompress(content).decode("utf-8", errors="replace")
    except (gzip.BadGzipFile, OSError):
        text = content.decode("utf-8", errors="replace")

    results = []
    for line in text.splitlines():
        row = parse_alb_line(line)
        if row is not None:
            results.append(row)
    return results
