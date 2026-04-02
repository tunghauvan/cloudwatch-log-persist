import re


def get_log_group_key(log_group_name, log_stream_name):
    return f"{log_group_name}/{log_stream_name}"


def parse_filter_pattern(filter_pattern, message):
    if not filter_pattern or filter_pattern.strip() == "":
        return True

    if filter_pattern.strip() == "ALL":
        return True

    filter_pattern = filter_pattern.strip()

    if filter_pattern.startswith("[") and filter_pattern.endswith("]"):
        return _parse_json_filter_pattern(filter_pattern, message)

    if " like " in filter_pattern:
        return _parse_like_filter(filter_pattern, message)

    if "!" in filter_pattern or "!=" in filter_pattern:
        return _parse_not_equal_filter(filter_pattern, message)

    return filter_pattern.lower() in message.lower()


def _parse_json_filter_pattern(pattern, message):
    try:
        inner = pattern[1:-1]
        if not inner.strip():
            return True

        parts = _split_filter_parts(inner)

        for part in parts:
            part = part.strip()
            if "=" in part:
                key, value = part.split("=", 1)
                key = key.strip().strip(",")
                value = value.strip().strip(",")
                if f'"{key}"' not in message:
                    return False

        if "msg" in inner or "message" in inner:
            if "msg != " in inner or "message != " in inner:
                match = re.search(r'(msg|message)\s*!=\s*"([^"]+)"', inner)
                if match:
                    exclude_msg = match.group(2)
                    if exclude_msg in message:
                        return False

        return True
    except Exception:
        return pattern.lower() in message.lower()


def _split_filter_parts(s):
    parts = []
    depth = 0
    current = ""
    for c in s:
        if c == "[":
            depth += 1
            current += c
        elif c == "]":
            depth -= 1
            current += c
        elif c == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += c
    if current:
        parts.append(current)
    return parts


def _parse_like_filter(pattern, message):
    match = re.search(r"(\w+)\s+like\s+/([^/]+)/", pattern)
    if match:
        field, regex = match.groups()
        try:
            return bool(re.search(regex, message, re.IGNORECASE))
        except re.error:
            return regex.lower() in message.lower()
    return pattern.lower() in message.lower()


def _parse_not_equal_filter(pattern, message):
    parts = pattern.replace("!=", " !=").split()
    for part in parts:
        if part.startswith("!"):
            exclude = part[1:].strip()
            if exclude.lower() in message.lower():
                return False
    return True


def format_log_event(event, log_group_name, log_stream_name):
    return {
        "timestamp": event.get("timestamp"),
        "message": event.get("message"),
        "ingestionTime": event.get("ingestionTime"),
        "logGroupName": log_group_name,
        "logStreamName": log_stream_name,
    }
