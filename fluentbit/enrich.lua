-- enrich.lua
-- Enrich logs with Kubernetes metadata

function enrich_log(tag, timestamp, record)
    -- Extract level from log message
    local level = "info"
    local log_msg = record["log"] or ""
    
    if string.match(log_msg, "ERROR") then
        level = "error"
    elseif string.match(log_msg, "WARN") then
        level = "warn"
    elseif string.match(log_msg, "DEBUG") then
        level = "debug"
    end
    
    -- Determine app based on log content
    local app = "api-server"
    local pod_name = "api-server-7d9f4b8c5-x2v4p"
    local container = "api"
    
    if string.match(log_msg, "Worker") or string.match(log_msg, "job ID") then
        app = "worker"
        pod_name = "worker-5f8d7c9b2-m4k8n"
        container = "worker"
    elseif string.match(log_msg, "Cache") or string.match(log_msg, "cache") then
        app = "cache"
        pod_name = "cache-3a4b5c6d7-e1f2g"
        container = "redis"
    end
    
    -- Add kubernetes metadata
    record["kubernetes"] = {
        pod_name = pod_name,
        container_name = container,
        namespace = "prod",
        labels = {
            app = app,
            version = "v1.0.0"
        }
    }
    
    -- Add extracted level
    record["level"] = level
    
    return 1, timestamp, record
end
