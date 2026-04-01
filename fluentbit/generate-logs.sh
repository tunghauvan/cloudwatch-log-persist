#!/bin/sh
# Generate Kubernetes-like logs with real timestamps

while true; do
    TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    NS=$(( $(date +%s%N) / 1000 ))
    
    # API Server logs
    echo "[$TS] INFO Application started successfully"
    echo "[$TS] ERROR Connection timeout to database"
    echo "[$TS] INFO Request completed: 200 OK in 45ms"
    echo "[$TS] ERROR Failed to connect to upstream service"
    
    # Worker logs  
    echo "[$TS] WARN High memory usage detected: 85%"
    echo "[$TS] INFO Processing job ID: $RANDOM"
    
    # Cache logs
    echo "[$TS] DEBUG Cache hit ratio: 0.95"
    
    sleep 1
done
