import logging
import os
import sys

def setup_logging():
    """Consolidated logging configuration for the service."""
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    
    # Define log format
    # Example: 2023-10-27 10:00:00,000 - name - LEVEL - Business-friendly message
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Reduce noise from 3rd party libraries if needed
    logging.getLogger("werkzeug").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    
    return logging.getLogger("service")

# Initialize logger for this module context if needed, 
# but usually we call setup_logging() in server.py
logger = logging.getLogger("service")
