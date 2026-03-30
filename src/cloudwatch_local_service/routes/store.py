import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Blueprint, jsonify

from cloudwatch_local_service.services.log_store import log_store
from cloudwatch_local_service.services.file_storage import FileStorage

LOGS_DIR = Path(os.getenv("LOGS_DIR", "./logs"))
LOGS_DIR.mkdir(parents=True, exist_ok=True)
file_storage = FileStorage(LOGS_DIR)

store_bp = Blueprint("store", __name__)


@store_bp.route("/store/all", methods=["GET"])
def get_all_stored_logs():
    return jsonify(log_store.get_all())


@store_bp.route("/store/files", methods=["GET"])
def get_file_stored_logs():
    try:
        result = file_storage.get_all_logs_from_files()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@store_bp.route("/store/files/export", methods=["GET"])
def export_logs_as_file():
    return get_file_stored_logs()
