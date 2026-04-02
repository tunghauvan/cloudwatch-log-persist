import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Blueprint, jsonify, current_app

store_bp = Blueprint("store", __name__)


@store_bp.route("/warehouse/stats", methods=["GET"])
def get_warehouse_stats():
    try:
        from service.server import warehouse

        if not warehouse:
            return jsonify({"error": "Warehouse not initialized"}), 500
        stats = warehouse.get_stats()
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@store_bp.route("/warehouse/compact", methods=["POST"])
def trigger_compaction():
    try:
        from service.server import warehouse

        if not warehouse:
            return jsonify({"error": "Warehouse not initialized"}), 500
        result = warehouse.compact()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@store_bp.route("/warehouse/retention", methods=["POST"])
def trigger_retention():
    try:
        from service.server import warehouse

        if not warehouse:
            return jsonify({"error": "Warehouse not initialized"}), 500
        result = warehouse.enforce_retention()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
