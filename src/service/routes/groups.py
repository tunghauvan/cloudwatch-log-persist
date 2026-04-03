import sys
from pathlib import Path
import os

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from flask import Blueprint, request, jsonify

from service.services.log_store import log_store

groups_bp = Blueprint("groups", __name__)


@groups_bp.route("/DescribeLogGroups", methods=["GET", "POST"])
def describe_log_groups():
    groups = log_store.get_all_log_groups()
    return jsonify({"logGroups": list(groups.values())})


@groups_bp.route("/log-groups", methods=["GET", "POST"])
def describe_log_groups_alt():
    return describe_log_groups()
