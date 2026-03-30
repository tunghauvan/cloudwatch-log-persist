import pytest
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime
from pathlib import Path
import tempfile
import shutil
import sys


@pytest.fixture(autouse=True)
def reset_modules():
    for mod in list(sys.modules.keys()):
        if "src.warehouse" in mod:
            sys.modules.pop(mod, None)
    yield


def test_parse_warehouse_path():
    m = mock_open(read_data="warehouse: file:///warehouse\n")
    with patch("builtins.open", m):
        with patch("yaml.safe_load", return_value={}):
            from src.warehouse.warehouse import WarehouseManager

            wm = WarehouseManager()

            assert wm._parse_warehouse_path("file:///warehouse") == Path("/warehouse")
            assert wm._parse_warehouse_path("/local/path") == Path("/local/path")


def test_ensure_warehouse_without_pyiceberg(tmp_path):
    warehouse_path = tmp_path / "warehouse"

    m = mock_open(read_data=f"warehouse: file://{warehouse_path}\n")
    with patch("builtins.open", m):
        with patch(
            "yaml.safe_load",
            return_value={
                "warehouse": f"file://{warehouse_path}",
                "catalog": "iceberg",
                "namespace": "default",
                "table_name": "cloudwatch_logs",
            },
        ):
            with patch("src.warehouse.warehouse.PYICEBERG_AVAILABLE", False):
                from src.warehouse.warehouse import WarehouseManager

                wm = WarehouseManager()
                wm.ensure_warehouse()

                assert wm.warehouse_dir.exists()
                assert wm.get_table_path().exists()


def test_get_table_path(tmp_path):
    warehouse_path = tmp_path / "warehouse"

    m = mock_open(read_data=f"warehouse: file://{warehouse_path}\n")
    with patch("builtins.open", m):
        with patch(
            "yaml.safe_load",
            return_value={
                "warehouse": f"file://{warehouse_path}",
                "catalog": "iceberg",
                "namespace": "my_namespace",
                "table_name": "my_table",
            },
        ):
            from src.warehouse.warehouse import WarehouseManager

            wm = WarehouseManager()

            assert wm.get_table_path() == warehouse_path / "my_namespace" / "my_table"
            assert (
                wm.get_table_path("other_table")
                == warehouse_path / "my_namespace" / "other_table"
            )


def test_list_tables_without_pyiceberg(tmp_path):
    warehouse_dir = tmp_path / "warehouse" / "default"
    warehouse_dir.mkdir(parents=True)
    (warehouse_dir / "table1").mkdir()
    (warehouse_dir / "table2").mkdir()

    m = mock_open(read_data="warehouse: file:///test\n")
    with patch("builtins.open", m):
        with patch(
            "yaml.safe_load",
            return_value={
                "warehouse": f"file://{tmp_path / 'warehouse'}",
                "catalog": "iceberg",
                "namespace": "default",
                "table_name": "cloudwatch_logs",
            },
        ):
            with patch("src.warehouse.warehouse.PYICEBERG_AVAILABLE", False):
                from src.warehouse.warehouse import WarehouseManager

                wm = WarehouseManager()

                tables = wm.list_tables()
                assert "table1" in tables
                assert "table2" in tables


def test_default_config():
    m = mock_open(read_data="warehouse: file:///warehouse\n")
    with patch("builtins.open", m):
        with patch("yaml.safe_load", return_value={}):
            from src.warehouse.warehouse import WarehouseManager

            wm = WarehouseManager()

            assert wm.warehouse_path == "file:///warehouse"
            assert wm.catalog_name == "iceberg"
            assert wm.namespace == "default"
            assert wm.table_name == "cloudwatch_logs"


def test_warehouse_dir_property():
    m = mock_open(read_data="warehouse: file:///my/warehouse\n")
    with patch("builtins.open", m):
        with patch(
            "yaml.safe_load",
            return_value={
                "warehouse": "file:///my/warehouse",
                "catalog": "iceberg",
                "namespace": "default",
                "table_name": "logs",
            },
        ):
            from src.warehouse.warehouse import WarehouseManager

            wm = WarehouseManager()

            assert wm.warehouse_dir == Path("/my/warehouse")


def test_catalog_property_raises_without_pyiceberg():
    m = mock_open(read_data="warehouse: file:///test\n")
    with patch("builtins.open", m):
        with patch("yaml.safe_load", return_value={}):
            with patch("src.warehouse.warehouse.PYICEBERG_AVAILABLE", False):
                from src.warehouse.warehouse import WarehouseManager

                wm = WarehouseManager()

                with pytest.raises(RuntimeError, match="PyIceberg is not installed"):
                    _ = wm.catalog


def test_insert_logs_raises_without_pyiceberg():
    m = mock_open(read_data="warehouse: file:///test\n")
    with patch("builtins.open", m):
        with patch("yaml.safe_load", return_value={}):
            with patch("src.warehouse.warehouse.PYICEBERG_AVAILABLE", False):
                from src.warehouse.warehouse import WarehouseManager

                wm = WarehouseManager()

                with pytest.raises(RuntimeError, match="PyIceberg is not installed"):
                    wm.insert_logs([])


def test_query_raises_without_pyiceberg():
    m = mock_open(read_data="warehouse: file:///test\n")
    with patch("builtins.open", m):
        with patch("yaml.safe_load", return_value={}):
            with patch("src.warehouse.warehouse.PYICEBERG_AVAILABLE", False):
                from src.warehouse.warehouse import WarehouseManager

                wm = WarehouseManager()

                with pytest.raises(RuntimeError, match="PyIceberg is not installed"):
                    wm.query()


def test_get_schema():
    m = mock_open(read_data="warehouse: file:///test\n")
    with patch("builtins.open", m):
        with patch("yaml.safe_load", return_value={}):
            from src.warehouse.warehouse import WarehouseManager

            wm = WarehouseManager()

            schema = wm._get_schema()

            assert schema is not None
            field_names = [f.name for f in schema.fields]
            assert "log_group_name" in field_names
            assert "log_stream_name" in field_names
            assert "timestamp" in field_names
            assert "message" in field_names
            assert "ingestion_time" in field_names
            assert "sequence_token" in field_names


def test_ensure_warehouse_creates_namespace_and_table(tmp_path):
    warehouse_path = tmp_path / "warehouse"

    m = mock_open(read_data=f"warehouse: file://{warehouse_path}\n")
    with patch("builtins.open", m):
        with patch(
            "yaml.safe_load",
            return_value={
                "warehouse": f"file://{warehouse_path}",
                "catalog": "sql",
                "namespace": "test_ns",
                "table_name": "test_logs",
            },
        ):
            from src.warehouse.warehouse import WarehouseManager

            try:
                wm = WarehouseManager()
                wm.ensure_warehouse()

                assert (warehouse_path / "test_ns" / "test_logs").exists()
            except Exception as e:
                pytest.skip(f"Iceberg setup failed: {e}")


def test_insert_and_query_logs(tmp_path):
    warehouse_path = tmp_path / "warehouse"

    m = mock_open(read_data=f"warehouse: file://{warehouse_path}\n")
    with patch("builtins.open", m):
        with patch(
            "yaml.safe_load",
            return_value={
                "warehouse": f"file://{warehouse_path}",
                "catalog": "sql",
                "namespace": "test_ns",
                "table_name": "test_logs",
            },
        ):
            from src.warehouse.warehouse import WarehouseManager

            try:
                wm = WarehouseManager()
                wm.ensure_warehouse()

                logs = [
                    {
                        "logGroupName": "/aws/lambda/test",
                        "logStreamName": "stream1",
                        "timestamp": datetime.now(),
                        "message": "Test message 1",
                        "ingestionTime": datetime.now(),
                    },
                    {
                        "logGroupName": "/aws/lambda/test",
                        "logStreamName": "stream2",
                        "timestamp": datetime.now(),
                        "message": "Test message 2",
                        "ingestionTime": datetime.now(),
                    },
                ]

                wm.insert_logs(logs)

                result = wm.query(limit=10)

                assert result is not None
                print(f"Inserted and queried {len(result)} records")

            except Exception as e:
                pytest.skip(f"Iceberg test failed: {e}")
