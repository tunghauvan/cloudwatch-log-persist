import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import unittest
import threading
import time
from unittest.mock import Mock, patch

from src.cloudwatch_local_service.services.log_buffer import LogBuffer


class TestLogBuffer(unittest.TestCase):
    def test_buffer_init(self):
        buffer = LogBuffer(max_size=100, flush_interval_seconds=5)
        self.assertEqual(buffer.size(), 0)
        self.assertEqual(buffer.max_size, 100)

    def test_add_increases_size(self):
        buffer = LogBuffer(max_size=100, flush_interval_seconds=60)
        logs = [{"message": f"log {i}"} for i in range(10)]
        buffer.add(logs)
        self.assertEqual(buffer.size(), 10)

    def test_add_multiple_batches(self):
        buffer = LogBuffer(max_size=100, flush_interval_seconds=60)
        buffer.add([{"message": f"log {i}"} for i in range(30)])
        buffer.add([{"message": f"log {i}"} for i in range(30, 60)])
        self.assertEqual(buffer.size(), 60)

    def test_auto_flush_on_max_size(self):
        mock_warehouse = Mock()
        buffer = LogBuffer(max_size=50, flush_interval_seconds=60)
        buffer.set_warehouse(mock_warehouse)

        buffer.add([{"message": f"log {i}"} for i in range(30)])
        self.assertEqual(buffer.size(), 30)
        self.assertEqual(mock_warehouse.insert_logs.call_count, 0)

        buffer.add([{"message": f"log {i}"} for i in range(30, 55)])
        self.assertEqual(mock_warehouse.insert_logs.call_count, 1)
        self.assertEqual(buffer.size(), 0)

    def test_manual_flush(self):
        mock_warehouse = Mock()
        buffer = LogBuffer(max_size=1000, flush_interval_seconds=60)
        buffer.set_warehouse(mock_warehouse)

        buffer.add([{"message": f"log {i}"} for i in range(100)])
        self.assertEqual(buffer.size(), 100)

        flushed = buffer.flush()
        self.assertEqual(flushed, 100)
        self.assertEqual(buffer.size(), 0)
        self.assertEqual(mock_warehouse.insert_logs.call_count, 1)
        mock_warehouse.insert_logs.assert_called_once_with(
            [{"message": f"log {i}"} for i in range(100)]
        )

    def test_flush_empty_buffer(self):
        mock_warehouse = Mock()
        buffer = LogBuffer(max_size=100, flush_interval_seconds=60)
        buffer.set_warehouse(mock_warehouse)

        flushed = buffer.flush()
        self.assertEqual(flushed, 0)
        self.assertEqual(mock_warehouse.insert_logs.call_count, 0)

    def test_flush_on_timeout(self):
        mock_warehouse = Mock()
        buffer = LogBuffer(max_size=10000, flush_interval_seconds=1)
        buffer.set_warehouse(mock_warehouse)
        buffer.start()

        buffer.add([{"message": f"log {i}"} for i in range(50)])
        self.assertEqual(buffer.size(), 50)

        time.sleep(2)
        self.assertEqual(mock_warehouse.insert_logs.call_count, 1)

        buffer.stop()

    def test_stop_flushes_remaining(self):
        mock_warehouse = Mock()
        buffer = LogBuffer(max_size=10000, flush_interval_seconds=60)
        buffer.set_warehouse(mock_warehouse)
        buffer.start()

        buffer.add([{"message": f"log {i}"} for i in range(25)])
        buffer.stop()

        mock_warehouse.insert_logs.assert_called()

    def test_stats(self):
        buffer = LogBuffer(max_size=100, flush_interval_seconds=30)
        buffer.add([{"message": "test"} for _ in range(50)])

        stats = buffer.stats()
        self.assertEqual(stats["buffer_size"], 50)
        self.assertEqual(stats["max_size"], 100)
        self.assertEqual(stats["flush_interval_seconds"], 30)
        self.assertLess(stats["last_flush_age_seconds"], 1)
        self.assertIsNone(stats["last_error"])

    def test_thread_safety(self):
        buffer = LogBuffer(max_size=10000, flush_interval_seconds=60)
        errors = []

        def add_logs():
            try:
                for _ in range(100):
                    buffer.add([{"message": "test"}])
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=add_logs) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0)
        self.assertEqual(buffer.size(), 1000)


class TestLogBufferWithWarehouse(unittest.TestCase):
    def test_warehouse_error_does_not_lose_data(self):
        mock_warehouse = Mock()
        mock_warehouse.insert_logs.side_effect = Exception("S3 error")

        buffer = LogBuffer(max_size=10, flush_interval_seconds=60)
        buffer.set_warehouse(mock_warehouse)

        buffer.add([{"message": "log 1"} for _ in range(15)])

        self.assertEqual(buffer.size(), 15)
        self.assertIsNotNone(buffer.stats()["last_error"])

        mock_warehouse.insert_logs.side_effect = None
        mock_warehouse.insert_logs.return_value = None

        flushed = buffer.flush()
        self.assertEqual(flushed, 15)
        self.assertEqual(buffer.size(), 0)


if __name__ == "__main__":
    unittest.main()
