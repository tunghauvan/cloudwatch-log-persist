import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("service.warehouse")

try:
    from pyiceberg.catalog import load_catalog

    PYICEBERG_AVAILABLE = True
except ImportError:
    PYICEBERG_AVAILABLE = False


class MigrationMixin:
    """Table migration operations."""

    def migrate_to_timestamp_partitioning(self, table_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Migrate table from ingestion_time partitioning to timestamp partitioning.
        This improves query performance because PyIceberg can prune partitions based on timestamp filters.

        Process:
        1. Create temp table with new partition spec (source_id=3 for timestamp)
        2. Scan all data from original table → insert into temp table
        3. Drop original table
        4. Rename temp table to original name

        Returns migration status dict.
        """
        if not PYICEBERG_AVAILABLE:
            return {"status": "skipped", "reason": "PyIceberg not available"}

        target_table = table_name or self.table_name
        table_id = f"{self.namespace}.{target_table}"
        temp_table_id = f"{self.namespace}.{target_table}_v2_migration"

        logger.info(f"[Migration] Starting timestamp partitioning migration for {table_id}")

        try:
            # 1. Check if original table exists and is using old partition spec
            orig_table = self.catalog.load_table(table_id)
            orig_spec = orig_table.spec()

            # Check if already migrated (partition field source_id = 3 = timestamp)
            for pf in orig_spec.fields:
                if pf.source_id == 3:  # timestamp
                    logger.info(f"[Migration] Table {table_id} already using timestamp partitioning")
                    return {"status": "skipped", "reason": "already_migrated"}

            logger.info(f"[Migration] Original table has {orig_table.scan().to_arrow().num_rows} rows")

            # 2. Create temp table with new partition spec
            try:
                self.catalog.load_table(temp_table_id)
                logger.warning(f"[Migration] Temp table {temp_table_id} exists, dropping it first")
                self.catalog.drop_table(temp_table_id)
            except Exception:
                pass  # Doesn't exist yet

            temp_table = self.catalog.create_table(
                temp_table_id,
                schema=self._get_schema(),
                partition_spec=self._get_partition_spec_v2(),
            )
            logger.info(f"[Migration] Created temp table {temp_table_id} with timestamp partitioning")

            # 3. Copy data: scan original → insert into temp
            orig_data = orig_table.scan().to_arrow()
            if orig_data.num_rows > 0:
                temp_table.append(orig_data)
                logger.info(f"[Migration] Copied {orig_data.num_rows} rows to temp table")
            else:
                logger.info(f"[Migration] Original table is empty")

            # 4. Drop original and rename temp
            self.catalog.drop_table(table_id)
            logger.info(f"[Migration] Dropped original table {table_id}")

            # Rename via SQL if available, otherwise drop+recreate with data
            try:
                # PyIceberg doesn't have rename_table, so we manually do drop+recreate
                # Get temp table location
                temp_location = temp_table.location()

                # Create new table with original name at temp location
                # Actually, we can't just move files. Let's do it properly:
                # Drop temp, recreate original with new spec, copy data back
                self.catalog.drop_table(temp_table_id)

                # Recreate original table with new partition spec
                new_table = self.catalog.create_table(
                    table_id,
                    schema=self._get_schema(),
                    partition_spec=self._get_partition_spec_v2(),
                )
                logger.info(f"[Migration] Recreated {table_id} with timestamp partitioning")

                # Re-insert data into new table
                if orig_data.num_rows > 0:
                    new_table.append(orig_data)
                    logger.info(f"[Migration] Re-inserted {orig_data.num_rows} rows into migrated table")

            except Exception as rename_err:
                logger.error(f"[Migration] Rename/recreate failed: {rename_err}")
                # Cleanup: drop temp table if it still exists
                try:
                    self.catalog.drop_table(temp_table_id)
                except Exception:
                    pass
                raise

            # 5. Verify migration
            migrated_table = self.catalog.load_table(table_id)
            migrated_spec = migrated_table.spec()
            logger.info(f"[Migration] Migration complete. New spec: {migrated_spec}")

            return {
                "status": "success",
                "message": f"Migrated {table_id} to timestamp partitioning",
                "rows_migrated": orig_data.num_rows if orig_data else 0,
            }

        except Exception as e:
            logger.error(f"[Migration] Failed: {type(e).__name__}: {str(e)[:200]}")
            return {
                "status": "error",
                "message": str(e),
            }
