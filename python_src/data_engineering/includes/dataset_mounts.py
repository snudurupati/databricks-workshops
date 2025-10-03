"""Python port of `Includes/Dataset-Mounts.scala`.

Notes:
- For production, prefer Unity Catalog External Locations/Volumes over ad-hoc mounts.
- This module avoids embedding secrets. It expects admins to configure access.
"""

from __future__ import annotations

from typing import Dict, Tuple

from pyspark.sql import SparkSession


def _get_username(spark: SparkSession) -> str:
    return spark.sparkContext.getLocalProperty("databricks.user") or spark.sql(
        "SELECT current_user()"
    ).collect()[0][0]


def mount(
    source: str,
    mount_point: str,
    extra_configs: Dict[str, str] | None = None,
) -> None:
    from dbruntime.dbutils import DBUtils  # type: ignore

    dbutils = DBUtils(SparkSession.getActiveSession()._jsparkSession.sparkContext())
    try:
        if extra_configs:
            dbutils.fs.mount(source=source, mount_point=mount_point, extra_configs=extra_configs)
        else:
            dbutils.fs.mount(source=source, mount_point=mount_point)
    except Exception as e:  # noqa: BLE001
        print(f"*** ERROR: Unable to mount {mount_point}: {e}")


def is_mounted(mount_point: str) -> bool:
    from dbruntime.dbutils import DBUtils  # type: ignore

    dbutils = DBUtils(SparkSession.getActiveSession()._jsparkSession.sparkContext())
    return any(m.mountPoint == mount_point for m in dbutils.fs.mounts())


def auto_mount_training_datasets(spark: SparkSession) -> None:
    """Attempt to mount training datasets to `/mnt/training` if not present.

    This function mirrors the Scala `autoMount()` behavior at a high-level
    without embedding credentials. If mounts are disabled or not configured,
    it will no-op with a message.
    """

    mount_dir = "/mnt/training"
    if is_mounted(mount_dir):
        print(f"Already mounted {mount_dir}")
        return

    # In secure environments, rely on pre-configured IAM roles or UC locations.
    # We deliberately do not embed keys or SAS tokens here.
    print("Skipping auto-mount: no embedded credentials; use UC Volumes or admin-managed mounts.")


