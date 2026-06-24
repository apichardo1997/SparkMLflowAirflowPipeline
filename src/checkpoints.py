"""Idempotency helpers so retries skip already-completed work."""
from __future__ import annotations

import hashlib
from pathlib import Path

from pyspark.sql import DataFrame


def zone_is_fresh(path: str | Path) -> bool:
    """True if a Spark write completed at path (drops a _SUCCESS file)."""
    return (Path(path) / "_SUCCESS").exists()


def data_fingerprint(df: DataFrame) -> str:
    """Stable hash of row count + schema. Identifies the training input version."""
    signature = f"{df.count()}|{df.schema.simpleString()}"
    return hashlib.sha256(signature.encode("utf-8")).hexdigest()[:16]


def find_trained_run(client, experiment_id: str, fingerprint: str):
    """Finished MLflow run for this data fingerprint, or None."""
    runs = client.search_runs(
        [experiment_id],
        filter_string=(
            f"tags.data_fingerprint = '{fingerprint}' "
            "and attributes.status = 'FINISHED'"
        ),
        max_results=1,
    )
    return runs[0] if runs else None
