from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Iterable, Union

import polars as pl

from feray.data_structures import DataFrameType, RowId, S, StorageId
from feray.protocols import FeatureStore


class LocalParquetFeatureStore(FeatureStore[str, pl.DataFrame]):
    """A concrete FeatureStore implementation using Polars and a local parquet file system."""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        print(f"Initialized LocalParquetFeatureStore at: {self.base_dir}")

    def write_version(self, *, asset_key: str, df: pl.DataFrame, storage_id: StorageId):
        """
        Writes a version to a directory named after its unique storage_id.
        """
        path = self.base_dir / asset_key / storage_id
        path.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path / "data.parquet")

    def read_version(self, *, asset_key: str, storage_id: StorageId) -> pl.DataFrame:
        """Reads a version using its unique storage_id."""
        return pl.read_parquet(self.base_dir / asset_key / storage_id / "data.parquet")

    def read_subsample(
        self, *, asset_key: str, storage_id: StorageId, fraction: float
    ) -> pl.DataFrame:
        """Reads a subsample from a version identified by its unique storage_id."""
        file_path = self.base_dir / asset_key / storage_id / "data.parquet"
        return pl.scan_parquet(file_path).sample(fraction=fraction).collect()

    def read_for_keys(
        self,
        *,
        asset_key: str,
        storage_id: StorageId,
        entity_keys: list[str],
        key_values: Union[dict[str, Any], list[dict[str, Any]]],
    ) -> pl.DataFrame:
        """Reads data for specific keys from a version identified by its unique storage_id."""
        file_path = self.base_dir / asset_key / storage_id / "data.parquet"

        keys_to_find = key_values if isinstance(key_values, list) else [key_values]
        if not keys_to_find:
            return pl.DataFrame()

        keys_df = pl.DataFrame(keys_to_find)
        main_df = pl.read_parquet(file_path)
        return main_df.join(keys_df, on=entity_keys, how="semi")

    def delete_ids(self, *, asset_key: str, row_ids: Iterable[RowId]) -> None:
        """Deletes rows across all physical versions (all storage_id directories)."""
        asset_dir = self.base_dir / asset_key
        if not asset_dir.exists():
            return
        row_id_set = set(row_ids)
        if not row_id_set:
            return

        for version_dir in asset_dir.iterdir():
            if (
                version_dir.is_dir()
                and (data_file := version_dir / "data.parquet").exists()
            ):
                df = pl.read_parquet(data_file)
                if "__row_id" in df.columns:
                    filtered_df = df.filter(~pl.col("__row_id").is_in(row_id_set))
                    if len(filtered_df) < len(df):
                        filtered_df.write_parquet(data_file)

    def prune_data_versions(
        self, *, asset_key: str, storage_ids_to_delete: list[StorageId]
    ) -> None:
        """Prunes entire directories corresponding to the given storage_ids."""
        asset_dir = self.base_dir / asset_key
        if not asset_dir.exists():
            return
        for storage_id in storage_ids_to_delete:
            if (version_path := asset_dir / storage_id).exists():
                shutil.rmtree(version_path)
