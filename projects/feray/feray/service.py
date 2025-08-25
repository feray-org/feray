from __future__ import annotations

import hashlib
import logging
from datetime import timedelta
from typing import Any, Callable, Generic

# Import all necessary types from the foundational data_structures module
from .data_structures import (
    CodeVersion,
    DataFrameType,
    DataVersion,
    FeatureDefinition,
    MerkleHash,
    Provenance,
    RowId,
    S,
    StorageId,
    ValidationStatus,
)
from .protocols import FeatureStore, MetadataStore, ProvenanceHasher


class FeatureRegistry(Generic[S, DataFrameType]):
    """Holds a collection of FeatureDefinitions and can resolve their dependency graph."""

    def __init__(self, definitions: list[FeatureDefinition[S, DataFrameType]]):
        self.definitions = {d.asset_key: d for d in definitions}
        self.dag = {d.asset_key: set(d.dependencies) for d in self.definitions.values()}
        self.reverse_dag = self._build_reverse_dag()

    def resolve_execution_order(self, target_asset_keys: list[S]) -> list[S]:
        """Performs a topological sort to get the correct materialization order."""
        order: list[S] = []
        visited: set[S] = set()

        def visit(key: S):
            if key in visited:
                return
            visited.add(key)
            for dep in sorted(self.dag.get(key, []), key=str):
                visit(dep)
            order.append(key)

        for key in sorted(target_asset_keys, key=str):
            visit(key)
        return order

    def _build_reverse_dag(self) -> dict[S, set[S]]:
        reverse_dag: dict[S, set[S]] = {key: set() for key in self.dag}
        for key, deps in self.dag.items():
            for dep in deps:
                reverse_dag.setdefault(dep, set()).add(key)
        return reverse_dag


class FeatureService(Generic[S, DataFrameType]):
    """
    The primary orchestration class. It is orchestrator-agnostic (works outside Dagster)
    and implementation-agnostic (works with any compliant stores).
    """

    def __init__(
        self,
        *,
        registry: FeatureRegistry[S, DataFrameType],
        feature_store: FeatureStore[S, DataFrameType],
        metadata_store: MetadataStore[S],
        hasher: ProvenanceHasher,
        data_version_fn: Callable[[DataFrameType], DataVersion],
        row_id_fn: Callable[[DataFrameType, list[str]], DataFrameType],
    ):
        self.registry = registry
        self._fs = feature_store
        self._ms = metadata_store
        self.hasher = hasher
        self.data_version_fn = data_version_fn
        self.row_id_fn = row_id_fn
        self.logger = logging.getLogger(__name__)

    def materialize(
        self, asset_keys: list[S] | None = None, force_recompute: bool = False
    ):
        """Materializes a set of assets, respecting dependencies and caching."""
        target_assets = asset_keys or list(self.registry.definitions.keys())
        execution_order = self.registry.resolve_execution_order(target_assets)
        self.logger.info(
            "\n--- Starting Batch Materialization (Force=%s) ---", force_recompute
        )
        for asset_key in execution_order:
            self._materialize_one(asset_key, force_recompute=force_recompute)

    def delete_entity_data(
        self, *, start_asset: S, entity_key_values: dict[str, Any], cascade: bool = True
    ):
        """Deletes data for a given entity, with optional cascading to downstream assets."""
        self.logger.info(
            "\n--- Deleting data for entity %s starting from '%s' ---",
            entity_key_values,
            start_asset,
        )

        # --- THE FIX: Create a canonical hash string that matches the one in polars_add_row_id_fn ---
        definition = self.registry.definitions[start_asset]
        try:
            # Build the string by joining the entity key values in their defined order.
            canonical_string = "".join(
                [str(entity_key_values[k]) for k in definition.entity_keys]
            )
        except KeyError as e:
            raise ValueError(
                f"Missing entity key {e} in provided values for asset '{start_asset}'."
            ) from e

        row_id_to_delete = RowId(hashlib.sha256(canonical_string.encode()).hexdigest())

        assets_to_process = self._resolve_cascading_assets(start_asset, cascade)
        self.logger.info("Deletion will apply to assets: %s", assets_to_process)

        for asset_key in assets_to_process:
            self._fs.delete_ids(asset_key=asset_key, row_ids=[row_id_to_delete])

    def prune_asset_versions(
        self,
        *,
        asset_key: S,
        keep_last_n: int | None = None,
        max_age: timedelta | None = None,
    ):
        """Prunes old data and metadata versions for a given asset based on policies."""
        self.logger.info("\n--- Pruning versions for asset '%s' ---", asset_key)
        all_prov = self._ms.all_provenance(asset_key=asset_key)
        if not all_prov:
            return

        provs_to_delete: list[Provenance] = []
        if keep_last_n is not None and len(all_prov) > keep_last_n:
            provs_to_delete = all_prov[:-keep_last_n]

        if max_age is not None:
            self.logger.warning("WARNING: Pruning by max_age is not fully implemented.")

        if provs_to_delete:
            storage_ids_to_delete = [StorageId(self.hasher(p)) for p in provs_to_delete]
            self.logger.info(
                "  - Pruning %d versions from stores.", len(storage_ids_to_delete)
            )
            self._fs.prune_data_versions(
                asset_key=asset_key, storage_ids_to_delete=storage_ids_to_delete
            )
            self._ms.prune_provenance(
                asset_key=asset_key, storage_ids_to_delete=storage_ids_to_delete
            )

    def _materialize_one(self, asset_key: S, force_recompute: bool) -> DataFrameType:
        """The core logic for materializing a single asset."""
        definition = self.registry.definitions[asset_key]
        latest_stored_prov = self._ms.latest_provenance(asset_key=asset_key)

        latest_stored_storage_id: StorageId | None = (
            StorageId(self.hasher(latest_stored_prov)) if latest_stored_prov else None
        )

        if not force_recompute and latest_stored_prov and latest_stored_prov.is_locked:
            assert latest_stored_storage_id is not None
            self.logger.info("[%s] SKIPPED: Version is LOCKED.", asset_key)
            return self._fs.read_version(
                asset_key=asset_key, storage_id=latest_stored_storage_id
            )

        desired_prov = self._calculate_desired_provenance(asset_key, latest_stored_prov)

        if (
            not force_recompute
            and latest_stored_prov
            and self.hasher(latest_stored_prov) == self.hasher(desired_prov)
        ):
            assert latest_stored_storage_id is not None
            self.logger.info(
                "[%s] SKIPPED: Cache hit (provenance unchanged).", asset_key
            )
            return self._fs.read_version(
                asset_key=asset_key, storage_id=latest_stored_storage_id
            )

        self.logger.info(
            "[%s] EXECUTING: Cache miss, computing new version.", asset_key
        )

        upstream_data: dict[S, DataFrameType] = {}
        for dep_key in sorted(definition.dependencies, key=str):
            latest_dep_prov = self._ms.latest_provenance(asset_key=dep_key)
            if latest_dep_prov is None:
                raise RuntimeError(
                    f"Integrity error: Dependency '{dep_key}' has no versioned data."
                )

            dep_storage_id = StorageId(self.hasher(latest_dep_prov))
            upstream_data[dep_key] = self._fs.read_version(
                asset_key=dep_key, storage_id=dep_storage_id
            )

        computed_df = definition.compute_fn(upstream_data)
        df_with_ids = self.row_id_fn(computed_df, definition.entity_keys)
        new_data_version = self.data_version_fn(df_with_ids)

        new_prov = Provenance(
            code_version=CodeVersion(definition.code_version),
            data_version=new_data_version,
            parent_hash=self._get_parent_merkle_hash(definition.dependencies),
            validation_status=ValidationStatus.PENDING,
        )

        new_storage_id = StorageId(self.hasher(new_prov))

        self._fs.write_version(
            asset_key=asset_key, df=df_with_ids, storage_id=new_storage_id
        )
        self._ms.write_provenance(asset_key=asset_key, provenance=new_prov)

        self.logger.info(
            "[%s] Wrote new version (data_hash=%s) to storage (storage_id=%s)",
            asset_key,
            str(new_data_version)[:8],
            new_storage_id[:8],
        )
        return df_with_ids

    def _calculate_desired_provenance(
        self, asset_key: S, latest_stored: Provenance | None
    ) -> Provenance:
        """Calculates the provenance an asset *would* have if it were computed now."""
        definition = self.registry.definitions[asset_key]
        return Provenance(
            CodeVersion(definition.code_version),
            latest_stored.data_version if latest_stored else DataVersion(""),
            self._get_parent_merkle_hash(definition.dependencies),
            latest_stored.validation_status
            if latest_stored
            else ValidationStatus.PENDING,
        )

    def _get_parent_merkle_hash(self, dependencies: list[S]) -> MerkleHash:
        """Computes a stable hash of all parent provenances."""
        hashes: list[MerkleHash] = []
        for dep_key in sorted(dependencies, key=str):
            latest_prov = self._ms.latest_provenance(asset_key=dep_key)
            if latest_prov is not None:
                hashes.append(self.hasher(latest_prov))

        if not hashes:
            return MerkleHash(hashlib.sha256(b"").hexdigest())

        return MerkleHash(hashlib.sha256("".join(sorted(hashes)).encode()).hexdigest())

    def _resolve_cascading_assets(self, start_asset: S, cascade: bool) -> list[S]:
        """Finds all downstream assets from a starting point if cascade is true."""
        if not cascade:
            return [start_asset]

        assets_to_process: list[S] = []
        q, visited = [start_asset], {start_asset}
        while q:
            curr = q.pop(0)
            assets_to_process.append(curr)
            for downstream_dep in self.registry.reverse_dag.get(curr, set()):
                if downstream_dep not in visited:
                    visited.add(downstream_dep)
                    q.append(downstream_dep)
        return assets_to_process
