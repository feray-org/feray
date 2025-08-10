
# conceptual  API
# llms did support
# lets refiew/refine (consider pseudocode)

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Generic, Hashable, Iterable, List, NewType, Optional, TypeVar

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Config,
    ConfigurableResource,
    Definitions,
    job,
    multi_asset,
    op,
    get_dagster_logger
)

# ==================================================================================
# PART 1: THE STANDALONE CORE PLATFORM LIBRARY
# ==================================================================================

# --------------------------------------------------------------------------
# Core data structures, enums, and storage contracts.
# --------------------------------------------------------------------------

S = TypeVar("S", bound=Hashable)
RowId = NewType("RowId", str)
CodeVersion = NewType("CodeVersion", str)
DataVersion = NewType("DataVersion", str)
MerkleHash = NewType("MerkleHash", str)

class ValidationStatus(Enum):
    PENDING = "pending"
    HUMAN_VALIDATED = "human_validated"
    LOCKED = "locked"

@dataclass(frozen=True)
class Provenance:
    code_version: CodeVersion
    data_version: DataVersion
    parent_hash: Optional[MerkleHash]
    validation_status: ValidationStatus = ValidationStatus.PENDING

    @property
    def merkle_hash(self) -> MerkleHash:
        payload = json.dumps({
            "cv": self.code_version, "dv": self.data_version,
            "parent": self.parent_hash or "", "val_status": self.validation_status.value
        }, sort_keys=True).encode()
        return MerkleHash(hashlib.sha256(payload).hexdigest())

    @property
    def is_locked(self) -> bool:
        return self.validation_status == ValidationStatus.LOCKED

@dataclass(frozen=True)
class FeatureDefinition(Generic[S]):
    asset_key: S
    compute_fn: Callable[[Dict[S, pl.DataFrame]], pl.DataFrame]
    output_features: List[str]
    entity_keys: List[str]
    dependencies: List[S] = field(default_factory=list)
    code_version: str = "1.0"

class FeatureStore(ABC, Generic[S]):
    @abstractmethod
    def write_version(self, *, asset_key: S, df: pl.DataFrame, data_version: DataVersion) -> None: ...
    @abstractmethod
    def read_version(self, *, asset_key: S, data_version: DataVersion, filter_on_keys: Optional[Dict[str, Any]] = None) -> pl.DataFrame: ...
    @abstractmethod
    def delete_ids(self, *, asset_key: S, row_ids: Iterable[RowId]) -> None: ...
    @abstractmethod
    def prune_data_versions(self, *, asset_key: S, versions_to_delete: List[DataVersion]) -> None: ...

class MetadataStore(ABC, Generic[S]):
    @abstractmethod
    def write_provenance(self, *, asset_key: S, provenance: Provenance) -> None: ...
    @abstractmethod
    def latest_provenance(self, *, asset_key: S) -> Provenance | None: ...
    @abstractmethod
    def all_provenance(self, *, asset_key: S) -> List[Provenance]: ...
    @abstractmethod
    def update_provenance_status(self, *, asset_key: S, data_version: DataVersion, status: ValidationStatus) -> bool: ...
    # --- NEW: Abstract methods for deletion and pruning ---
    @abstractmethod
    def prune_provenance(self, *, asset_key: S, versions_to_delete: List[DataVersion]) -> None: ...

# --------------------------------------------------------------------------
# Core orchestrator-agnostic service classes.
# --------------------------------------------------------------------------

class FeatureRegistry(Generic[S]):
    def __init__(self, definitions: List[FeatureDefinition[S]]):
        self.definitions = {d.asset_key: d for d in definitions}
        self.dag = {d.asset_key: set(d.dependencies) for d in self.definitions.values()}
        self.reverse_dag = self._build_reverse_dag()
        self.feature_to_asset_map = {feat: d.asset_key for d in self.definitions.values() for feat in d.output_features}

    def resolve_execution_order(self, target_asset_keys: List[S]) -> List[S]:
        order, visited = [], set()
        def visit(key):
            if key in visited: return
            visited.add(key)
            for dep in self.dag.get(key, []): visit(dep)
            order.append(key)
        for key in target_asset_keys: visit(key)
        return order

    def _build_reverse_dag(self) -> Dict[S, set[S]]:
        reverse_dag = {key: set() for key in self.dag}
        for key, deps in self.dag.items():
            for dep in deps:
                reverse_dag.setdefault(dep, set()).add(key)
        return reverse_dag

class FeatureService(Generic[S]):
    def __init__(self, *, registry: FeatureRegistry[S], feature_store: FeatureStore[S], metadata_store: MetadataStore[S]):
        self.registry = registry
        self._fs = feature_store
        self._ms = metadata_store
        self.logger = get_dagster_logger() if "dagster" in globals() else print

    def materialize(self, asset_keys: Optional[List[S]] = None, force_recompute: bool = False):
        target_assets = asset_keys or list(self.registry.definitions.keys())
        execution_order = self.registry.resolve_execution_order(target_assets)
        self.logger(f"\n--- Starting Standalone Batch Materialization (Force={force_recompute}) ---")
        for asset_key in execution_order:
            self._materialize_one(asset_key, force_recompute=force_recompute)

    def delete_entity_data(self, *, start_asset: S, entity_key_values: Dict[str, Any], cascade: bool = True):
        self.logger(f"\n--- Deleting data for entity {entity_key_values} starting from '{start_asset}' ---")
        row_id_to_delete = RowId(hashlib.sha256(json.dumps(entity_key_values, sort_keys=True).encode()).hexdigest())
        assets_to_process = [start_asset]
        if cascade:
            q, visited = [start_asset], {start_asset}
            while q:
                curr = q.pop(0)
                for downstream_dep in self.registry.reverse_dag.get(curr, []):
                    if downstream_dep not in visited:
                        visited.add(downstream_dep); assets_to_process.append(downstream_dep); q.append(downstream_dep)
        self.logger(f"Deletion will apply to assets: {assets_to_process}")
        for asset_key in assets_to_process:
            self._fs.delete_ids(asset_key=asset_key, row_ids=[row_id_to_delete])

    def prune_asset_versions(self, *, asset_key: S, keep_last_n: int | None = None, max_age: timedelta | None = None):
        self.logger(f"\n--- Pruning versions for asset '{asset_key}' ---")
        all_prov = self._ms.all_provenance(asset_key=asset_key)
        if not all_prov: return
        
        # Sort by timestamp if available, otherwise just use list order. For demo, we assume order.
        all_prov.sort(key=lambda p: p.data_version, reverse=True) # A bit of a hack for demo
        
        versions_to_delete = set()
        if keep_last_n is not None:
            versions_to_delete.update(p.data_version for p in all_prov[keep_last_n:])
            
        # A real implementation would store timestamps with provenance.
        # This part is conceptual for the demo.
        if max_age is not None:
             self.logger(f"WARNING: Pruning by max_age is not fully implemented in this demo.")

        if versions_to_delete:
            versions_list = list(versions_to_delete)
            self.logger(f"  - Pruning {len(versions_list)} versions from stores.")
            # Prune from both stores
            self._fs.prune_data_versions(asset_key=asset_key, versions_to_delete=versions_list)
            self._ms.prune_provenance(asset_key=asset_key, versions_to_delete=versions_list)

    def _materialize_one(self, asset_key: S, force_recompute: bool) -> pl.DataFrame:
        definition = self.registry.definitions[asset_key]
        latest_stored = self._ms.latest_provenance(asset_key=asset_key)

        if not force_recompute and latest_stored and latest_stored.is_locked:
            self.logger(f"[{asset_key}] SKIPPED: Version is LOCKED.")
            return self._fs.read_version(asset_key, latest_stored.data_version)

        cv = CodeVersion(definition.code_version)
        parent_hash = self._get_parent_merkle_hash(definition.dependencies)
        if not force_recompute and latest_stored and latest_stored.code_version == cv and latest_stored.parent_hash == parent_hash:
            self.logger(f"[{asset_key}] SKIPPED: Cache hit.")
            return self._fs.read_version(asset_key, latest_stored.data_version)

        self.logger(f"[{asset_key}] EXECUTING: Cache miss, computing new version.")
        upstream_data = {
            dep_key: self._fs.read_version(dep_key, self._ms.latest_provenance(dep_key).data_version)
            for dep_key in definition.dependencies
        }
        df = definition.compute_fn(upstream_data)
        dv = DataVersion(str(df.hash_rows().sum()))

        self._fs.write_version(asset_key=asset_key, df=df, data_version=dv)
        new_prov = Provenance(code_version=cv, data_version=dv, parent_hash=parent_hash)
        self._ms.write_provenance(asset_key=asset_key, provenance=new_prov)
        self.logger(f"[{asset_key}] Wrote new version {dv[:8]}")
        return df

    def _get_parent_merkle_hash(self, dependencies: List[S]) -> MerkleHash:
        hashes = [self._ms.latest_provenance(dep).merkle_hash for dep in sorted(dependencies) if self._ms.latest_provenance(dep)]
        return MerkleHash(hashlib.sha256("".join(hashes).encode()).hexdigest())

# --------------------------------------------------------------------------
# Concrete store implementations.
# --------------------------------------------------------------------------
class LocalParquetFeatureStore(FeatureStore[str]):
    def __init__(self, base_dir: Path): self.base_dir = base_dir
    def write_version(self, *, asset_key: str, df: pl.DataFrame, data_version: DataVersion):
        path = self.base_dir / asset_key / data_version; path.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path / "data.parquet")
    def read_version(self, *, asset_key: str, data_version: DataVersion, filter_on_keys: Optional[Dict[str, Any]] = None):
        lazy_frame = pl.scan_parquet(self.base_dir / asset_key / data_version / "data.parquet")
        if filter_on_keys:
            lazy_frame = lazy_frame.filter(pl.all_horizontal(pl.col(k) == v for k, v in filter_on_keys.items()))
        return lazy_frame.collect()
    def delete_ids(self, *, asset_key: str, row_ids: Iterable[RowId]) -> None:
        asset_dir = self.base_dir / asset_key
        if not asset_dir.exists(): return
        for version_dir in asset_dir.iterdir():
            if version_dir.is_dir():
                data_file = version_dir / "data.parquet"
                if data_file.exists():
                    df = pl.read_parquet(data_file)
                    if "__row_id" in df.columns:
                        filtered_df = df.filter(~pl.col("__row_id").is_in(list(row_ids)))
                        if len(filtered_df) < len(df):
                            filtered_df.write_parquet(data_file)
    def prune_data_versions(self, *, asset_key: str, versions_to_delete: List[DataVersion]) -> None:
        asset_dir = self.base_dir / asset_key
        if not asset_dir.exists(): return
        for version in versions_to_delete:
            version_path = asset_dir / version
            if version_path.exists():
                shutil.rmtree(version_path)

class InMemoryMetadataStore(MetadataStore[str]):
    def __init__(self):
        self._db: Dict[str, List[Provenance]] = {}
        self._lookup: Dict[tuple[str, DataVersion], Provenance] = {}
    def write_provenance(self, *, asset_key: str, provenance: Provenance):
        if (asset_key, provenance.data_version) not in self._lookup:
            self._db.setdefault(asset_key, []).append(provenance)
            self._lookup[(asset_key, provenance.data_version)] = provenance
    def latest_provenance(self, *, asset_key: str) -> Provenance | None:
        return self._db.get(asset_key, [])[-1] if self._db.get(asset_key) else None
    def all_provenance(self, *, asset_key: str) -> List[Provenance]: return self._db.get(asset_key, []).copy()
    def update_provenance_status(self, *, asset_key: str, data_version: DataVersion, status: ValidationStatus) -> bool:
        prov = self._lookup.get((asset_key, data_version))
        if not prov: return False
        new_prov = Provenance(prov.code_version, prov.data_version, prov.parent_hash, status)
        history = self._db[asset_key]
        history[history.index(prov)] = new_prov
        self._lookup[(asset_key, data_version)] = new_prov
        return True
    def prune_provenance(self, *, asset_key: str, versions_to_delete: List[DataVersion]) -> None:
        if asset_key in self._db:
            self._db[asset_key] = [p for p in self._db[asset_key] if p.data_version not in versions_to_delete]
            for version in versions_to_delete:
                if (asset_key, version) in self._lookup:
                    del self._lookup[(asset_key, version)]

# ==================================================================================
# PART 2: THE DAGSTER INTEGRATION LAYER
# ==================================================================================

class FeaturePlatformResource(ConfigurableResource):
    base_dir: str = tempfile.gettempdir()
    def get_service(self, registry: FeatureRegistry) -> FeatureService:
        fs = LocalParquetFeatureStore(base_dir=Path(self.base_dir))
        ms = InMemoryMetadataStore()
        return FeatureService(registry=registry, feature_store=fs, metadata_store=ms)

def add_row_id(df: pl.DataFrame, entity_keys: List[str]) -> pl.DataFrame:
    return df.with_columns(__row_id=pl.concat_str([pl.col(k).cast(pl.Utf8) for k in entity_keys]).hash())

feature_definitions = [
    FeatureDefinition("users", lambda _: add_row_id(pl.DataFrame({"user_id": [1, 2, 3]}), ["user_id"]), ["user_id"], ["user_id"]),
    FeatureDefinition("locations", lambda up: add_row_id(up["users"].with_columns(country=pl.lit("USA")), ["user_id"]), ["country"], ["user_id"], ["users"]),
]
feature_registry = FeatureRegistry(definitions=feature_definitions)

def build_feature_assets(registry: FeatureRegistry, resource_key: str) -> list:
    assets = []
    for asset_key, definition in registry.definitions.items():
        @multi_asset(
            name=definition.asset_key,
            group_name="feature_platform",
            required_resource_keys={resource_key},
            outs={asset_key: AssetOut()},
            internal_asset_deps={AssetKey(definition.asset_key): {AssetKey(dep) for dep in definition.dependencies}}
        )
        def _dynamic_asset(context: AssetExecutionContext, **kwargs):
            service = context.resources[resource_key].get_service(registry)
            service._materialize_one(asset_key=context.asset_key.to_user_string(), force_recompute=False)
        assets.append(_dynamic_asset)
    return assets

dagster_feature_assets = build_feature_assets(feature_registry, "feature_platform")

@op(required_resource_keys={"feature_platform"})
def prune_all_op(context):
    service = context.resources.feature_platform.get_service(feature_registry)
    for asset_key in service.registry.definitions:
        service.prune_asset_versions(asset_key=asset_key, keep_last_n=1) # Keep only the latest version

class DeleteEntityConfig(Config):
    start_asset: str = "users"
    entity_key_values: Dict[str, Any] = {"user_id": 1}

@op(required_resource_keys={"feature_platform"})
def delete_entity_op(context, config: DeleteEntityConfig):
    service = context.resources.feature_platform.get_service(feature_registry)
    service.delete_entity_data(start_asset=config.start_asset, entity_key_values=config.entity_key_values)

@job
def maintenance_job():
    prune_all_op()
    delete_entity_op()

defs = Definitions(
    assets=dagster_feature_assets,
    resources={"feature_platform": FeaturePlatformResource()},
    jobs=[maintenance_job],
)

# To run: `dagster dev -f feature_platform_complete_final.py`