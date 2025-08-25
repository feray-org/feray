from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    Any,
    Generic,
    Iterable,
    Protocol,
    Union,
)

# Import all necessary types from the foundational data_structures module
from .data_structures import (
    DataFrameType,
    DataVersion,
    MerkleHash,
    Provenance,
    RowId,
    S,
    StorageId,  # <-- The newly added and crucial type
)


class FeatureStore(ABC, Generic[S, DataFrameType]):
    """
    Abstract protocol for physical storage of feature data.
    All methods operate on the generic DataFrameType.
    """

    @abstractmethod
    def write_version(
        self, *, asset_key: S, df: DataFrameType, storage_id: StorageId
    ) -> None:
        """
        Writes a new, versioned dataframe to a location identified by its unique storage_id.
        """
        ...

    @abstractmethod
    def read_version(self, *, asset_key: S, storage_id: StorageId) -> DataFrameType:
        """
        Reads a specific version of a dataframe from storage using its unique storage_id.
        """
        ...

    @abstractmethod
    def read_subsample(
        self, *, asset_key: S, storage_id: StorageId, fraction: float
    ) -> DataFrameType:
        """
        Reads a random subsample of a versioned dataframe using its unique storage_id.
        """
        ...

    @abstractmethod
    def read_for_keys(
        self,
        *,
        asset_key: S,
        storage_id: StorageId,
        entity_keys: list[str],
        key_values: Union[dict[str, Any], list[dict[str, Any]]],
    ) -> DataFrameType:
        """
        Reads data for specific entity keys from a version identified by its unique storage_id.
        """
        ...

    @abstractmethod
    def delete_ids(self, *, asset_key: S, row_ids: Iterable[RowId]) -> None:
        """
        Deletes specific rows (identified by RowId) across all physical versions of an asset.
        """
        ...

    @abstractmethod
    def prune_data_versions(
        self, *, asset_key: S, storage_ids_to_delete: list[StorageId]
    ) -> None:
        """
        Deletes entire physical data artifacts corresponding to the given storage_ids.
        """
        ...


class ProvenanceHasher(Protocol):
    """A protocol for a function that can hash a Provenance object to a MerkleHash."""

    def __call__(self, prov: Provenance) -> MerkleHash: ...


class MetadataStore(ABC, Generic[S]):
    """Abstract protocol for storing provenance and metadata information."""

    @abstractmethod
    def __init__(self, *, hasher: ProvenanceHasher) -> None:
        """
        Initializes the metadata store, requiring a hasher for pruning operations.
        """
        ...

    @abstractmethod
    def write_provenance(self, *, asset_key: S, provenance: Provenance) -> None:
        """Writes a new provenance record to the asset's history."""
        ...

    @abstractmethod
    def latest_provenance(self, *, asset_key: S) -> Provenance | None:
        """Returns the most recently created provenance object for an asset."""
        ...

    @abstractmethod
    def all_provenance(self, *, asset_key: S) -> list[Provenance]:
        """Returns the entire chronological history of an asset's provenance."""
        ...

    @abstractmethod
    def prune_provenance(
        self, *, asset_key: S, storage_ids_to_delete: list[StorageId]
    ) -> None:
        """
        Deletes provenance records whose hash matches one of the given storage_ids.
        """
        ...
