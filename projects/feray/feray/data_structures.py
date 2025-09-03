from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Hashable,
    List,
    NewType,
    Optional,
    TypeVar,
)

DataFrameType = TypeVar("DataFrameType")
S = TypeVar("S", bound=Hashable)

RowId = NewType("RowId", str)
CodeVersion = NewType("CodeVersion", str)
DataVersion = NewType("DataVersion", str)
StorageId = NewType("StorageId", str)
MerkleHash = NewType("MerkleHash", str)


class ValidationStatus(Enum):
    PENDING = "pending"
    HUMAN_VALIDATED = "human_validated"
    LOCKED = "locked"


@dataclass(frozen=True)
class Provenance:
    """Stores the metadata for a single, immutable version of a feature asset."""

    code_version: CodeVersion
    data_version: DataVersion
    parent_hash: Optional[MerkleHash]
    validation_status: ValidationStatus = ValidationStatus.PENDING

    @property
    def is_locked(self) -> bool:
        return self.validation_status == ValidationStatus.LOCKED


@dataclass(frozen=True)
class FeatureDefinition(Generic[S, DataFrameType]):
    """
    A declarative definition of a feature asset.
    It is generic over the asset key type `S` and the dataframe type `DataFrameType`.
    """

    asset_key: S
    compute_fn: Callable[[Dict[S, DataFrameType]], DataFrameType]
    output_features: List[str]
    entity_keys: List[str]
    dependencies: List[S] = field(default_factory=list)
    code_version: str = "1.0"
