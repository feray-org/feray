from __future__ import annotations

import polars as pl
import polars_hash as plh
from feray.data_structures import (
    DataVersion,
    MerkleHash,
    Provenance,
)


def polars_provenance_hasher(prov: Provenance) -> MerkleHash:
    """A concrete ProvenanceHasher using Polars and polars-hash for stable hashing."""
    df = pl.DataFrame(
        {
            "cv": [prov.code_version],
            "dv": [prov.data_version],
            "parent": [prov.parent_hash or ""],
            "val_status": [prov.validation_status.value],
        }
    )
    hashed_expr = pl.concat_str(pl.all()).chash.sha256()
    return MerkleHash(df.select(hashed_expr).item())


def polars_data_version_fn(df: pl.DataFrame) -> DataVersion:
    """Calculates a robust hash of the dataframe's contents."""
    # The hash_rows().sum() method is the most reliable and deterministic
    # way to get a single hash representing the content of the entire dataframe.
    return DataVersion(str(df.hash_rows().sum()))


def polars_add_row_id_fn(df: pl.DataFrame, entity_keys: list[str]) -> pl.DataFrame:
    """
    Generates a stable, string-based __row_id for each row based on its entity keys
    using the SHA256 algorithm.
    """
    if not entity_keys:
        raise ValueError("entity_keys must be provided to generate a row ID.")

    return df.with_columns(
        __row_id=pl.concat_str(
            [pl.col(k).cast(pl.Utf8) for k in entity_keys]
        ).chash.sha256()
    )
