from feray.data_structures import DataVersion, Provenance, S, StorageId
from feray.protocols import MetadataStore, ProvenanceHasher


class InMemoryMetadataStore(MetadataStore[str]):
    """A simple, non-persistent, in-memory metadata store for demonstration."""

    def __init__(self, *, hasher: ProvenanceHasher):
        """
        Initializes the store, requiring the hasher to perform pruning operations.
        """
        self._db: dict[str, list[Provenance]] = {}
        # The lookup is for fast reads of a specific version, not for uniqueness checks.
        self._lookup: dict[tuple[str, DataVersion], Provenance] = {}
        # Store the hasher to calculate StorageIds internally.
        self.hasher = hasher

    def write_provenance(self, *, asset_key: str, provenance: Provenance):
        """
        Writes a new provenance record to the asset's history.
        """
        # Always append to the list to record the history chronologically.
        self._db.setdefault(asset_key, []).append(provenance)

        # Update the lookup table for fast reads by data version.
        self._lookup[(asset_key, provenance.data_version)] = provenance

    def latest_provenance(self, *, asset_key: str) -> Provenance | None:
        """Returns the most recently written provenance for an asset."""
        return self._db.get(asset_key, [])[-1] if self._db.get(asset_key) else None

    def all_provenance(self, *, asset_key: str) -> list[Provenance]:
        """Returns a copy of the entire history for an asset."""
        return self._db.get(asset_key, []).copy()

    def prune_provenance(
        self, *, asset_key: str, storage_ids_to_delete: list[StorageId]
    ) -> None:
        """
        Removes provenance records whose hash matches one of the given storage_ids.
        """
        if asset_key not in self._db:
            return

        ids_to_delete_set = set(storage_ids_to_delete)

        provs_to_keep: list[Provenance] = []
        provs_deleted: list[Provenance] = []

        # Partition the existing provenances into ones to keep and ones to delete
        for p in self._db.get(asset_key, []):
            if self.hasher(p) in ids_to_delete_set:
                provs_deleted.append(p)
            else:
                provs_to_keep.append(p)

        # Update the main database with only the provenances we want to keep
        self._db[asset_key] = provs_to_keep

        # Surgically remove the deleted items from the lookup cache
        for p in provs_deleted:
            lookup_key = (asset_key, p.data_version)
            # Only delete from lookup if the object matches exactly, to avoid
            # accidentally deleting a newer record that has the same data_version.
            if self._lookup.get(lookup_key) == p:
                del self._lookup[lookup_key]
