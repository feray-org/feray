import dataclasses
import tempfile
from pathlib import Path

# --- Assumed imports (based on your original file) ---
import polars as pl
from feray.data_structures import FeatureDefinition, StorageId  # <-- Import StorageId
from feray_polars.dummy.in_memory_metadata import InMemoryMetadataStore
from feray_polars.dummy.parquet_store import LocalParquetFeatureStore
from feray_polars.utils import (
    polars_add_row_id_fn,
    polars_data_version_fn,
    polars_provenance_hasher,
)
from feray.service import FeatureRegistry, FeatureService
from loguru import logger as logging

# --- Define features and registry within the script ---
users_def = FeatureDefinition(
    asset_key="users",
    compute_fn=lambda _: pl.DataFrame({"user_id": [1, 2, 3]}),
    output_features=["user_id"],
    entity_keys=["user_id"],
)

locations_def = FeatureDefinition(
    asset_key="locations",
    compute_fn=lambda up: up["users"].with_columns(country=pl.lit("USA")),
    output_features=["country"],
    entity_keys=["user_id"],
    dependencies=["users"],
)

feature_registry = FeatureRegistry(definitions=[users_def, locations_def])


if __name__ == "__main__":
    print("\n" + "=" * 80 + "\n--- RUNNING STANDALONE DEMONSTRATION ---\n" + "=" * 80)

    with tempfile.TemporaryDirectory() as tmpdir:
        fs = LocalParquetFeatureStore(base_dir=Path(tmpdir))
        ms = InMemoryMetadataStore(hasher=polars_provenance_hasher)
        service = FeatureService(
            registry=feature_registry,
            feature_store=fs,
            metadata_store=ms,
            hasher=polars_provenance_hasher,
            data_version_fn=polars_data_version_fn,
            row_id_fn=polars_add_row_id_fn,
        )

        print("\n--- SCENARIO 1: Create multiple versions ---")
        service.materialize()
        old_loc_def = feature_registry.definitions["locations"]
        loc_def_as_dict = dataclasses.asdict(old_loc_def)
        loc_def_as_dict["code_version"] = "1.1"
        feature_registry.definitions["locations"] = FeatureDefinition(**loc_def_as_dict)
        service.materialize()

        all_loc_prov = ms.all_provenance(asset_key="locations")
        print(f"\nTotal versions for 'locations' before prune: {len(all_loc_prov)}")
        assert len(all_loc_prov) == 2
        assert len(list((Path(tmpdir) / "locations").iterdir())) == 2

        print("\n--- SCENARIO 2: Prune versions, keeping only the latest one ---")
        service.prune_asset_versions(asset_key="locations", keep_last_n=1)
        print(
            "Total versions for 'locations' after prune: "
            f"{len(ms.all_provenance(asset_key='locations'))}"
        )
        assert len(ms.all_provenance(asset_key="locations")) == 1
        assert len(list((Path(tmpdir) / "locations").iterdir())) == 1

        print("\n--- SCENARIO 3 & 4: Demonstrate reads and deletes ---")
        latest_users_prov = ms.latest_provenance(asset_key="users")
        if latest_users_prov:
            # --- THE FIX: Explicitly cast the MerkleHash to a StorageId ---
            latest_users_storage_id = StorageId(service.hasher(latest_users_prov))

            user_1_data = fs.read_for_keys(
                asset_key="users",
                storage_id=latest_users_storage_id,
                entity_keys=["user_id"],
                key_values={"user_id": 1},
            )
            print(f"\nRead data for user_id=1:\n{user_1_data}")
            assert len(user_1_data) == 1

            df_before = fs.read_version(
                asset_key="users", storage_id=latest_users_storage_id
            )
            print(f"\nUser count before deletion: {len(df_before)}")
            assert len(df_before) == 3

            service.delete_entity_data(
                start_asset="users", entity_key_values={"user_id": 2}, cascade=True
            )

            users_prov_after_delete = ms.latest_provenance(asset_key="users")
            locs_prov_after_delete = ms.latest_provenance(asset_key="locations")

            if users_prov_after_delete and locs_prov_after_delete:
                # --- THE FIX: Apply the same cast here ---
                users_storage_id_after = StorageId(
                    service.hasher(users_prov_after_delete)
                )
                locs_storage_id_after = StorageId(
                    service.hasher(locs_prov_after_delete)
                )

                df_after_users = fs.read_version(
                    asset_key="users", storage_id=users_storage_id_after
                )
                df_after_locs = fs.read_version(
                    asset_key="locations", storage_id=locs_storage_id_after
                )
                print(f"\nUser count after deleting user 2: {len(df_after_users)}")
                assert (
                    len(df_after_users) == 2
                    and 2 not in df_after_users["user_id"].to_list()
                )
                assert (
                    len(df_after_locs) == 2
                    and 2 not in df_after_locs["user_id"].to_list()
                )
                print("Deletion correctly cascaded to 'locations' asset.")
            else:
                logging.error("Could not find provenance after deletion.")
        else:
            logging.error("Could not find latest provenance for 'users' asset.")

    print("\n" + "=" * 80 + "\n--- DEMONSTRATION COMPLETE ---\n" + "=" * 80)
