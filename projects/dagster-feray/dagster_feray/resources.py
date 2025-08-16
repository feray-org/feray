import tempfile
import polars as pl
from pathlib import Path
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
    get_dagster_logger,
)

# --- Import from our new, separated modules ---
# In a real project, these would be proper package imports
# from platform_api.service import FeatureRegistry, FeatureService
# from platform_api.data_structures import FeatureDefinition
# from polars_implementation.stores import LocalParquetFeatureStore
# from polars_implementation.metadata import InMemoryMetadataStore
# from polars_implementation.utils import (
#     polars_provenance_hasher, polars_data_version_fn, polars_add_row_id_fn
# )


class FeaturePlatformResource(ConfigurableResource):
    """Dagster resource to provide access to the feature platform service."""

    # These are now typed to the abstract protocols
    feature_store: FeatureStore[str, pl.DataFrame]
    metadata_store: MetadataStore[str]

    def get_service(
        self, registry: FeatureRegistry[str, pl.DataFrame]
    ) -> FeatureService[str, pl.DataFrame]:
        """Constructs and returns the main FeatureService."""
        return FeatureService(
            registry=registry,
            feature_store=self.feature_store,
            metadata_store=self.metadata_store,
            hasher=polars_provenance_hasher,
            data_version_fn=polars_data_version_fn,
            row_id_fn=polars_add_row_id_fn,
        )


# --- Feature Definitions (This part remains largely the same) ---
# NOTE: The compute_fn is necessarily aware of the concrete DataFrameType (Polars).
# This is the "binding point" where abstract definitions meet concrete computation.

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


# --- Asset and Op Factories (This part remains largely the same) ---


def build_feature_assets(registry: FeatureRegistry, resource_key: str) -> list:
    assets = []
    for asset_key_str, definition in registry.definitions.items():

        def _create_compute_fn(def_closure: FeatureDefinition):
            # This decorator uses the asset key from the closure
            @multi_asset(
                name=def_closure.asset_key,
                group_name="feature_platform",
                required_resource_keys={resource_key},
                outs={def_closure.asset_key: AssetOut()},
                internal_asset_deps={
                    AssetKey(def_closure.asset_key): {
                        AssetKey(dep) for dep in def_closure.dependencies
                    }
                },
            )
            def _dynamic_asset(context: AssetExecutionContext, **kwargs):
                service = context.resources[resource_key].get_service(registry)
                # The service handles all logic, the asset just invokes it.
                df_result = service._materialize_one(
                    asset_key=context.asset_key.to_user_string(), force_recompute=False
                )
                yield df_result

            return _dynamic_asset

        assets.append(_create_compute_fn(definition))
    return assets


dagster_feature_assets = build_feature_assets(feature_registry, "feature_platform")


@op(required_resource_keys={"feature_platform"})
def prune_all_op(context):
    service = context.resources.feature_platform.get_service(feature_registry)
    for asset_key in service.registry.definitions:
        service.prune_asset_versions(asset_key=asset_key, keep_last_n=1)


class DeleteEntityConfig(Config):
    start_asset: str = "users"
    entity_key_values: Dict[str, Any] = {"user_id": 1}


@op(required_resource_keys={"feature_platform"})
def delete_entity_op(context, config: DeleteEntityConfig):
    service = context.resources.feature_platform.get_service(feature_registry)
    service.delete_entity_data(
        start_asset=config.start_asset, entity_key_values=config.entity_key_values
    )


@job
def maintenance_job():
    prune_all_op()
    delete_entity_op()


# --- Main Dagster Definitions object ---

defs = Definitions(
    assets=dagster_feature_assets,
    resources={
        "feature_platform": FeaturePlatformResource(
            feature_store=LocalParquetFeatureStore(
                base_dir=Path(tempfile.gettempdir()) / "dagster_feature_store"
            ),
            metadata_store=InMemoryMetadataStore(),
        )
    },
    jobs=[maintenance_job],
)
