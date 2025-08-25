from typing import Any

import polars as pl
from dagster import Config, job, op
from dagster_feray.resources import build_feature_assets
from feray.data_structures import FeatureDefinition
from feray.service import FeatureRegistry

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


dagster_feature_assets = build_feature_assets(
    registry=feature_registry, resource_key="feature_platform"
)


@op(required_resource_keys={"feature_platform"})
def prune_all_op(context):
    """A Dagster op to prune old versions of all feature assets."""
    service = context.resources.feature_platform.get_service(feature_registry)
    context.log.info("Starting pruning job for all defined feature assets.")
    for asset_key in service.registry.definitions:
        service.prune_asset_versions(asset_key=asset_key, keep_last_n=1)
    context.log.info("Pruning job complete.")


class DeleteEntityConfig(Config):
    start_asset: str = "users"
    entity_key_values: dict[str, Any] = {"user_id": 1}


@op(required_resource_keys={"feature_platform"})
def delete_entity_op(context, config: DeleteEntityConfig):
    """A Dagster op to delete data for a specific entity."""
    service = context.resources.feature_platform.get_service(feature_registry)
    context.log.info(
        "Starting entity deletion for %s from asset '%s'",
        config.entity_key_values,
        config.start_asset,
    )
    service.delete_entity_data(
        start_asset=config.start_asset, entity_key_values=config.entity_key_values
    )
    context.log.info("Entity deletion complete.")


@job(
    config={
        "ops": {
            "delete_entity_op": {
                "config": {"start_asset": "users", "entity_key_values": {"user_id": 2}}
            }
        }
    }
)
def maintenance_job():
    prune_all_op()
    delete_entity_op()
