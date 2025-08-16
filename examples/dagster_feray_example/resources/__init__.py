import os
import tempfile
from pathlib import Path

import dagster as dg
import polars as pl
from dagster_ray import LocalRay
from feray.protocols import FeatureStore, MetadataStore
from feray.service import FeatureRegistry, FeatureService
from feray_polars.dummy.in_memory_metadata import InMemoryMetadataStore
from feray_polars.dummy.parquet_store import LocalParquetFeatureStore
from feray_polars.utils import (
    polars_add_row_id_fn,
    polars_data_version_fn,
    polars_provenance_hasher,
)

from dagster_feray_example.resources.lazy_local_ray import (
    PipesRayJobClientLazyLocalResource,
)


class PolarsFeaturePlatformResource(dg.ConfigurableResource):
    """
    Dagster resource to provide access to the core feature platform service.
    This resource is configured with concrete store implementations.
    """

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


local_ray = LocalRay(
    ray_init_options={
        "ignore_reinit_error": True,
        "dashboard_host": "127.0.0.1",
        "dashboard_port": 8265,
    }
)

RESOURCES_LOCAL = {
    "ray_cluster": local_ray,
    "pipes_ray_job_client": PipesRayJobClientLazyLocalResource(ray_cluster=local_ray),
    "feature_platform": PolarsFeaturePlatformResource(
        feature_store=LocalParquetFeatureStore(
            base_dir=Path(tempfile.gettempdir()) / "dagster_feature_store"
        ),
        metadata_store=InMemoryMetadataStore(hasher=polars_provenance_hasher),
    ),
}

resource_defs_by_deployment_name = {
    "dev": RESOURCES_LOCAL,
}


def get_dagster_deployment_environment(
    deployment_key: str = "DAGSTER_DEPLOYMENT", default_value="dev"
):
    deplyoment = os.environ.get(deployment_key, default_value)
    dg.get_dagster_logger().debug("dagster deployment environment: %s", deplyoment)
    return deplyoment


def get_resources_for_deployment(log_env: bool = True):
    deployment_name = get_dagster_deployment_environment()
    resources = resource_defs_by_deployment_name[deployment_name]
    if log_env:
        dg.get_dagster_logger().info(f"Using deployment of: {deployment_name}")

    return resources
