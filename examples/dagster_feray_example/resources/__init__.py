import os

import dagster as dg
from dagster_ray import LocalRay

from dagster_feray_example.resources.lazy_local_ray import (
    PipesRayJobClientLazyLocalResource,
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
