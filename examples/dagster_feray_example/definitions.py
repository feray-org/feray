# ruff: noqa: E402
import warnings

import dagster as dg
from dagster._utils import warnings as dagster_warnings

warnings.filterwarnings("ignore", category=dagster_warnings.BetaWarning)
warnings.filterwarnings("ignore", category=dagster_warnings.PreviewWarning)

from pathlib import Path

from dagster_feray_example import defs as example_defs
from dagster_feray_example.defs import ray as ray_defs
from dagster_feray_example.resources import get_resources_for_deployment


@dg.definitions
def defs():
    resource_defs = get_resources_for_deployment()

    all_assets = dg.with_source_code_references(
        [
            *dg.load_assets_from_package_module(
                ray_defs,
                automation_condition=dg.AutomationCondition.eager()
                | dg.AutomationCondition.on_missing(),
                group_name="ray_example",
            ),
        ]
    )

    all_asset_checks = [*dg.load_asset_checks_from_package_module(example_defs)]

    all_assets = dg.link_code_references_to_git(
        assets_defs=all_assets,
        git_url="https://github.com/feray-org",
        git_branch="main",
        file_path_mapping=dg.AnchorBasedFilePathMapping(
            local_file_anchor=Path(__file__).parent,
            file_anchor_path_in_repository="examples/dagster_feray_example",
        ),
    )

    return dg.Definitions(
        assets=[
            *all_assets,
        ],
        asset_checks=[*all_asset_checks],
        resources=resource_defs,
    )
