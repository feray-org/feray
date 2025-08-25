import dagster as dg
from feray.data_structures import FeatureDefinition
from feray.service import FeatureRegistry


def build_feature_assets(registry: FeatureRegistry, resource_key: str) -> list:
    """
    A factory function that dynamically creates Dagster multi_assets from a FeatureRegistry.
    """
    assets = []
    for _asset_key, definition in registry.definitions.items():

        def _create_compute_fn(def_closure: FeatureDefinition):
            deps = {
                def_closure.asset_key: {
                    dg.AssetKey(dep) for dep in def_closure.dependencies
                }
            }

            @dg.multi_asset(
                name=def_closure.asset_key,
                group_name="feature_platform",
                required_resource_keys={resource_key},
                outs={def_closure.asset_key: dg.AssetOut()},
                internal_asset_deps=deps,
            )
            def _dynamic_asset(context: dg.AssetExecutionContext):
                service = getattr(context.resources, resource_key).get_service(registry)
                context.log.info(
                    "Invoking feature service to materialize asset: %s",
                    def_closure.asset_key,
                )
                yield service._materialize_one(
                    asset_key=context.asset_key.to_user_string(), force_recompute=False
                )

            _dynamic_asset.__name__ = f"asset_{definition.asset_key}"
            return _dynamic_asset

        assets.append(_create_compute_fn(definition))
    return assets
