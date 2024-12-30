import sys
from dagster import AssetChecksDefinition

from .energy_prediction import * # noqa: F403
from .weekly_totals import * # noqa: F403

asset_checks = [
    getattr(sys.modules[__name__], name)
    for name in dir(sys.modules[__name__])
    if isinstance(getattr(sys.modules[__name__], name), AssetChecksDefinition)
]
