import datetime as dt
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CompletenessConfig:
    data_type: str
    start_delay: dt.timedelta
    start_margin: dt.timedelta
    expected_collection_duration: dt.timedelta
    classic_science_zone_gap_size: dt.timedelta
    intent_type: str = "ScienceCollection"
    median_diff: Optional[float] = None


MRM_COMPLETENESS_CONFIG = CompletenessConfig(
    data_type="MRM",
    start_delay=dt.timedelta(seconds=0),
    start_margin=dt.timedelta(seconds=3),
    expected_collection_duration=dt.timedelta(minutes=24),
    classic_science_zone_gap_size=dt.timedelta(minutes=5),
    intent_type="AttitudeCollection",
    median_diff=0.32,  # (mrm sample rate = 56.25/18)
)

FGM_COMPLETENESS_CONFIG = CompletenessConfig(
    data_type="FGM",
    start_delay=dt.timedelta(seconds=50),  # 3 second margin
    start_margin=dt.timedelta(seconds=3),
    expected_collection_duration=dt.timedelta(minutes=6, seconds=5),
    classic_science_zone_gap_size=dt.timedelta(minutes=20),
)

EPDE_COMPLETENESS_CONFIG = CompletenessConfig(
    data_type="EPDE",
    start_delay=dt.timedelta(seconds=50),  # first two spin periods discarded, with 3 seconds margin
    start_margin=dt.timedelta(seconds=9),
    expected_collection_duration=dt.timedelta(minutes=6, seconds=5),
    classic_science_zone_gap_size=dt.timedelta(minutes=20),
)

EPDE_32_COMPLETENESS_CONFIG = CompletenessConfig(
    data_type="EPDE_32",
    start_delay=dt.timedelta(seconds=50),  # first two spin periods discarded, with 3 seconds margin
    start_margin=dt.timedelta(seconds=9),
    expected_collection_duration=dt.timedelta(minutes=6, seconds=5),
    classic_science_zone_gap_size=dt.timedelta(minutes=20),
)

EPDI_COMPLETENESS_CONFIG = CompletenessConfig(
    data_type="EPDI",
    start_delay=dt.timedelta(seconds=50),
    start_margin=dt.timedelta(seconds=9),
    expected_collection_duration=dt.timedelta(minutes=6, seconds=5),
    classic_science_zone_gap_size=dt.timedelta(minutes=20),
)

IEPDE_COMPLETENESS_CONFIG = EPDE_COMPLETENESS_CONFIG
IEPDI_COMPLETENESS_CONFIG = EPDI_COMPLETENESS_CONFIG

COMPLETENESS_CONFIG_MAP = {
    "MRM": MRM_COMPLETENESS_CONFIG,
    "FGM": FGM_COMPLETENESS_CONFIG,
    "EPDE": EPDE_COMPLETENESS_CONFIG,
    "EPDE_32": EPDE_32_COMPLETENESS_CONFIG,
    "EPDI": EPDI_COMPLETENESS_CONFIG,
    "IEPDE": IEPDE_COMPLETENESS_CONFIG,
    "IEPDI": IEPDI_COMPLETENESS_CONFIG,
}
