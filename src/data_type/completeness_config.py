import datetime as dt
from abc import ABC

# TODO: Maybe these should be represented by dictionaries instead


class CompletenessConfig(ABC):
    idpu_type = None
    data_type = None
    intent_type = None
    start_delay = None
    start_margin = None
    median_diff = None
    expected_collection_duration = None


class MrmCompletenessConfig(CompletenessConfig):
    idpu_type = -1
    data_type = "MRM"
    intent_type = "AttitudeCollection"
    start_delay = dt.timedelta(seconds=0)
    start_margin = dt.timedelta(seconds=3)
    median_diff = 0.32  # (mrm sample rate = 56.25/18)
    expected_collection_duration = dt.timedelta(minutes=24)


class FgmCompletenessConfig(CompletenessConfig):
    idpu_type = 2
    data_type = "FGM"
    intent_type = "ScienceCollection"
    start_delay = dt.timedelta(seconds=50)  # 3 second margin
    start_margin = dt.timedelta(seconds=3)
    median_diff = None
    expected_collection_duration = dt.timedelta(minutes=6, seconds=5)


class EpdeCompletenessConfig(CompletenessConfig):
    idpu_type = 4
    data_type = "EPDE"
    intent_type = "ScienceCollection"
    # first two spin periods discarded, with 3 seconds margin
    start_delay = dt.timedelta(seconds=50)
    start_margin = dt.timedelta(seconds=9)
    median_diff = None
    expected_collection_duration = dt.timedelta(minutes=6, seconds=5)


class EpdiCompletenessConfig(CompletenessConfig):
    idpu_type = 6
    data_type = "EPDI"
    intent_type = "ScienceCollection"
    start_delay = dt.timedelta(seconds=50)
    start_margin = dt.timedelta(seconds=9)
    median_diff = None
    expected_collection_duration = dt.timedelta(minutes=6, seconds=5)
