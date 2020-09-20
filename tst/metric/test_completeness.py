import pandas as pd

from data_type.completeness_config import EpdiCompletenessConfig
from dummy import DummyPipelineConfig, DummyProcessingRequest
from metric.completeness import CompletenessUpdater


class TestCompletenessUpdater:
    def test_update_completeness_table(self):
        pipeline_config = DummyPipelineConfig()
        completeness_config = EpdiCompletenessConfig()
        processing_request = DummyProcessingRequest()
        empty_series = pd.Series(dtype="datetime64[ns]")

        completeness_updater = CompletenessUpdater(pipeline_config.session, completeness_config)
        assert completeness_updater.update_completeness_table(processing_request, empty_series) is False
