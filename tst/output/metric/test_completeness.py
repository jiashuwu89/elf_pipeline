import pandas as pd

from data_type.completeness_config import COMPLETENESS_CONFIG_MAP
from output.metric.completeness import CompletenessUpdater
from util.dummy import DummyPipelineConfig, DummyProcessingRequest


class TestCompletenessUpdater:
    def test_update_completeness_table_with_single_idpu_type(self):
        pipeline_config = DummyPipelineConfig()
        processing_request = DummyProcessingRequest()
        empty_df = pd.DataFrame()

        completeness_updater = CompletenessUpdater(pipeline_config.session, COMPLETENESS_CONFIG_MAP)
        assert (
            completeness_updater.update_completeness_table_with_single_idpu_type(processing_request, empty_df, False)
            is False
        )
