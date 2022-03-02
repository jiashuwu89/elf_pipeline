import datetime as dt

import pandas as pd

from data_type.completeness_config import COMPLETENESS_CONFIG_MAP, EPDE_COMPLETENESS_CONFIG
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

    def test_split_science_zones(self):
        pipeline_config = DummyPipelineConfig()
        processing_request = DummyProcessingRequest()

        # Only keep science zones that occur on the day of the processing_request
        times = pd.Series(
            [
                dt.datetime.combine(processing_request.date, dt.datetime.min.time())
                + dt.timedelta(days=-1, seconds=3 * i)
                for i in range(5)  # Delete, data is all on the day before
            ]
            + [
                dt.datetime.combine(processing_request.date, dt.datetime.min.time()) + dt.timedelta(seconds=3 * i)
                for i in range(-3, 2)  # Keep since some data is on the current day
            ]
            + [
                dt.datetime.combine(processing_request.date, dt.datetime.min.time())
                + dt.timedelta(hours=12, seconds=3 * i)
                for i in range(5)  # Keep since all data is on the current day
            ]
            + [
                dt.datetime.combine(processing_request.date, dt.datetime.min.time())
                + dt.timedelta(days=1, seconds=3 * i)
                for i in range(5)  # Delete, data is all on the day after
            ]
        )

        completeness_updater = CompletenessUpdater(pipeline_config.session, COMPLETENESS_CONFIG_MAP)
        szs = completeness_updater.split_science_zones(processing_request, EPDE_COMPLETENESS_CONFIG, times)

        assert len(szs) == 2
        for sz in szs:
            assert len(sz) == 5
            assert any(t.date() == processing_request.date for t in sz)
