import datetime as dt
import os

import pytest

from data_type.pipeline_query import ParameterizedPipelineQuery
from data_type.time_type import TimeType
from request.request_getter.mrm_request_getter import MrmRequestGetter
from util.constants import CREDENTIALS_FILE
from util.dummy import DummyPipelineConfig


class TestMrmRequestGetter:
    MRM_REQUEST_GETTER = MrmRequestGetter(DummyPipelineConfig())

    # @pytest.mark.skipif(not os.path.isfile(CREDENTIALS_FILE), reason="Probably in CI/CD pipeline")
    def test_get(self):
        pq_1 = ParameterizedPipelineQuery(
            [1], ["mrma"], (dt.datetime(1990, 1, 1), dt.datetime(1990, 2, 2), TimeType.COLLECTION)
        )
        assert len(self.MRM_REQUEST_GETTER.get(pq_1)) == 0

        pq_2 = ParameterizedPipelineQuery(
            [1], ["mrma"], (dt.datetime(2019, 6, 1), dt.datetime(2019, 6, 6), TimeType.COLLECTION)
        )
        res_2 = self.MRM_REQUEST_GETTER.get(pq_2)
        assert len(res_2) == 5
        for pr in res_2:
            assert pr.mission_id == 1
            assert pr.data_product == "mrma"
            assert dt.date(2019, 6, 1) <= pr.date <= dt.date(2019, 6, 6)

        pq_3 = ParameterizedPipelineQuery(
            [1], ["mrma"], (dt.datetime(2019, 6, 1), dt.datetime(2019, 6, 6), TimeType.DOWNLINK)
        )
        res_3 = self.MRM_REQUEST_GETTER.get(pq_3)
        assert len(res_3) == 6
        for pr in res_3:
            assert pr.mission_id == 1
            assert pr.data_product == "mrma"
            assert dt.date(2019, 5, 30) <= pr.date <= dt.date(2019, 6, 6)

        pq_4 = ParameterizedPipelineQuery(
            [1], ["eng"], (dt.datetime(2019, 6, 1), dt.datetime(2019, 6, 6), TimeType.DOWNLINK)
        )
        assert len(self.MRM_REQUEST_GETTER.get(pq_4)) == 0
