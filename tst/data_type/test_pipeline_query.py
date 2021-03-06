import datetime as dt

import dateutil
import pytest

from data_type.pipeline_query import ArgparsePipelineQuery, ParameterizedPipelineQuery, PipelineQuery
from data_type.time_type import TimeType
from run import CLIHandler
from util.constants import ALL_PRODUCTS


class TestPipelineQuery:
    def test_data_products_to_idpu_types(self):
        assert PipelineQuery.data_products_to_idpu_types([]) == []
        assert PipelineQuery.data_products_to_idpu_types(["eng"]) == [14, 15, 16]
        assert sorted(PipelineQuery.data_products_to_idpu_types(["epdef", "fgs"])) == [1, 2, 3, 4, 17, 18, 22, 24]


class TestParameterizedPipelineQuery:
    TIME_TUPLE = (dt.datetime(2020, 5, 5), dt.datetime(2020, 5, 6), TimeType.COLLECTION)
    PARAMETERIZED_PIPELINE_QUERY = ParameterizedPipelineQuery([1], ["mrma"], TIME_TUPLE)

    def test_mission_ids(self):
        assert 1 in self.PARAMETERIZED_PIPELINE_QUERY.mission_ids
        assert 2 not in self.PARAMETERIZED_PIPELINE_QUERY.mission_ids

    def test_data_products(self):
        assert "mrma" in self.PARAMETERIZED_PIPELINE_QUERY.data_products
        assert "mrmi" not in self.PARAMETERIZED_PIPELINE_QUERY.data_products

    def test_times(self):
        assert self.PARAMETERIZED_PIPELINE_QUERY.times == TimeType.COLLECTION

    def test_start_time(self):
        assert self.PARAMETERIZED_PIPELINE_QUERY.start_time == dt.datetime(2020, 5, 5)

    def test_end_time(self):
        assert self.PARAMETERIZED_PIPELINE_QUERY.end_time == dt.datetime(2020, 5, 6)


class TestArgparsePipelineQuery:
    ARGS = ["-v", "-w", "-q", "-o", ".", "daily"]
    ARGPARSE_PIPELINE_QUERY = ArgparsePipelineQuery(CLIHandler.get_argparser().parse_args(ARGS))

    def test_mission_ids(self):
        assert 1 in self.ARGPARSE_PIPELINE_QUERY.mission_ids
        assert 2 in self.ARGPARSE_PIPELINE_QUERY.mission_ids

    def test_data_products(self):
        assert ALL_PRODUCTS == self.ARGPARSE_PIPELINE_QUERY.data_products

    def test_times(self):
        assert TimeType.DOWNLINK == self.ARGPARSE_PIPELINE_QUERY.times

    def test_start_time(self):
        assert isinstance(self.ARGPARSE_PIPELINE_QUERY.start_time, dt.datetime)
        assert self.ARGPARSE_PIPELINE_QUERY.start_time < self.ARGPARSE_PIPELINE_QUERY.end_time

    def test_end_time(self):
        assert isinstance(self.ARGPARSE_PIPELINE_QUERY.end_time, dt.datetime)
        assert self.ARGPARSE_PIPELINE_QUERY.start_time < self.ARGPARSE_PIPELINE_QUERY.end_time

    def test_str(self):
        assert isinstance(str(self.ARGPARSE_PIPELINE_QUERY), str)

    def test_repr(self):
        assert isinstance(self.ARGPARSE_PIPELINE_QUERY.__repr__(), str)
        assert self.ARGPARSE_PIPELINE_QUERY.__repr__() == str(self.ARGPARSE_PIPELINE_QUERY)

    def test_get_mission_ids(self):
        assert ArgparsePipelineQuery.get_mission_ids(True, True, True) == [1, 2, 3]
        assert ArgparsePipelineQuery.get_mission_ids(True, True, False) == [1, 2]
        assert ArgparsePipelineQuery.get_mission_ids(True, False, True) == [1, 3]
        assert ArgparsePipelineQuery.get_mission_ids(True, False, False) == [1]
        assert ArgparsePipelineQuery.get_mission_ids(False, True, True) == [2, 3]
        assert ArgparsePipelineQuery.get_mission_ids(False, True, False) == [2]
        assert ArgparsePipelineQuery.get_mission_ids(False, False, True) == [3]
        assert ArgparsePipelineQuery.get_mission_ids(False, False, False) == [1, 2]

    def test_get_data_products(self):
        assert ArgparsePipelineQuery.get_data_products(["fgf", "fgs"]) == ["fgf", "fgs"]

        with pytest.raises(ValueError):
            ArgparsePipelineQuery.get_data_products([])

    def test_get_times(self):
        assert ArgparsePipelineQuery.get_times(True) == TimeType.COLLECTION
        assert ArgparsePipelineQuery.get_times(False) == TimeType.DOWNLINK

    def test_validate_time(self):
        start_time_str = "2020-09-08"
        end_time_str = "2020-09-12"
        start_time = dt.datetime(2020, 9, 8)
        end_time = dt.datetime(2020, 9, 12)

        assert ArgparsePipelineQuery.validate_time(start_time_str, end_time_str) == (start_time, end_time)

        with pytest.raises(RuntimeError):
            ArgparsePipelineQuery.validate_time(end_time_str, start_time_str)

        with pytest.raises(dateutil.parser._parser.ParserError):
            ArgparsePipelineQuery.validate_time("BLAH", 5)
