import datetime as dt

from data_type.pipeline_query import ParameterizedPipelineQuery
from data_type.processing_request import ProcessingRequest
from data_type.time_type import TimeType
from output.exception_collector import ExceptionCollector
from request.request_getter.request_getter import RequestGetter
from request.request_getter_manager import RequestGetterManager
from util.dummy import SafeTestPipelineConfig


class DummyRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        return {
            ProcessingRequest(1, "epdef", dt.date(2021, 1, 2)),
            ProcessingRequest(1, "epdef", dt.date(2021, 1, 3)),
            ProcessingRequest(1, "epdef", dt.date(2001, 1, 2)),
        }


class RuntimeErrorRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        return RuntimeError()


class TestRequestGetterManager:
    def test_init(self):
        RequestGetterManager(SafeTestPipelineConfig(), [], ExceptionCollector([]))

    def test_get_processing_requests(self):
        pipeline_config = SafeTestPipelineConfig()
        request_getter_manager = RequestGetterManager(
            SafeTestPipelineConfig(),
            [DummyRequestGetter(pipeline_config), RuntimeErrorRequestGetter(pipeline_config)],
            ExceptionCollector([]),
        )

        obtained_processing_requests = request_getter_manager.get_processing_requests(
            ParameterizedPipelineQuery(
                [1], "epdef", (dt.datetime(2021, 1, 1), dt.datetime(2021, 1, 5), TimeType.DOWNLINK)
            )
        )

        assert obtained_processing_requests == [
            ProcessingRequest(1, "epdef", dt.date(2021, 1, 2)),
            ProcessingRequest(1, "epdef", dt.date(2021, 1, 3)),
        ]
        assert request_getter_manager.exception_collector.count == 1
