from output.exception_collector import ExceptionCollector
from request.request_getter.request_getter import RequestGetter
from request.request_getter_manager import RequestGetterManager
from util.dummy import DummyProcessingRequest, SafeTestPipelineConfig


class DummyRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        return {2, 1, 3}  # TODO: This could probably be better


class TestRequestGetterManager:
    def test_init(self):
        RequestGetterManager(SafeTestPipelineConfig(), [], ExceptionCollector([]))

    def test_get_processing_requests(self):
        pipeline_config = SafeTestPipelineConfig()
        request_getter = DummyRequestGetter(pipeline_config)
        request_getter_manager = RequestGetterManager(
            SafeTestPipelineConfig(), [request_getter], ExceptionCollector([])
        )

        assert request_getter_manager.get_processing_requests(DummyProcessingRequest()) == [1, 2, 3]
