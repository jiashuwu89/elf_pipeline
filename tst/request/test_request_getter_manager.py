from dummy import SafeTestPipelineConfig
from request.request_getter_manager import RequestGetterManager


class TestRequestGetterManager:
    def test_init(self):
        RequestGetterManager(SafeTestPipelineConfig(), [])
