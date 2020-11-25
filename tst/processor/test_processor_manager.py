import datetime as dt

from data_type.pipeline_config import ArgparsePipelineConfig
from data_type.processing_request import ProcessingRequest
from output.exception_collector import ExceptionCollector
from processor.processor_manager import ProcessorManager
from processor.science_processor import ScienceProcessor
from run import CLIHandler


class DummyGoodProcessor(ScienceProcessor):
    def generate_files(self, processing_request):
        return ["filename"]


class DummyBadProcessor(ScienceProcessor):
    def generate_files(self, processing_request):
        raise Exception("exception")


class TestProcessorManager:
    ARGS = ["-v", "-w", "-q", "-o", ".", "daily"]
    PIPELINE_CONFIG = ArgparsePipelineConfig(CLIHandler.get_argparser().parse_args(ARGS))
    PROCESSOR_MAP = {"good": DummyGoodProcessor(PIPELINE_CONFIG), "bad": DummyBadProcessor(PIPELINE_CONFIG)}
    EXCEPTION_COLLECTOR = ExceptionCollector([])

    PROCESSOR_MANAGER = ProcessorManager(PIPELINE_CONFIG, PROCESSOR_MAP, EXCEPTION_COLLECTOR)

    def test_generate_files(self):
        processing_requests_1 = [ProcessingRequest(1, "good", dt.date(2020, 1, 2))]
        files_1 = self.PROCESSOR_MANAGER.generate_files(processing_requests_1)
        assert files_1 == {"filename"}

        processing_requests_2 = [
            ProcessingRequest(1, "good", dt.date(2020, 1, 2)),
            ProcessingRequest(1, "good", dt.date(2020, 1, 3)),
        ]
        files_2 = self.PROCESSOR_MANAGER.generate_files(processing_requests_2)
        assert files_2 == {"filename"}

        processing_requests_3 = [ProcessingRequest(1, "bad", dt.date(2020, 1, 2))]
        files_3 = self.PROCESSOR_MANAGER.generate_files(processing_requests_3)
        assert files_3 == set()
