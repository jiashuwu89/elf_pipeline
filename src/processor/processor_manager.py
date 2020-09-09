"""The ProcessorManager assigns file-generation tasks to processors"""
import traceback

from output.exception_collector import ExceptionCollector
from processor.eng_processor import EngProcessor
from processor.idpu.epd_processor import EpdProcessor
from processor.idpu.fgm_processor import FgmProcessor
from processor.mrm_processor import MrmProcessor
from processor.state_processor import StateProcessor


class ProcessorManager:
    """A class to generate files using processors, given processing requests."""

    def __init__(self, session, exception_collector):
        self.session = session
        self.processors = self.init_processors()
        self.exception_collector = exception_collector

    def generate_files(self, processing_requests):
        """Given requests, generate appropriate files using processors"""
        files = set()

        for pr in processing_requests:
            try:
                files.update(self.processors[pr.data_product].generate_files(pr))
            except Exception as e:
                traceback_msg = traceback.format_exc()
                self.exception_collector.record_exception(str(pr), e, traceback_msg)

        return files

    def init_processors(self):
        """Creates a dict mapping data product name to processor"""
        return {
            "eng": EngProcessor(),
            "epd": EpdProcessor(),
            "fgm": FgmProcessor(),
            "mrm": MrmProcessor(),
            "state": StateProcessor(),
        }
