import traceback

from util.constants import DAILY_EMAIL_LIST
from util.exception_collector import ExceptionCollector

from processor.eng_processor import EngProcessor
from processor.epd_processor import EpdProcessor
from processor.fgm_processor import FgmProcessor
from processor.mrm_processor import MrmProcessor
from processor.state_processor import StateProcessor


class ProcessorManager:
    """A class to generate files using processors, given processing requests."""
    def __init__(self, session):
        self.session = session
        self.processors = {
            "eng": EngProcessor(),
            "epd": EpdProcessor(),
            "fgm": FgmProcessor(),
            "mrm": MrmProcessor(),
            "state": StateProcessor(),
        }
        self.exception_collector = ExceptionCollector(DAILY_EMAIL_LIST)

    def generate_files(self, processing_requests):
        files = set()

        for pr in processing_requests:
            try:
                files.update(self.processors[pr.data_product].generate_files(pr))
            except Exception as e:
                traceback_msg = traceback.format_exc()
                self.exception_collector.record_exception(pr.to_string(), e, traceback_msg)

        if self.exception_collector.email_list:
            self.exception_collector.email()

        return files
