import traceback

from util.exception_collector import ExceptionCollector
from util.constants import DAILY_EMAIL_LIST


class ProcessorManager:

    def __init__(self, session):
        self.session = session
        self.processors = {
            "fgm": FgmProcessor(),
            "epd": EpdProcessor(),
            "state": StateProcessor(),
            "mrm": MrmProcessor(),
            "eng": EngProcessor()
        }
        self.exception_collector = ExceptionCollector(DAILY_EMAIL_LIST)

    def generate_files(self, processing_requests):
        files = set()

        for pr in processing_requests:
            try:
                files.update(self.processors[pr.data_product].generate_files(pr))
            except Exception as e:
                traceback_msg = traceback.format_exc()
                self.error_collector.record_exception(pr, e, traceback_msg)

        self.error_collector.email_if_exceptions_occurred()

        return files
