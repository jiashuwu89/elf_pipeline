"""The ProcessorManager assigns file-generation tasks to processors"""
import logging
import traceback

from processor.idpu.eng_processor import EngProcessor
from processor.idpu.epd_processor import EpdProcessor
from processor.idpu.fgm_processor import FgmProcessor
from processor.mrm_processor import MrmProcessor
from processor.state_processor import StateProcessor


class ProcessorManager:
    """A class to generate files using processors, given processing requests."""

    def __init__(self, pipeline_config, exception_collector):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pipeline_config = pipeline_config
        self.exception_collector = exception_collector

        self.eng_processor = EngProcessor(self.pipeline_config)
        self.epd_processor = EpdProcessor(self.pipeline_config)
        self.fgm_processor = FgmProcessor(self.pipeline_config)
        self.mrm_processor = MrmProcessor(self.pipeline_config)
        self.state_processor = StateProcessor(self.pipeline_config)
        self.processors = {
            "eng": self.eng_processor,
            "epdef": self.epd_processor,
            "epdes": self.epd_processor,
            "epdif": self.epd_processor,
            "epdis": self.epd_processor,
            "fgf": self.fgm_processor,
            "fgs": self.fgm_processor,
            "mrma": self.mrm_processor,
            "mrmi": self.mrm_processor,
            "state": self.state_processor,
        }

    def generate_files(self, processing_requests):
        """Given requests, generate appropriate files using processors"""
        files = set()

        for pr in processing_requests:
            self.logger.info(f"🔔🔔🔔  Generating files for {str(pr)}")
            try:
                generated_files = self.processors[pr.data_product].generate_files(pr)
                self.logger.info(f"🕶\tSuccessfully generated files: {generated_files}")
                files.update(generated_files)
            except Exception as e:
                traceback_msg = traceback.format_exc()
                self.exception_collector.record_exception(str(pr), e, traceback_msg)

        return files
