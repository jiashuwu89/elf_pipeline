"""The ProcessorManager assigns file-generation tasks to processors"""
import logging
import traceback


class ProcessorManager:
    """A class to generate files using processors, given processing requests."""

    def __init__(self, pipeline_config, processor_map, exception_collector):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pipeline_config = pipeline_config
        self.processor_map = processor_map
        self.exception_collector = exception_collector

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
