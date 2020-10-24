"""The ProcessorManager assigns file-generation tasks to processors"""
import logging
import os
import traceback
from typing import Dict, Iterable, Set, Type

from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from output.exception_collector import ExceptionCollector
from processor.science_processor import ScienceProcessor


class ProcessorManager:
    """A class to generate files using processors, given processing requests.

    Parameters
    ----------
    pipeline_config : Type[PipelineConfig]
    processor_map : Dict[str, ScienceProcessor]
    exception_collector : ExceptionCollector
        If a certain processing request causes an exception, the exception
        will be caught and recorded using exception_collector. This set up
        allows for other processing requests to be addressed, as opposed to
        letting the exception propagate upwards.
    """

    def __init__(
        self,
        pipeline_config: Type[PipelineConfig],
        processor_map: Dict[str, ScienceProcessor],
        exception_collector: ExceptionCollector,
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pipeline_config = pipeline_config
        self.processor_map = processor_map
        self.exception_collector = exception_collector

    # TODO: Should the return type of generate_files of processors also be Set[str]?
    def generate_files(self, processing_requests: Iterable[ProcessingRequest]) -> Set[str]:
        """Given requests, generate appropriate files using processors.

        Parameters
        ----------
        processing_requests : Iterable[ProcessingRequest]
            A collection of objects for which files will be generated

        Returns
        -------
        Set[str]
            A set of the filenames of files created by the method.
        """
        files = set()

        for pr in processing_requests:
            self.logger.info(f"🔔🔔🔔  Generating files for {str(pr)}")
            try:
                generated_files = self.processor_map[pr.data_product].generate_files(pr)
                self.logger.info(
                    f"🕶  Successfully generated files: {', '.join(os.path.basename(f) for f in generated_files)}"
                )
                files.update(generated_files)
            except Exception as e:
                traceback_msg = traceback.format_exc()
                self.exception_collector.record_exception(str(pr), e, traceback_msg)

        return files
