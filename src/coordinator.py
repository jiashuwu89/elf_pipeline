import logging
import traceback
from typing import List, Set, Type

from data_type.pipeline_config import PipelineConfig
from data_type.pipeline_query import PipelineQuery
from data_type.processing_request import ProcessingRequest
from output.downlink.downlink_manager import DownlinkManager
from output.exception_collector import ExceptionCollector
from output.server_manager import ServerManager
from processor.idpu.eng_processor import EngProcessor
from processor.idpu.epd_processor import EpdProcessor
from processor.idpu.fgm_processor import FgmProcessor
from processor.mrm_processor import MrmProcessor
from processor.processor_manager import ProcessorManager
from processor.state_processor import StateProcessor
from request.request_getter.eng_request_getter import EngRequestGetter
from request.request_getter.idpu_request_getter import IdpuRequestGetter
from request.request_getter.mrm_request_getter import MrmRequestGetter
from request.request_getter.state_request_getter import StateRequestGetter
from request.request_getter_manager import RequestGetterManager
from util import science_utils
from util.constants import DAILY_EMAIL_LIST


class Coordinator:
    """A class to coordinate the main tasks of the pipeline.

    These tasks are:
        1. Extract: Determining what new data was obtained and files to be created
        2. Transform: Creation of new files
        3. Load: Uploading of new files, and reporting errors

    Parameters
    ----------
    pipeline_config : Type[PipelineConfig]
        Configuration object for the pipeline
    """

    def __init__(self, pipeline_config: Type[PipelineConfig]):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.exception_collector = ExceptionCollector(DAILY_EMAIL_LIST)

        self.pipeline_config = pipeline_config

        self.downlink_manager = DownlinkManager(pipeline_config)

        # Initialize Pipeline Managers
        request_getters = [
            IdpuRequestGetter(pipeline_config, self.downlink_manager),
            MrmRequestGetter(pipeline_config),
            EngRequestGetter(pipeline_config),
            StateRequestGetter(pipeline_config),
        ]
        self.request_getter_manager = RequestGetterManager(pipeline_config, request_getters, self.exception_collector)

        eng_processor = EngProcessor(pipeline_config, self.downlink_manager)
        epd_processor = EpdProcessor(pipeline_config, self.downlink_manager)
        fgm_processor = FgmProcessor(pipeline_config, self.downlink_manager)
        mrm_processor = MrmProcessor(pipeline_config)
        state_processor = StateProcessor(pipeline_config)
        processor_map = {
            "eng": eng_processor,
            "epdef": epd_processor,
            "epdes": epd_processor,
            "epdif": epd_processor,
            "epdis": epd_processor,
            "fgf": fgm_processor,
            "fgs": fgm_processor,
            "mrma": mrm_processor,
            "mrmi": mrm_processor,
            "state-defn": state_processor,
            "state-pred": state_processor,
        }
        self.processor_manager = ProcessorManager(pipeline_config, processor_map, self.exception_collector)

        self.server_manager = ServerManager()  # TODO: Only instantiate this when necessary, not during initialization

    def execute_pipeline(self, pipeline_query: Type[PipelineQuery]) -> None:
        """Executes the pipeline.

        Any exceptions will be caught and recorded, if they were not caught at
        a lower level (ex. by the ProcessorManager).

        Parameters
        ----------
        pipeline_query : Type[PipelineQuery]
        """
        self.logger.info(f"Executing pipeline on query:\n\n{str(pipeline_query)}\n")
        try:
            # Extract
            self.logger.info("🌥 🌥 🌥 🌥 🌥  Getting Processing Requests")
            processing_requests = self.get_processing_requests(pipeline_query)
            self.logger.info(
                f"🌥 🌥 🌥 🌥 🌥  Got {len(processing_requests)} "
                + f"processing request{science_utils.s_if_plural(processing_requests)}:\n\n\t"
                + "\n\t".join(str(pr) for pr in sorted(processing_requests))
                + "\n"
            )

            # Transform
            self.logger.info("⛅️ ⛅️ ⛅️ ⛅️ ⛅️  Generating Files")
            generated_files = self.generate_files(processing_requests)
            self.logger.info(
                f"⛅️ ⛅️ ⛅️ ⛅️ ⛅️  Generated {len(generated_files)} file{science_utils.s_if_plural(generated_files)}:"
                + "\n\n\t"
                + "\n\t".join(generated_files)
                + "\n"
            )

            # Load
            self.logger.info("🌤 🌤 🌤 🌤 🌤  Transferring Files")
            transferred_files_count = self.transfer_files(generated_files)
            self.logger.info(f"🌤 🌤 🌤 🌤 🌤  Transferred {transferred_files_count} files")

        except Exception as e:
            traceback_msg = traceback.format_exc()
            self.exception_collector.record_exception(e, traceback_msg)

        if self.exception_collector.count and self.pipeline_config.email:
            self.logger.info("🌦 🌦 🌦 🌦 🌦  Problems detected, sending email notification")
            self.exception_collector.email()
        else:
            self.logger.info(
                "🌞🌞🌞🌞🌞  Pipeline completed successfully"
                if not self.exception_collector.count
                else f"🌦 🌦 🌦 🌦 🌦  Detected {self.exception_collector.count} "
                + f"problem{science_utils.s_if_plural(self.exception_collector.exception_list)}"
            )

    def get_processing_requests(self, pipeline_query: Type[PipelineQuery]) -> List[ProcessingRequest]:
        """Calculates processing requests that indicate files to be created.

        Parameters
        ----------
        pipeline_query : Type[PipelineQuery]
        """
        return self.request_getter_manager.get_processing_requests(pipeline_query)

    def generate_files(self, processing_requests: List[ProcessingRequest]) -> Set[str]:
        """Generates files specified by processing requests, if necessary.

        Parameters
        ----------
        processing_requests : List[ProcessingRequest]
            A list of objects, each representing a data product, mission, and
            date combination for which files should be generated

        Returns
        -------
        Set[str]
            A list of filenames of the generated files
        """
        if not self.generate_files:
            self.logger.info("Received option to avoid generating files")
            return []

        return self.processor_manager.generate_files(processing_requests)

    def transfer_files(self, generated_files: Set[str]) -> int:
        """Sends generated files to the server, if necessary.

        Parameters
        ----------
        generated_files : List[str]
            A list of filenames to be transferred to the server

        Returns
        -------
        int
            The number of files transferred successfully
        """
        if not self.pipeline_config.upload:
            self.logger.info("Received option to avoid transferring files")
            return 0

        transferred_files_count = self.server_manager.transfer_files(generated_files)

        if len(generated_files) != transferred_files_count:
            raise RuntimeError(f"Transferred only {transferred_files_count}/{len(generated_files)} files")

        return transferred_files_count
