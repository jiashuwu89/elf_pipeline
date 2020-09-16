"""A class to coordinate the main tasks of the pipeline.
These tasks are:
    - (Extract) Determining what new data was obtained and what files should
    be created
    - (Transform) Creation of new files
    - (Load) Uploading of new files, and reporting errors
"""
import logging
import traceback

from output.exception_collector import ExceptionCollector
from output.server_manager import ServerManager
from processor.processor_manager import ProcessorManager
from request.request_getter_manager import RequestGetterManager
from util import science_utils
from util.constants import DAILY_EMAIL_LIST

# TODO: self.times should be an enum


# TODO: Eliminate Pipeline query
class Coordinator:
    """Coordinator class to coordinate the pipeline.

    mission_ids
        list containing subset of 1, 2, 3 for ELA, ELB, EM3 (respectively)
    times
        "downlink" or "collection", for downlink time and collection time,
        respectively
    start_time/end_time
        time range to search for data
    products
        list containing subset of ALL_PRODUCTS, specifying products for which
        to search for data
    calculate
        Search for new data and use to calculate new downlinks, as opposed to
        using downlinks already found in science downlinks table
    update_db
        Relevant ONLY IF calculate is True. Upload calculated downlinks to the
        science downlinks table
    output_dir
        Directory in which to put files when generated
    upload
        Upload generated files to server
    email
        Email notifications if exceptions occurred during processing
    """

    def __init__(self, pipeline_config):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)
        self.exception_collector = ExceptionCollector(DAILY_EMAIL_LIST)

        self.pipeline_config = pipeline_config

        # Initialize Pipeline Managers
        self.request_getter_manager = RequestGetterManager(pipeline_config)
        self.processor_manager = ProcessorManager(pipeline_config, self.exception_collector)
        self.server_manager = ServerManager()

    def execute_pipeline(self, pipeline_query):
        """Execute the pipeline"""
        try:
            # Extract
            self.logger.info("🌥  Getting Processing Requests")
            processing_requests = self.get_processing_requests(pipeline_query)
            self.logger.info(
                f"🌥  Got {len(processing_requests)} processing requests:\n\n\t"
                + "\n\t".join(str(pr) for pr in processing_requests)
                + "\n"
            )  # TODO: s_if_plural

            # Transform
            self.logger.info("⛅️  Generating Files")
            generated_files = self.generate_files(processing_requests)
            self.logger.info(
                f"⛅️  Generated {len(generated_files)} file{science_utils.s_if_plural(generated_files)}:\n\n\t"
                + "\n\t".join(generated_files)
                + "\n"
            )

            # Load
            self.logger.info("🌤\tTransferring Files")
            transferred_files_count = self.transfer_files(generated_files)
            self.logger.info(f"🌤\tTransferred {transferred_files_count} files")

        except Exception as e:
            traceback_msg = traceback.format_exc()
            self.exception_collector.record_exception(e, traceback_msg)

        if self.exception_collector.email_list and self.pipeline_config.email:
            self.logger.info("🌦\tProblems detected, sending email notification")
            self.exception_collector.email()
        else:
            self.logger.info(
                "🌞\tPipeline completed successfully"
                if not self.exception_collector.email_list
                else "🌦\tProblems detected"
            )

    def get_processing_requests(self, pipeline_query):
        return self.request_getter_manager.get_processing_requests(pipeline_query)

    def generate_files(self, processing_requests):
        if not self.generate_files:
            self.logger.info("No files generated")
            return []

        generated_files = self.processor_manager.generate_files(processing_requests)

        return generated_files

    def transfer_files(self, generated_files):
        if not self.pipeline_config.upload:
            self.logger.info("No files transferred")
            return 0

        transferred_files_count = self.server_manager.transfer_files(generated_files)

        if len(generated_files) != transferred_files_count:
            raise RuntimeError(f"Transferred only {transferred_files_count}/{len(generated_files)} files")

        return transferred_files_count
