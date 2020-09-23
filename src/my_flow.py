"""Attempting to take advantage of prefect.

Functionally just run.py and coordinator.py combined
"""
from prefect import Flow, task

from data_type.pipeline_config import ArgparsePipelineConfig
from data_type.pipeline_query import ArgparsePipelineQuery
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
from run import CLIHandler
from util.constants import DAILY_EMAIL_LIST


@task
def handle_args():
    argparser = CLIHandler.get_argparser()
    args = argparser.parse_args()

    pipeline_config = ArgparsePipelineConfig(args)
    pipeline_query = ArgparsePipelineQuery(args)

    return pipeline_config, pipeline_query


@task
def get_processing_requests(pipeline_config, pipeline_query):
    request_getters = [
        IdpuRequestGetter(pipeline_config),
        MrmRequestGetter(pipeline_config),
        EngRequestGetter(pipeline_config),
        StateRequestGetter(pipeline_config),
    ]
    request_getter_manager = RequestGetterManager(pipeline_config, request_getters)
    return request_getter_manager.get_processing_requests(pipeline_query)


@task
def generate_files(pipeline_config, processing_requests):
    if not pipeline_config.generate_files:
        return []

    eng_processor = EngProcessor(pipeline_config)
    epd_processor = EpdProcessor(pipeline_config)
    fgm_processor = FgmProcessor(pipeline_config)
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
        "state": state_processor,
    }
    exception_collector = ExceptionCollector(DAILY_EMAIL_LIST)

    processor_manager = ProcessorManager(pipeline_config, processor_map, exception_collector)

    generated_files = processor_manager.generate_files(processing_requests)

    if exception_collector.count and pipeline_config.email:
        # self.logger.info("🌦 🌦 🌦 🌦 🌦  Problems detected, sending email notification")
        exception_collector.email()

    return generated_files


@task
def transfer_files(pipeline_config, generated_files):
    if not pipeline_config.upload:
        # self.logger.info("No files transferred, as specified")
        return 0

    server_manager = ServerManager()

    # if len(generated_files) != transferred_files_count:
    #     raise RuntimeError(f"Transferred only {transferred_files_count}/{len(generated_files)} files")

    return server_manager.transfer_files(generated_files)


with Flow("Science Processing Pipeline") as flow:
    objs = handle_args()
    pipeline_config, pipeline_query = objs[0], objs[1]

    processing_requests = get_processing_requests(pipeline_config, pipeline_query)
    generated_files = generate_files(pipeline_config, processing_requests)
    trasnferred_files_count = transfer_files(pipeline_config, generated_files)

flow.run()
