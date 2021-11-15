"""Definition of class to determine data to process, files to generate"""
import logging
import traceback
from typing import List, Type

from data_type.pipeline_config import PipelineConfig
from data_type.pipeline_query import PipelineQuery
from data_type.processing_request import ProcessingRequest
from output.exception_collector import ExceptionCollector
from request.request_getter.request_getter import RequestGetter
from util.constants import MISSION_START_DATE


class RequestGetterManager:
    """An object to determine mission/time/product combinations to process.

    The RequestGetterManager owns several RequestGetters that can be used to
    get ProcessingRequest's.

    Parameters
    ----------
    pipeline_config : PipelineConfig
    request_getters : List[RequestGetter]
    exception_collector : ExceptionCollector
    """

    def __init__(
        self,
        pipeline_config: Type[PipelineConfig],
        request_getters: List[RequestGetter],
        exception_collector: ExceptionCollector,
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.pipeline_config = pipeline_config
        self.request_getters = request_getters
        self.exception_collector = exception_collector

    def get_processing_requests(self, pipeline_query: Type[PipelineQuery]) -> List[ProcessingRequest]:
        """Determines products to be created.

        Utilizes RequestGetter's in self.request_getters to get the actual
        ProcessingRequest's. The major data products are FGM, EPD, MRM, State,
        and ENG.

        Parameters
        ----------
        pipeline_query : PipelineQuery

        Returns
        -------
        Set[ProcessingRequests]
            All ProcessingRequests found, representing items to be generated
        """
        processing_requests = set()

        for request_getter in self.request_getters:
            try:
                processing_requests.update(request_getter.get(pipeline_query))
            except Exception as e:
                traceback_msg = traceback.format_exc()
                self.exception_collector.record_exception(request_getter.__class__, e, traceback_msg)

        return sorted(pr for pr in processing_requests if pr.date >= MISSION_START_DATE)
