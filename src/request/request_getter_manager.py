"""Definition of class to determine data to process, files to generate"""
import logging
from typing import Set

from data_type.pipeline_config import PipelineConfig
from data_type.pipeline_query import PipelineQuery
from data_type.processing_request import ProcessingRequest


class RequestGetterManager:
    """An object to determine mission/time/product combinations to process.

    The RequestGetterManager owns several RequestGetters that can be used to
    get ProcessingRequest's.

    Parameters
    ----------
    pipeline_config: PipelineConfig
    request_getters: List[RequestGetter]
        A list of request getters
    """

    def __init__(self, pipeline_config: PipelineConfig, request_getters):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.pipeline_config = pipeline_config
        self.request_getters = request_getters

    def get_processing_requests(self, pipeline_query: PipelineQuery) -> Set[ProcessingRequest]:
        """Determines IDPU, MRM, ENG, and State products to be created.

        Parameters
        ----------
        pipeline_query: PipelineQuery

        Returns
        -------
        Set[ProcessingRequests]
            All ProcessingRequests found, representing items to be generated
        """
        processing_requests = set()

        for request_getter in self.request_getters:
            processing_requests.update(request_getter.get(pipeline_query))

        return sorted(processing_requests)
