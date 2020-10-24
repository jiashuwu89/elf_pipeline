"""Definition of class to determine data to process, files to generate"""
import logging
from typing import List, Type

from data_type.pipeline_config import PipelineConfig
from data_type.pipeline_query import PipelineQuery
from data_type.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter


class RequestGetterManager:
    """An object to determine mission/time/product combinations to process.

    The RequestGetterManager owns several RequestGetters that can be used to
    get ProcessingRequest's.

    Parameters
    ----------
    pipeline_config : PipelineConfig
    request_getters : List[RequestGetter]
    """

    def __init__(self, pipeline_config: Type[PipelineConfig], request_getters: List[RequestGetter]):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.pipeline_config = pipeline_config
        self.request_getters = request_getters

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
            processing_requests.update(request_getter.get(pipeline_query))

        return sorted(processing_requests)
