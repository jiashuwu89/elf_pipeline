"""Definition of class to determine data to process, files to generate"""
import logging
from typing import Set

from data_type.pipeline_config import PipelineConfig
from data_type.pipeline_query import PipelineQuery
from data_type.processing_request import ProcessingRequest
from request.request_getter.eng_request_getter import EngRequestGetter
from request.request_getter.idpu_request_getter import IdpuRequestGetter
from request.request_getter.mrm_request_getter import MrmRequestGetter
from request.request_getter.state_request_getter import StateRequestGetter


class RequestGetterManager:
    """An object to determine mission/time/product combinations to process.

    The RequestGetterManager owns several RequestGetters that can be used to
    get ProcessingRequest's.

    Parameters
    ----------
    pipeline_config: PipelineConfig
    """

    def __init__(self, pipeline_config: PipelineConfig):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.pipeline_config = pipeline_config

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

        # TODO: give request getters to this manager via a list as a parameter to constructor
        processing_requests.update(IdpuRequestGetter(self.pipeline_config).get(pipeline_query))
        processing_requests.update(MrmRequestGetter(self.pipeline_config).get(pipeline_query))
        processing_requests.update(EngRequestGetter(self.pipeline_config).get(pipeline_query))
        processing_requests.update(StateRequestGetter(self.pipeline_config).get(pipeline_query))

        return sorted(processing_requests)
