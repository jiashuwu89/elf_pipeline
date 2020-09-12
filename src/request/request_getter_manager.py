"""Definition of class to determine data to process, files to generate"""
import logging

from request.request_getter.eng_request_getter import EngRequestGetter
from request.request_getter.idpu_request_getter import IdpuRequestGetter
from request.request_getter.mrm_request_getter import MrmRequestGetter
from request.request_getter.state_request_getter import StateRequestGetter


class RequestGetterManager:
    """An object to determine mission/time/product combinations to process"""

    def __init__(self, pipeline_config):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.pipeline_config = pipeline_config

    def get_processing_requests(self, pipeline_query):
        """Determines IDPU, MRM, ENG, and State products to be created"""
        # TODO: "times" is just for idpu products?

        processing_requests = set()

        processing_requests.update(IdpuRequestGetter(self.pipeline_config).get(pipeline_query))
        processing_requests.update(MrmRequestGetter(self.pipeline_config).get(pipeline_query))
        processing_requests.update(EngRequestGetter(self.pipeline_config).get(pipeline_query))
        processing_requests.update(StateRequestGetter(self.pipeline_config).get(pipeline_query))

        return processing_requests
