import logging
from abc import ABC, abstractmethod


class RequestGetter(ABC):
    def __init__(self, pipeline_config):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.pipeline_config = pipeline_config

    @abstractmethod
    def get(self, start_time, end_time):
        pass
