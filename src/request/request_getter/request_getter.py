import logging
from abc import ABC, abstractmethod


class RequestGetter(ABC):
    def __init__(self, session):
        self.session = session
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def get(start_time, end_time):
        pass
