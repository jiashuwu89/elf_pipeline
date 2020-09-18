import logging
from abc import ABC, abstractmethod


class RequestGetter(ABC):
    """Object to obtain ProcessingRequests, given the PipelineQuery.

    Parameters
    ----------
    pipeline_config : PipelineConfig
    """

    def __init__(self, pipeline_config):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.pipeline_config = pipeline_config

    @abstractmethod
    def get(self, pipeline_query):
        """Given a PipelineQuery, determines what needs to be processed.

        Should be overriden by derived classes

        Parameters
        ----------
        pipeline_query : PipelineQuery

        Returns
        -------
        Set[ProcessingRequests]
            All ProcessingRequests related to the particular RequestGetter
        """
        raise NotImplementedError

    @staticmethod
    def get_relevant_products(data_products, product_map):
        selected_products = set()
        for product in data_products:
            if product in product_map:
                if isinstance(product_map[product], str):
                    selected_products.add(product_map[product])
                else:
                    selected_products.update(product_map[product])
        return selected_products
