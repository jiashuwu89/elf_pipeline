import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable, Set, Type, Union

from data_type.pipeline_config import PipelineConfig
from data_type.pipeline_query import PipelineQuery
from data_type.processing_request import ProcessingRequest


class RequestGetter(ABC):
    """Object to obtain ProcessingRequests, given the PipelineQuery.

    Parameters
    ----------
    pipeline_config : Type[PipelineConfig]
    """

    def __init__(self, pipeline_config: Type[PipelineConfig]):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.pipeline_config = pipeline_config

    @abstractmethod
    def get(self, pipeline_query: Type[PipelineQuery]) -> Set[ProcessingRequest]:
        """Given a PipelineQuery, determines what needs to be processed.

        Should be overriden by derived classes

        Parameters
        ----------
        pipeline_query : Type[PipelineQuery]

        Returns
        -------
        Set[ProcessingRequests]
            All ProcessingRequests related to the particular RequestGetter
        """
        raise NotImplementedError

    @staticmethod
    def get_relevant_products(data_products: Iterable[str], product_map: Dict[str, Any]) -> Set[Union[int, str]]:
        """Given data products, look up relevant values and produce a set.

        This can be used to map the names of data products to their idpu
        types, for example.

        Parameters
        ----------
        data_products : Iterable[str]
            Some collection of data_products to look up in product_map
        product_map : Dict[str, Any]
            A mapping from data products to values.

        Returns
        -------
        Set[Union[int, str]]
            Values that are relevant to the given data product, according to
            the product_map
        """
        selected_products = set()
        for product in data_products:
            if product in product_map:
                if isinstance(product_map[product], str):
                    selected_products.add(product_map[product])
                else:
                    selected_products.update(product_map[product])
        return selected_products
