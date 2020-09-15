from abc import ABC

from util.constants import SCIENCE_TYPES


class PipelineQuery(ABC):
    """A class to hold the query to the pipeline, what the user has requested

    Attributes
    ----------
    mission_ids: List[int]
        Mission IDs (ELA=1, ELB=2, EM3=3)
    data_products: List[str]
        Data products ("fgf", "epdef", "state", etc.)
    times
        Specifies if the times should be interpreted as downlink or collection
        times
    start_time: dt.datetime
        The earliest time for data to occur
    end_time: dt.datetime
        The time which all data must precede
    """

    @staticmethod
    def data_products_to_idpu_types(data_products):
        idpu_types = []
        for data_product in data_products:
            idpu_types += SCIENCE_TYPES.get(data_product, [])
        return idpu_types
