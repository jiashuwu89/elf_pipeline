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

    def __str__(self):
        return (
            "PipelineQuery(\n"
            + f"\tmission_ids={self.mission_ids},\n"
            + f"\tdata_products={self.data_products},\n"
            + f"\ttimes={self.times},\n"
            + f"\tstart_time={self.start_time},\n"
            + f"\tend_time={self.end_time}\n)"
        )

    def __repr__(self):
        return (
            "PipelineQuery(\n"
            + f"\tmission_ids={self.mission_ids},\n"
            + f"\tdata_products={self.data_products},\n"
            + f"\ttimes={self.times},\n"
            + f"\tstart_time={self.start_time},\n"
            + f"\tend_time={self.end_time}\n)"
        )
