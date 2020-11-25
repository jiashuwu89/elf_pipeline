import argparse
import datetime as dt
import logging
from abc import ABC, abstractmethod
from typing import List, Tuple

from dateutil.parser import parse as dateparser

from data_type.time_type import TimeType
from util.constants import ALL_MISSIONS, SCIENCE_TYPES


class PipelineQuery(ABC):
    """A class to hold the query to the pipeline; what the user requested"""

    @property
    @abstractmethod
    def mission_ids(self) -> List[int]:
        """List of mission IDs with the mapping: ELA=1 ELB=2 EM3=3"""
        raise NotImplementedError

    @property
    @abstractmethod
    def data_products(self) -> List[str]:
        """List of data products ("fgf", "epdef", "state", etc.)"""
        raise NotImplementedError

    @property
    @abstractmethod
    def times(self) -> TimeType:
        """Specifies if times are downlink or collection times"""
        raise NotImplementedError

    @property
    @abstractmethod
    def start_time(self) -> dt.datetime:
        """The earliest time for data to occur"""
        raise NotImplementedError

    @property
    @abstractmethod
    def end_time(self) -> dt.datetime:
        """The time which all data must precede"""
        raise NotImplementedError

    @staticmethod
    def data_products_to_idpu_types(data_products: List[str]) -> List[str]:
        idpu_types = []
        for data_product in data_products:
            idpu_types += SCIENCE_TYPES.get(data_product, [])
        return idpu_types

    def __str__(self) -> str:
        return (
            "PipelineQuery(\n"
            + f"\tmission_ids={self.mission_ids},\n"
            + f"\tdata_products={self.data_products},\n"
            + f"\ttimes={self.times},\n"
            + f"\tstart_time={self.start_time},\n"
            + f"\tend_time={self.end_time}\n)"
        )

    def __repr__(self) -> str:
        return (
            "PipelineQuery(\n"
            + f"\tmission_ids={self.mission_ids},\n"
            + f"\tdata_products={self.data_products},\n"
            + f"\ttimes={self.times},\n"
            + f"\tstart_time={self.start_time},\n"
            + f"\tend_time={self.end_time}\n)"
        )


class ParameterizedPipelineQuery(PipelineQuery):
    """A PipelineQuery that can be initialized with parameters.

    This class exists to help with testing the various request getters.

    Parameters
    ----------
    mission_ids : List[int]
    data_products : List[str]
    time_tuple : Tuple[dt.datetime, dt.datetime, TimeType]
        A tuple to describe the time values of the query. The first two items
        are the start and end times of the query, and the final value is a
        TimeType to specify how the times should be interpreted.
    """

    def __init__(
        self, mission_ids: List[int], data_products: List[str], time_tuple: Tuple[dt.datetime, dt.datetime, TimeType]
    ):
        self._mission_ids = mission_ids
        self._data_products = data_products
        self._start_time, self._end_time, self._times = time_tuple

    @property
    def mission_ids(self) -> List[int]:
        """List of mission IDs with the mapping: ELA=1 ELB=2 EM3=3"""
        return self._mission_ids

    @property
    def data_products(self) -> List[str]:
        """List of data products ("fgf", "epdef", "state", etc.)"""
        return self._data_products

    @property
    def times(self) -> TimeType:
        """Specifies if times are downlink or collection times"""
        return self._times

    @property
    def start_time(self) -> dt.datetime:
        """The earliest time for data to occur"""
        return self._start_time

    @property
    def end_time(self) -> dt.datetime:
        """The time which all data must precede"""
        return self._end_time


class ArgparsePipelineQuery(PipelineQuery):
    def __init__(self, args: argparse.Namespace):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(args.__dict__)

        self.args = args

        self._mission_ids = self.get_mission_ids(args.ela, args.elb, args.em3)
        self._data_products = self.get_data_products(args.products)
        self._times = self.get_times(args.select_downlinks_by_collection_time)
        self._start_time, self._end_time = self.validate_time(args.start_time, args.end_time)

    @property
    def mission_ids(self) -> List[int]:
        return self._mission_ids

    @property
    def data_products(self) -> List[str]:
        return self._data_products

    @property
    def times(self) -> TimeType:
        return self._times

    @property
    def start_time(self) -> dt.datetime:
        return self._start_time

    @property
    def end_time(self) -> dt.datetime:
        return self._end_time

    @staticmethod
    def get_mission_ids(ela: bool, elb: bool, em3: bool) -> List[int]:
        """Determine which missions to process, defaulting to ELA and ELB only"""
        mission_ids = []

        if ela:
            mission_ids.append(1)
        if elb:
            mission_ids.append(2)
        if em3:
            mission_ids.append(3)
        if len(mission_ids) == 0:
            mission_ids = ALL_MISSIONS.copy()

        return mission_ids

    @staticmethod
    def get_data_products(products: List[str]) -> List[str]:
        if not products:
            raise ValueError("Products should not be null!")
        return products

    @staticmethod
    def get_times(collection: bool) -> str:
        return TimeType.COLLECTION if collection else TimeType.DOWNLINK

    @staticmethod
    def validate_time(start_time: str, end_time: str) -> Tuple[dt.datetime, dt.datetime]:
        start_time = dateparser(start_time, tzinfos=0)
        end_time = dateparser(end_time, tzinfos=0)

        if start_time >= end_time:
            raise RuntimeError(f"Start time {start_time} should be earlier than end time {end_time}")

        return start_time, end_time
