"""Defines a class to describe a processing job that must be performed."""

import datetime as dt
from dataclasses import dataclass
from typing import List

from util import science_utils
from util.constants import MISSION_DICT


@dataclass(frozen=True, order=True)
class ProcessingRequest:
    """A class to create objects describing files to generate"""

    mission_id: int
    data_product: str
    date: dt.date

    @property
    def probe(self) -> str:
        """Gives the probe (ela, elb, or em3)"""
        return MISSION_DICT[self.mission_id]

    @property
    def idpu_types(self) -> List[int]:
        return science_utils.convert_data_product_to_idpu_types(self.data_product)
