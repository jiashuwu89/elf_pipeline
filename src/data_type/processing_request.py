"""Defines a class to describe a processing job that must be performed."""

from util.constants import MISSION_DICT, SCIENCE_TYPES


class ProcessingRequest:
    """A class to create objects describing files to generate"""

    def __init__(self, mission_id, data_product, date):
        """Constructor for ProcessingRequest class.
        mission_id = 1, 2, 3
        data_product (SHOULD BE STRING)
        date = date for which to generate file (ex. 2020-08-05)
        """
        self.mission_id = mission_id
        self.data_product = data_product
        self.date = date  # Should be a DATE, not a DATETIME

    def __eq__(self, other):
        return (
            self.mission_id == other.mission_id and self.data_product == other.data_product and self.date == other.date
        )

    def __hash__(self):
        return hash((self.mission_id, self.data_product, self.date))

    def __str__(self):
        return f"ProcessingRequest(\
            mission_id={self.mission_id}, \
            data_product={self.data_product}, \
            date={self.date}"

    @property
    def probe(self):
        """Gives the probe (ex. ELA, ELB, EM3)"""
        return MISSION_DICT[self.mission_id]

    @property
    def idpu_types(self):
        return SCIENCE_TYPES.get(self.data_product, [])
