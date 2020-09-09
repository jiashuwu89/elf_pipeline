"""Defines a class to describe a processing job that must be performed."""

from util.constants import MISSION_DICT


class ProcessingRequest:
    """A class to create objects describing files to generate"""

    def __init__(self, mission_id, data_product, date):
        """Constructor for ProcessingRequest class.
        mission_id = 1, 2, 3
        data_product
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

    def get_probe(self):
        """Gives the probe (ex. ELA, ELB, EM3)"""
        # TODO: Deprecate in favor of probe property
        return MISSION_DICT[self.mission_id]

    @property
    def probe(self):
        """Gives the probe (ex. ELA, ELB, EM3)"""
        return MISSION_DICT[self.mission_id]
