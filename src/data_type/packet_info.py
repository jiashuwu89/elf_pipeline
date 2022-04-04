"""Definition of PacketInfo class"""
import datetime as dt
from dataclasses import dataclass


@dataclass(frozen=True)
class PacketInfo:
    """Information about a single packet

    Attributes
    ----------
    idpu_time: dt.DateTime
        IDPU Time
    collection_time: dt.DateTime
        Time at which the packet was collected
    denominator: int

    Methods
    -------
    """

    science_packet_id: int
    idpu_time: dt.datetime
    collection_time: dt.datetime
    denominator: int
