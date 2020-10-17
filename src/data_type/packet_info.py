"""Definition of PacketInfo class"""
import datetime as dt


class PacketInfo:
    """Information about a single packet

    Attributes
    ----------
    science_packet_id: int
        id in the packets table??
    idpu_time: dt.DateTime
        IDPU Time
    collection_time: dt.DateTime
        Time at which the packet was collected
    denominator: int

    Methods
    -------
    """

    def __init__(self, science_packet_id: int, idpu_time: dt.datetime, collection_time: dt.datetime, denominator: int):
        # I've decided packet_id isn't useful enough to include here, but can be added later if necessary
        # self.packet_id = packet_id
        self.science_packet_id = science_packet_id
        self.idpu_time = idpu_time
        self.collection_time = collection_time
        self.denominator = denominator

    def __eq__(self, other) -> bool:
        return (
            self.science_packet_id == other.science_packet_id
            and self.idpu_time == other.idpu_time
            and self.collection_time == other.collection_time
            and self.denominator == other.denominator
        )

    def __hash__(self) -> int:
        return hash((self.science_packet_id, self.idpu_time, self.collection_time, self.denominator))

    def __str__(self) -> str:
        return (
            f"PacketInfo(science_packet_id={self.science_packet_id}, "
            + f"idpu_time={self.idpu_time}, "
            + f"collection_time={self.collection_time}, "
            + f"denom={self.denominator})"
        )

    def __repr__(self) -> str:
        return (
            f"PacketInfo(science_packet_id={self.science_packet_id}, "
            + f"idpu_time={self.idpu_time}, "
            + f"collection_time={self.collection_time}, "
            + f"denom={self.denominator})"
        )
