"""Definition of PacketInfo class"""


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

    def __init__(self, science_packet_id, idpu_time, collection_time, denominator):
        # I've decided packet_id isn't useful enough to include here, but can be added later if necessary
        # self.packet_id = packet_id
        self.science_packet_id = science_packet_id
        self.idpu_time = idpu_time
        self.collection_time = collection_time
        self.denominator = denominator

    def __eq__(self, other):
        return (
            self.science_packet_id == other.science_packet_id
            and self.idpu_time == other.idpu_time
            and self.collection_time == other.collection_time
            and self.denominator == other.denominator
        )

    def __hash__(self):
        return hash((self.science_packet_id, self.idpu_time, self.collection_time, self.denominator))

    def __str__(self):
        return (
            f"PacketInfo(science_packet_id={self.science_packet_id}, "
            + f"idpu_time={self.idpu_time}, "
            + f"collection_time={self.collection_time}, "
            + f"denominator={self.denominator})"
        )

    def __repr__(self):
        return (
            f"PacketInfo(science_packet_id={self.science_packet_id}, "
            + f"idpu_time={self.idpu_time}, "
            + f"collection_time={self.collection_time}, "
            + f"denominator={self.denominator})"
        )
