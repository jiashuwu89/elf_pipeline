"""Definition of PacketInfo class"""


class PacketInfo:
    """Information about a single packet

    Attributes
    ----------
    packet_id: int
        id in the packets table??
    id: int
        id in the science_packets table??
    idpu_time: dt.DateTime
        IDPU Time
    collection_time: dt.DateTime
        Time at which the packet was collected

    Methods
    -------
    """

    def __init__(self, packet_id, science_packet_id, idpu_time, collection_time):
        self.packet_id = packet_id
        self.science_packet_id = science_packet_id  # TODO: ID in science_packets table, rename
        self.idpu_time = idpu_time
        self.collection_time = collection_time

    def __eq__(self, other):
        return (
            self.packet_id == other.packet_id
            and self.science_packet_id == other.science_packet_id
            and self.idpu_time == other.idpu_time
            and self.collection_time == other.collection_time
        )

    def __hash__(self):
        return hash((self.packet_id, self.science_packet_id, self.idpu_time, self.collection_time))

    def __str__(self):
        return (
            f"PacketInfo(packet_id={self.packet_id}, science_packet_id={self.science_packet_id}, "
            + f"idpu_time={self.idpu_time}, collection_time={self.collection_time})"
        )

    def __repr__(self):
        return (
            f"PacketInfo(packet_id={self.packet_id}, science_packet_id={self.science_packet_id}, "
            + f"idpu_time={self.idpu_time}, collection_time={self.collection_time})"
        )
