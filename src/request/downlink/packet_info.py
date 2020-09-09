class PacketInfo:
    def __init__(self, packet_id, id, idpu_time, collection_time):
        self.packet_id = packet_id
        self.id = id  # ID in science_packets table
        self.idpu_time = idpu_time
        self.collection_time = collection_time

    def __eq__(self, other):
        return (
            self.packet_id == other.packet_id
            and self.id == other.id
            and self.idpu_time == other.idpu_time
            and self.collection_time == other.collection_time
        )

    def __hash__(self):
        return hash((self.packet_id, self.id, self.idpu_time, self.collection_time))

    def to_string(self):
        return f"PacketInfo(packet_id={self.packet_id}, id={self.id}, \
                idpu_time={self.idpu_time}, collection_time={self.collection_time})"
