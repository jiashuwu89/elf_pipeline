class Downlink:
    def __init__(self, mission_id, idpu_type, denominator, first_packet_info, last_packet_info):
        self.mission_id = mission_id
        self.idpu_type = idpu_type
        self.denominator = denominator

        self.first_packet_info = first_packet_info
        self.last_packet_info = last_packet_info

    def __eq__(self, other):
        return (
            self.mission_id == other.mission_id
            and self.idpu_type == other.idpu_type
            and self.denominator == other.denominator
            and self.first_packet_info == other.first_packet_info
            and self.last_packet_info == other.last_packet_info
        )

    def __hash__(self):
        return hash((self.mission_id, self.idpu_type, self.denominator, self.first_packet_info, self.last_packet_info))

    def __str__(self):
        return f"Downlink:\n\
                \tmission_id={self.mission_id}, idpu_type={self.idpu_type}, denominator={self.denominator}\n\
                \tFirst: {str(self.first_packet_info)}\n\
                \tSecond: {str(self.last_packet_info)}"
