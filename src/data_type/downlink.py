class Downlink:
    def __init__(self, mission_id, idpu_type, first_packet_info, last_packet_info):
        self.mission_id = mission_id
        self.idpu_type = idpu_type

        if first_packet_info.denominator != last_packet_info.denominator:
            raise RuntimeError(
                f"Packet denominators differ: {first_packet_info.denominator} != {last_packet_info.denominator}"
            )
        self.first_packet_info = first_packet_info
        self.last_packet_info = last_packet_info

    @property
    def denominator(self):
        return self.first_packet_info.denominator

    def __eq__(self, other):
        return (
            self.mission_id == other.mission_id
            and self.idpu_type == other.idpu_type
            and self.denominator == other.denominator
            and self.first_packet_info == other.first_packet_info
            and self.last_packet_info == other.last_packet_info
        )

    def __lt__(self, other):
        self_tuple = (
            self.first_packet_info.idpu_time,
            self.last_packet_info.science_packet_id - self.first_packet_info.science_packet_id,
        )
        other_tuple = (
            other.first_packet_info.idpu_time,
            other.last_packet_info.science_packet_id - other.first_packet_info.science_packet_id,
        )

        return self_tuple < other_tuple

    def __hash__(self):
        return hash((self.mission_id, self.idpu_type, self.denominator, self.first_packet_info, self.last_packet_info))

    def __str__(self):
        return "\n".join(
            [
                "Downlink(",
                f"\tmission_id={self.mission_id}, idpu_type={self.idpu_type}, denominator={self.denominator}, ",
                f"\tFirst: {str(self.first_packet_info)}",
                f"\tSecond: {str(self.last_packet_info)})",
            ]
        )

    def __repr__(self):
        return "\n".join(
            [
                "Downlink(",
                f"\tmission_id={self.mission_id}, idpu_type={self.idpu_type}, denominator={self.denominator}, ",
                f"\tFirst: {str(self.first_packet_info)}",
                f"\tSecond: {str(self.last_packet_info)})",
            ]
        )
