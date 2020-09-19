import datetime as dt

from data_type.downlink import Downlink
from data_type.packet_info import PacketInfo


class TestDownlink:
    # self, packet_id, id, idpu_time, collection_time):
    # mission_id, idpu_type, denominator, first_packet_info, last_packet_info
    PACKET_INFO_1 = PacketInfo(10, 20, dt.datetime(2019, 1, 1, 1, 1, 1), dt.datetime(2019, 2, 2, 2, 2, 2))
    PACKET_INFO_2 = PacketInfo(20, 40, dt.datetime(2019, 1, 1, 1, 1, 10), dt.datetime(2019, 2, 2, 2, 2, 20))

    def test_init(self):
        Downlink(1, 1, 1000, self.PACKET_INFO_1, self.PACKET_INFO_2)

    def test_eq(self):
        dl_1 = Downlink(1, 1, 1000, self.PACKET_INFO_1, self.PACKET_INFO_2)
        dl_2 = Downlink(
            1,
            1,
            1000,
            PacketInfo(10, 20, dt.datetime(2019, 1, 1, 1, 1, 1), dt.datetime(2019, 2, 2, 2, 2, 2)),
            PacketInfo(20, 40, dt.datetime(2019, 1, 1, 1, 1, 10), dt.datetime(2019, 2, 2, 2, 2, 20)),
        )
        assert dl_1 == dl_2

        dl_3 = Downlink(2, 1, 1000, self.PACKET_INFO_1, self.PACKET_INFO_2)
        assert dl_1 != dl_3

        dl_4 = Downlink(1, 2, 1000, self.PACKET_INFO_1, self.PACKET_INFO_2)
        assert dl_1 != dl_4

        dl_5 = Downlink(
            1,
            1,
            1000,
            PacketInfo(10, 20, dt.datetime(2019, 1, 1, 1, 10, 30), dt.datetime(2019, 2, 2, 2, 2, 2)),
            PacketInfo(20, 40, dt.datetime(2019, 1, 1, 1, 1, 10), dt.datetime(2019, 2, 2, 2, 2, 20)),
        )
        assert dl_1 != dl_5

        dl_6 = Downlink(
            1,
            1,
            1000,
            PacketInfo(10, 20, dt.datetime(2019, 1, 1, 1, 1, 1), dt.datetime(2019, 2, 2, 2, 2, 2)),
            PacketInfo(20, 40, dt.datetime(2019, 1, 1, 1, 1, 50), dt.datetime(2019, 2, 2, 2, 2, 50)),
        )
        assert dl_1 != dl_6

    def test_hash(self):
        dl_1 = Downlink(1, 1, 1000, self.PACKET_INFO_1, self.PACKET_INFO_2)
        dl_2 = Downlink(
            1,
            1,
            1000,
            PacketInfo(10, 20, dt.datetime(2019, 1, 1, 1, 1, 1), dt.datetime(2019, 2, 2, 2, 2, 2)),
            PacketInfo(20, 40, dt.datetime(2019, 1, 1, 1, 1, 10), dt.datetime(2019, 2, 2, 2, 2, 20)),
        )
        assert dl_1.__hash__() == dl_2.__hash__()

    def test_str(self):
        dl = Downlink(1, 1, 1000, self.PACKET_INFO_1, self.PACKET_INFO_2)
        assert isinstance(str(dl), str)

    def test_repr(self):
        dl = Downlink(1, 1, 1000, self.PACKET_INFO_1, self.PACKET_INFO_2)
        assert isinstance(dl.__repr__(), str)
        assert dl.__repr__() == str(dl)
