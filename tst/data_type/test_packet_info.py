import datetime as dt

from data_type.packet_info import PacketInfo


class TestPacketInfo:
    PACKET_ID = 5
    ID = 10
    IDPU_TIME = dt.datetime(2019, 5, 6, 7, 8, 9)
    COLLECTION_TIME = dt.datetime(2019, 6, 7, 8, 9, 10)

    ALT_PACKET_ID = 500
    ALT_ID = 1000
    ALT_IDPU_TIME = dt.datetime(2020, 8, 7, 6, 5, 4)
    ALT_COLLECTION_TIME = dt.datetime(2020, 9, 8, 7, 6, 5)

    def test_init(self):
        PacketInfo(self.PACKET_ID, self.ID, self.IDPU_TIME, self.COLLECTION_TIME)

    def test_eq(self):
        pi_1 = PacketInfo(self.PACKET_ID, self.ID, self.IDPU_TIME, self.COLLECTION_TIME)
        pi_2 = PacketInfo(self.PACKET_ID, self.ID, self.IDPU_TIME, self.COLLECTION_TIME)
        assert pi_1 == pi_2

        pi_3 = PacketInfo(self.ALT_PACKET_ID, self.ID, self.IDPU_TIME, self.COLLECTION_TIME)
        assert pi_1 != pi_3

        pi_4 = PacketInfo(self.PACKET_ID, self.ALT_ID, self.IDPU_TIME, self.COLLECTION_TIME)
        assert pi_1 != pi_4

        pi_5 = PacketInfo(self.PACKET_ID, self.ID, self.ALT_IDPU_TIME, self.COLLECTION_TIME)
        assert pi_1 != pi_5

        pi_6 = PacketInfo(self.PACKET_ID, self.ID, self.IDPU_TIME, self.ALT_COLLECTION_TIME)
        assert pi_1 != pi_6

    def test_hash(self):
        pi_1 = PacketInfo(self.PACKET_ID, self.ID, self.IDPU_TIME, self.COLLECTION_TIME)
        pi_2 = PacketInfo(self.PACKET_ID, self.ID, self.IDPU_TIME, self.COLLECTION_TIME)
        assert pi_1.__hash__() == pi_2.__hash__()

    def test_str(self):
        pi = PacketInfo(self.PACKET_ID, self.ID, self.IDPU_TIME, self.COLLECTION_TIME)
        assert isinstance(str(pi), str)

    def test_repr(self):
        pi = PacketInfo(self.PACKET_ID, self.ID, self.IDPU_TIME, self.COLLECTION_TIME)
        assert isinstance(pi.__repr__(), str)
        assert pi.__repr__() == str(pi)
