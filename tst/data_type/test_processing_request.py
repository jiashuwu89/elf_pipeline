import datetime as dt

from data_type.processing_request import ProcessingRequest


class TestProcessingRequest:
    MISSION_ID = 2
    DATA_PRODUCT = "epdef"
    DATE = dt.date(2020, 9, 15)

    ALT_MISSION_ID = 1
    ALT_DATA_PRODUCT = "state"
    ALT_DATE = dt.date(2019, 5, 2)

    EM3 = 3

    def test_init(self):
        ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)

    def test_eq(self):
        pr_1 = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        pr_2 = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert pr_1 == pr_2

        pr_3 = ProcessingRequest(self.ALT_MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert pr_1 != pr_3

        pr_4 = ProcessingRequest(self.MISSION_ID, self.ALT_DATA_PRODUCT, self.DATE)
        assert pr_1 != pr_4

        pr_5 = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.ALT_DATE)
        assert pr_1 != pr_5

    def test_lt(self):
        pr_1 = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        pr_2 = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert (pr_1 < pr_2) is False

        pr_3 = ProcessingRequest(self.ALT_MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert (pr_1 < pr_3) is False
        assert (pr_1 > pr_3) is True

    def test_hash(self):
        pr_1 = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        pr_2 = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert pr_1.__hash__() == pr_2.__hash__()

    def test_str(self):
        pr = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert isinstance(str(pr), str)

    def test_repr(self):
        pr = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert isinstance(pr.__repr__(), str)
        assert pr.__str__() == pr.__repr__()

    def test_probe(self):
        pr_1 = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert pr_1.probe == "elb"

        pr_2 = ProcessingRequest(self.ALT_MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert pr_2.probe == "ela"

        pr_3 = ProcessingRequest(self.EM3, self.DATA_PRODUCT, self.DATE)
        assert pr_3.probe == "em3"

    def test_idpu_types(self):
        pr_1 = ProcessingRequest(self.MISSION_ID, self.DATA_PRODUCT, self.DATE)
        assert pr_1.idpu_types == [3, 4]

        pr_2 = ProcessingRequest(self.MISSION_ID, self.ALT_DATA_PRODUCT, self.DATE)
        assert pr_2.idpu_types == []
