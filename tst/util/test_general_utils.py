import datetime as dt

from util.general_utils import convert_date_to_datetime


class TestGeneralUtils:
    def test_convert_date_to_datetime(self):
        assert isinstance(convert_date_to_datetime(dt.date(2020, 1, 2)), dt.datetime)
        assert convert_date_to_datetime(dt.date(2020, 1, 2)) == dt.datetime(2020, 1, 2)
