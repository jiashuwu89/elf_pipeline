import datetime as dt

from spacepy import pycdf

from src.util import science_utils


class Test_dt_to_tt2000:
    def test_one(self):
        assert science_utils.dt_to_tt2000(None) is None

    def test_two(self):
        d = dt.datetime(2020, 1, 2, 3, 4, 5)
        assert d == pycdf.lib.tt2000_to_datetime(science_utils.dt_to_tt2000(d))


class Test_s_if_plural:
    def test_one(self):
        l = [5]
        assert science_utils.s_if_plural(l) == ""

    def test_two(self):
        l = [1, 5, 2]
        assert science_utils.s_if_plural(l) == "s"
