import datetime as dt

from spacepy import pycdf

from src.util import science_utils


class TestScienceUtils:
    def test_dt_to_tt2000(self):
        assert science_utils.dt_to_tt2000(None) is None

        sample_dt = dt.datetime(2020, 1, 2, 3, 4, 5)
        assert sample_dt == pycdf.lib.tt2000_to_datetime(science_utils.dt_to_tt2000(sample_dt))

    def test_s_if_plural(self):
        sample_list = [5]
        assert science_utils.s_if_plural(sample_list) == ""

        sample_list = [1, 5, 2]
        assert science_utils.s_if_plural(sample_list) == "s"

        sample_list = []
        assert science_utils.s_if_plural(sample_list) == "s"
