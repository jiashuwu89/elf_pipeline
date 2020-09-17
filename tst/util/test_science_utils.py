import datetime as dt

import numpy as np
import pytest
from spacepy import pycdf

from util import science_utils


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

    def test_twos_comp(self):
        assert science_utils.twos_comp(0, 24) == 0
        assert science_utils.twos_comp(1, 24) == 1
        assert science_utils.twos_comp(-1, 24) == -16777217  # TODO: Check this

    def test_hex_to_int(self):
        with pytest.raises(ValueError):
            assert science_utils.hex_to_int(50)

    def test_get_attribute_or_none(self):
        s = "BLAH"
        assert science_utils.get_attribute_or_none(s, "__doc__") == s.__doc__
        assert science_utils.get_attribute_or_none(s, "BLAH") is None

    def test_interpolate_attitude(self):
        # TODO: This test case should be beefed up
        # EXAMPLE FUNCTION CALL (using first two entries in ELA attitude file)
        Si = np.array([0.49483781545421507, 0.63977898197938476, 0.58806325392250303])
        Sf = np.array([0.55442686811141506, 0.55396341823980311, 0.62107598501974026])
        Ti = "2019-04-30/17:38:32"
        Tf = "2019-05-02/09:29:14"

        dt_init = dt.datetime.strptime(Ti, "%Y-%m-%d/%H:%M:%S")
        dt_fin = dt.datetime.strptime(Tf, "%Y-%m-%d/%H:%M:%S")

        interp_times, interp_atts = science_utils.interpolate_attitude(Si, dt_init, Sf, dt_fin)

        # print(interp_times, interp_atts)
        assert interp_times.shape == (2391,)
        assert interp_atts.shape == (2391, 3)
