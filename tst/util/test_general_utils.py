import atexit
import datetime as dt
import os

from util import general_utils


class TestGeneralUtils:
    def test_convert_date_to_datetime(self):
        assert isinstance(general_utils.convert_date_to_datetime(dt.date(2020, 1, 2)), dt.datetime)
        assert general_utils.convert_date_to_datetime(dt.date(2020, 1, 2)) == dt.datetime(2020, 1, 2)

    def test_calculate_file_md5sum(self):
        fname_1 = "tst/test_data/cdf/ela_l1_epdef_20200404_v01.cdf"
        assert general_utils.calculate_file_md5sum(fname_1) == "08a87dbef01519e6bf502bf0e5f4219e"

        fname_2 = "tst/test_data/cdf/elb_l1_state_defn_20200613_v01.cdf"
        assert general_utils.calculate_file_md5sum(fname_2) == "9cde058e6c6c9e967728409dd56c8377"

    def test_tmpdir(self):
        dname = general_utils.tmpdir()

        assert os.path.isdir(dname)

        # Tests that the directory is actually cleaned up, but avoiding for now
        # atexit._run_exitfuncs()
        # assert os.path.exists(dname) is False
        # assert os.path.isdir(dname) is False
