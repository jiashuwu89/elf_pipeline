import datetime as dt
import filecmp
import os

import pandas as pd
import pytest
from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from processor.idpu.fgm_processor import FgmProcessor
from util.constants import CREDENTIALS_FILE, TEST_DATA_DIR
from util.dummy import DUMMY_DOWNLINK_MANAGER, SafeTestPipelineConfig


class TestFgmProcessor:

    fgm_processor = FgmProcessor(SafeTestPipelineConfig(), DUMMY_DOWNLINK_MANAGER)

    # TODO: Rename to generate_products?
    # TODO: Check old FGM files. It seems that some of their data does not fall under the
    # correct file (out of the range of the day)
    @pytest.mark.skipif(not os.path.isfile(CREDENTIALS_FILE), reason="Probably in CI/CD pipeline")
    def test_generate_files(self):
        pr_1 = ProcessingRequest(1, "fgs", dt.date(2020, 7, 1))
        generated_files = self.fgm_processor.generate_files(pr_1)
        assert len(generated_files) == 2
        generated_l0_file, generated_l1_file = generated_files

        assert filecmp.cmp(generated_l0_file, f"{TEST_DATA_DIR}/pkt/ela_l0_fgs_20200701_21137.pkt")

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ela_l1_fgs_20200701_v01.cdf")

        assert all([a == b for a, b in zip(new_cdf.keys(), expected_cdf.keys())]) is True

        for new_time, expected_time in zip(new_cdf["ela_fgs_time"][...], expected_cdf["ela_fgs_time"][...]):
            assert new_time == expected_time

        for new_row, expected_row in zip(new_cdf["ela_fgs"][...], expected_cdf["ela_fgs"][...]):
            for new_val, expected_val in zip(new_row, expected_row):
                assert new_val == expected_val

        # Clearly, no data from year 1999
        pr_2 = ProcessingRequest(1, "fgs", dt.date(1999, 1, 1))
        with pytest.raises(RuntimeError):
            self.fgm_processor.generate_files(pr_2)

    # def test_generate_l0_products(self):
    #     pr_1 = ProcessingRequest(1, "fgs", dt.date(2020, 3, 7))
    #     pr_2 = ProcessingRequest(1, "fgs", dt.date(2019, 8, 30))

    #     self.fgm_processor = FgmProcessor(SafeTestPipelineConfig())
    #     self.fgm_processor.

    def test_get_merged_dataframes(self):
        with pytest.raises(RuntimeError):
            self.fgm_processor.get_merged_dataframes([])

    def test_drop_packets_by_freq(self):
        pr = ProcessingRequest(1, "state", dt.date(2020, 7, 1))
        with pytest.raises(ValueError):
            self.fgm_processor.drop_packets_by_freq(pr, pd.DataFrame())

    def test_get_completeness_updater(self):
        pr = ProcessingRequest(1, "fgs", dt.date(2020, 7, 1))
        completeness_updater = self.fgm_processor.get_completeness_updater(pr)
        assert completeness_updater is not None
