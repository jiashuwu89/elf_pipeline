import datetime as dt
import os

import pytest
from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from dummy import SafeTestPipelineConfig
from processor.mrm_processor import MrmProcessor
from util.constants import TEST_DATA_DIR

# TODO: test_utils dir!


class TestMrmProcessor:
    @pytest.mark.skipif(not os.path.isfile("./src/util/credentials.py"), reason="Probably in CI/CD pipeline")
    def test_generate_files(self):
        mrm_processor = MrmProcessor(SafeTestPipelineConfig())

        pr_1 = ProcessingRequest(2, "mrma", dt.date(2020, 4, 18))
        generated_files = mrm_processor.generate_files(pr_1)
        assert len(generated_files) == 1
        (generated_l1_file,) = generated_files

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/elb_l1_mrma_20200418_v01.cdf")

        assert all([a == b for a, b in zip(new_cdf.keys(), expected_cdf.keys())]) is True

        for new_time, expected_time in zip(new_cdf["elb_mrma_time"][...], expected_cdf["elb_mrma_time"][...]):
            assert new_time == expected_time

        for new_row, expected_row in zip(new_cdf["elb_mrma"][...], expected_cdf["elb_mrma"][...]):
            for new_val, expected_val in zip(new_row, expected_row):
                assert new_val == expected_val

        pr_2 = ProcessingRequest(2, "mrmi", dt.date(2019, 1, 1))
        assert mrm_processor.generate_files(pr_2) == []

        # Clearly, no data from year 1999
        pr_3 = ProcessingRequest(1, "mrmi", dt.date(1999, 1, 1))
        assert mrm_processor.generate_files(pr_3) == []
