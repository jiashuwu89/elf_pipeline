import datetime as dt

import pytest
from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from processor.mrm_processor import MrmProcessor
from util import general_utils
from util.constants import TEST_DATA_DIR
from util.dummy import SafeTestPipelineConfig

# TODO: test_utils dir!


class TestMrmProcessor:
    @pytest.mark.integration
    def test_generate_files(self):
        mrm_processor = MrmProcessor(SafeTestPipelineConfig())

        pr_1 = ProcessingRequest(2, "mrma", dt.date(2020, 4, 18))
        generated_files = mrm_processor.generate_files(pr_1)
        assert len(generated_files) == 1
        (generated_l1_file,) = generated_files

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/elb_l1_mrma_20200418_v01.cdf")

        general_utils.compare_cdf(new_cdf, expected_cdf, ["elb_mrma"], ["elb_mrma_time"], [])

        pr_2 = ProcessingRequest(2, "mrmi", dt.date(2019, 1, 1))
        assert mrm_processor.generate_files(pr_2) == []

        # Clearly, no data from year 1999
        pr_3 = ProcessingRequest(1, "mrmi", dt.date(1999, 1, 1))
        assert mrm_processor.generate_files(pr_3) == []
