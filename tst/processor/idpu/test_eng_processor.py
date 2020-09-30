import datetime as dt
import os

import numpy as np
import pytest
from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from processor.idpu.eng_processor import EngProcessor
from util.constants import CREDENTIALS_FILE, TEST_DATA_DIR
from util.dummy import DUMMY_DOWNLINK_MANAGER, SafeTestPipelineConfig

# TODO: Eng data was processed incorrectly by original pipeline!!!!!


class TestEngProcessor:
    @pytest.mark.skipif(not os.path.isfile(CREDENTIALS_FILE), reason="Probably in CI/CD pipeline")
    def test_generate_files(self):
        eng_processor = EngProcessor(SafeTestPipelineConfig(), DUMMY_DOWNLINK_MANAGER)

        pr_1 = ProcessingRequest(1, "eng", dt.date(2020, 4, 23))
        generated_files = eng_processor.generate_files(pr_1)
        assert len(generated_files) == 1
        (generated_l1_file,) = generated_files

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ela_l1_eng_20200423_v01.cdf")

        assert all([a == b for a, b in zip(new_cdf.keys(), expected_cdf.keys())]) is True

        for key in new_cdf.keys():
            for new_val, expected_val in zip(new_cdf[key][...], expected_cdf[key][...]):
                try:
                    assert new_val == expected_val
                except AssertionError as e:
                    if np.isnan(new_val) and np.isnan(expected_val):
                        continue
                    raise AssertionError(
                        f"AssertionError {e} on key {key}"
                    )  # TODO: would be good to capture the AssertionError to provide more info in the other tests
