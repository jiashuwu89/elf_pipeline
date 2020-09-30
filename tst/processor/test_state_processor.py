import datetime as dt
import os

import pytest
from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from processor.state_processor import StateProcessor
from util.constants import CREDENTIALS_FILE, TEST_DATA_DIR
from util.dummy import DummyProcessingRequest, SafeTestPipelineConfig

# TODO: test_utils dir!


class TestStateProcessor:
    @pytest.mark.skipif(not os.path.isfile(CREDENTIALS_FILE), reason="Probably in CI/CD pipeline")
    def test_generate_files(self):
        state_processor = StateProcessor(SafeTestPipelineConfig())

        pr_1 = ProcessingRequest(2, "mrma", dt.date(2020, 6, 13))
        generated_files = state_processor.generate_files(pr_1)
        assert len(generated_files) == 1
        (generated_l1_file,) = generated_files

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/elb_l1_state_defn_20200613_v01.cdf")

        assert all([a == b for a, b in zip(new_cdf.keys(), expected_cdf.keys())]) is True

        for key in ["elb_att_gei", "elb_pos_gei", "elb_vel_gei"]:
            for new_row, expected_row in zip(new_cdf[key][...], expected_cdf[key][...]):
                for new_val, expected_val in zip(new_row, expected_row):
                    assert new_val == expected_val

        # TODO: is wrong, most likely
        for key in ["elb_att_solution_date", "elb_att_uncertainty", "elb_state_time"]:
            for new_row, expected_row in zip(new_cdf[key][...], expected_cdf[key][...]):
                assert new_row == expected_row

        # allow some very small precision errors
        for key in ["elb_spin_orbnorm_angle", "elb_spin_sun_angle", "elb_sun"]:
            for new_row, expected_row in zip(new_cdf[key][...], expected_cdf[key][...]):
                assert abs(new_row - expected_row) < 1e-13

    def test_get_cdf_fields(self):
        state_processor = StateProcessor(SafeTestPipelineConfig())
        assert state_processor.get_cdf_fields(DummyProcessingRequest()) == {}
