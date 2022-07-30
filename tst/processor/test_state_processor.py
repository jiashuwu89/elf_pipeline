import datetime as dt
import os

import pytest
from elfin.common import models
from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from processor.state_processor import StateProcessor
from util import general_utils
from util.constants import TEST_DATA_DIR
from util.dummy import DummyProcessingRequest, SafeTestPipelineConfig

# TODO: test_utils dir!


class TestStateProcessor:
    @pytest.mark.integration
    @pytest.mark.skip(reason="Updated mastercdfs from Cindy's email on 2021-11-16")
    def test_generate_files(self):
        state_processor = StateProcessor(SafeTestPipelineConfig())

        pr_1 = ProcessingRequest(2, "state-defn", dt.date(2020, 6, 13))
        generated_files = state_processor.generate_files(pr_1)
        assert len(generated_files) == 1
        (generated_l1_file,) = generated_files

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/elb_l1_state_defn_20200613_v01.cdf")

        general_utils.compare_cdf(
            new_cdf,
            expected_cdf,
            ["elb_att_gei", "elb_pos_gei", "elb_vel_gei"],
            ["elb_att_solution_date", "elb_att_uncertainty", "elb_state_time"],
            ["elb_spin_orbnorm_angle", "elb_spin_sun_angle", "elb_sun"],
        )

    def test_get_cdf_fields(self):
        state_processor = StateProcessor(SafeTestPipelineConfig())
        assert state_processor.get_cdf_fields(DummyProcessingRequest()) == {}

    def test_make_filename(self):
        # TODO: Make sure that SafeTestPipelineConfig output dir is deleted after running
        state_processor = StateProcessor(SafeTestPipelineConfig())

        test_cases = [
            (ProcessingRequest(1, "state-defn", dt.date(2022, 4, 5)), "ela_l1_state_defn_20220405_v02.cdf"),
            (ProcessingRequest(2, "state-pred", dt.date(2022, 4, 6)), "elb_l1_state_pred_20220406_v02.cdf"),
        ]

        for pr, expected_fname in test_cases:
            assert state_processor.make_filename(pr, 1) == os.path.join(state_processor.output_dir, expected_fname)

        # Only level 1 state files supported
        pr = ProcessingRequest(1, "state-defn", dt.date(2022, 4, 5))
        with pytest.raises(ValueError):
            state_processor.make_filename(pr, 0, 100)

        # Bad data product name
        pr = ProcessingRequest(1, "BAD_DATA_PRODUCT", dt.date(2022, 4, 5))
        with pytest.raises(ValueError):
            state_processor.make_filename(pr, 1)

    def test_get_q_dict(self):
        state_processor = StateProcessor(SafeTestPipelineConfig())
        calculated_attitude = models.CalculatedAttitude(
            X=1, Y=2, Z=3, uncertainty=4, time=dt.datetime(2022, 7, 29), rpm=20
        )
        q_dict = state_processor.get_q_dict(calculated_attitude)
        assert q_dict["solution_date_dt"] == pycdf.lib.tt2000_to_datetime(q_dict["solution_date_tt2000"])
        assert q_dict["spinper"] == 3

        # Calculated Attitude without RPM
        calculated_attitude = models.CalculatedAttitude(
            X=1, Y=2, Z=3, uncertainty=4, time=dt.datetime(2022, 7, 29), rpm=None
        )
        q_dict = state_processor.get_q_dict(calculated_attitude)
        assert q_dict["solution_date_dt"] == pycdf.lib.tt2000_to_datetime(q_dict["solution_date_tt2000"])
        assert q_dict["spinper"] is None
