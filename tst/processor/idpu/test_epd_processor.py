import datetime as dt
import filecmp
import os

import pytest
from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from processor.idpu.epd_processor import EpdProcessor
from util import general_utils
from util.constants import CREDENTIALS_FILE, TEST_DATA_DIR
from util.dummy import DUMMY_DOWNLINK_MANAGER, SafeTestPipelineConfig


class TestEpdProcessor:
    def test_generate_files(self):
        pr = ProcessingRequest(1, "epdef", dt.date(2020, 4, 4))
        epd_processor = EpdProcessor(SafeTestPipelineConfig(), DUMMY_DOWNLINK_MANAGER)
        generated_files = epd_processor.generate_files(pr)
        assert len(generated_files) == 2
        generated_l0_file, generated_l1_file = generated_files

        assert filecmp.cmp(generated_l0_file, f"{TEST_DATA_DIR}/pkt/ela_l0_epdef_20200404_344.pkt")

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ela_l1_epdef_20200404_v01.cdf")

        general_utils.compare_cdf(
            new_cdf, expected_cdf, ["ela_pef"], ["ela_pef_sectnum", "ela_pef_spinper", "ela_pef_time"], []
        )

        # TODO: Test ela_pef_energies_max, ela_pef_energies_mean, ela_pef_energies_min
        # TODO: Spinphase is 0?
