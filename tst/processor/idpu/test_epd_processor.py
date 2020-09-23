import datetime as dt
import filecmp
import os
import tempfile

import pytest
from elfin.common import db
from spacepy import pycdf

from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from dummy import SafeTestPipelineConfig
from processor.idpu.epd_processor import EpdProcessor
from util.constants import TEST_DATA_DIR


class TestEpdProcessor:
    @pytest.mark.skipif(not os.path.isfile("./src/util/credentials.py"), reason="Probably in CI/CD pipeline")
    def test_generate_files(self):
        pr = ProcessingRequest(1, "epdef", dt.date(2020, 4, 4))
        epd_processor = EpdProcessor(SafeTestPipelineConfig())
        generated_files = epd_processor.generate_files(pr)
        assert len(generated_files) == 2
        generated_l0_file, generated_l1_file = generated_files

        assert filecmp.cmp(generated_l0_file, f"{TEST_DATA_DIR}/pkt/ela_l0_epdef_20200404_344.pkt")

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ela_l1_epdef_20200404_v01.cdf")

        assert all([a == b for a, b in zip(new_cdf.keys(), expected_cdf.keys())]) is True

        # TODO: Test ela_pef_energies_max, ela_pef_energies_mean, ela_pef_energies_min
        # TODO: Spinphase is 0?
        # TODO: Explore ways to unify all of the CDF-comparing code

        for key in ["ela_pef_sectnum", "ela_pef_spinper", "ela_pef_time"]:
            for new_val, expected_val in zip(new_cdf[key][...], expected_cdf[key][...]):
                assert new_val == expected_val

        for new_row, expected_row in zip(new_cdf["ela_pef"][...], expected_cdf["ela_pef"][...]):
            for new_val, expected_val in zip(new_row, expected_row):
                assert new_val == expected_val
