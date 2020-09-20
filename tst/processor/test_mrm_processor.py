import datetime as dt
import os
import tempfile

import pytest
from elfin.common import db
from spacepy import pycdf

from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from processor.mrm_processor import MrmProcessor

# TODO: test_utils dir!
# TODO: If SafeTestPipelineConfig is used more, then we should move it to a more general location
TEST_DATA_DIR = "/Users/jamesking/Desktop/elfin/OPS/science/refactor/tst/test_data"


class SafeTestPipelineConfig(PipelineConfig):
    def __init__(self):
        if db.SESSIONMAKER is None:
            db.connect("production")
        self._session = db.SESSIONMAKER()
        self._output_dir = tempfile.mkdtemp()

    @property
    def session(self):
        return self._session

    @property
    def update_db(self):
        return False

    @property
    def generate_files(self):
        return True

    @property
    def output_dir(self):
        return self._output_dir

    @property
    def upload(self):
        return False

    @property
    def email(self):
        return False


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

        for new_time, expected_time in zip(new_cdf["elb_mrma_time"][...], expected_cdf["elb_mrma_time"][...]):
            assert new_time == expected_time

        for new_row, expected_row in zip(new_cdf["elb_mrma"][...], expected_cdf["elb_mrma"][...]):
            for new_val, expected_val in zip(new_row, expected_row):
                assert new_val == expected_val

        pr_2 = ProcessingRequest(2, "mrmi", dt.date(2019, 1, 1))
        assert mrm_processor.generate_files(pr_2) == []
