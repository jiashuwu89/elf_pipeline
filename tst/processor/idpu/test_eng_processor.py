import datetime as dt

from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from processor.idpu.eng_processor import EngProcessor
from util import general_utils
from util.constants import TEST_DATA_DIR
from util.dummy import DUMMY_DOWNLINK_MANAGER, SafeTestPipelineConfig

# TODO: Eng data was processed incorrectly by original pipeline!!!!!


class TestEngProcessor:
    def test_generate_files(self):
        eng_processor = EngProcessor(SafeTestPipelineConfig(), DUMMY_DOWNLINK_MANAGER)

        pr_1 = ProcessingRequest(1, "eng", dt.date(2020, 4, 23))
        generated_files = eng_processor.generate_files(pr_1)
        assert len(generated_files) == 1
        (generated_l1_file,) = generated_files

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ela_l1_eng_20200423_v01.cdf")

        general_utils.compare_cdf(new_cdf, expected_cdf, [], list(new_cdf.keys()), [])
