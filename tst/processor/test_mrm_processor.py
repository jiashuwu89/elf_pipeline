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
    mrm_processor = MrmProcessor(SafeTestPipelineConfig())

    @pytest.mark.integration
    def test_generate_files(self):
        pr_1 = ProcessingRequest(2, "mrma", dt.date(2020, 4, 18))
        generated_files = self.mrm_processor.generate_files(pr_1)
        assert len(generated_files) == 1
        (generated_l1_file,) = generated_files

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/elb_l1_mrma_20200418_v01.cdf")

        general_utils.compare_cdf(new_cdf, expected_cdf, ["elb_mrma"], ["elb_mrma_time"], [])

        # ELB IDPU MRM should be skipped
        pr_2 = ProcessingRequest(2, "mrmi", dt.date(2019, 1, 1))
        assert self.mrm_processor.generate_files(pr_2) == []

        # Clearly, no data from year 1999
        pr_3 = ProcessingRequest(1, "mrmi", dt.date(1999, 1, 1))
        assert self.mrm_processor.generate_files(pr_3) == []

    def test_get_mrm_df(self):
        mrm_df = self.mrm_processor.get_mrm_df(ProcessingRequest(2, "mrma", dt.date(2020, 4, 18)))
        assert set(mrm_df.columns) == {"timestamp", "timestamp_tt2000", "mrm"}
        assert all(mrm_df["timestamp"].apply(pycdf.lib.datetime_to_tt2000) == mrm_df["timestamp_tt2000"])
        assert all(mrm_df["mrm"].apply(lambda x: len(x) == 3))

    def test_get_cdf_fields(self):
        prs = [
            ProcessingRequest(1, "mrma", dt.date(2022, 4, 6)),
            ProcessingRequest(1, "mrmi", dt.date(2022, 4, 6)),
            ProcessingRequest(2, "mrma", dt.date(2022, 4, 6)),
            ProcessingRequest(2, "mrmi", dt.date(2022, 4, 6)),
        ]

        for pr in prs:
            cdf = self.mrm_processor.create_empty_cdf(self.mrm_processor.make_filename(pr, 1))
            field_mapping = self.mrm_processor.get_cdf_fields(pr)

            for field in field_mapping:
                assert field in cdf.keys()
