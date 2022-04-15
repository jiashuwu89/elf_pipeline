import datetime as dt

import pytest
from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from processor.idpu.eng_processor import EngProcessor
from util import general_utils
from util.constants import TEST_DATA_DIR
from util.dummy import DUMMY_DOWNLINK_MANAGER, SafeTestPipelineConfig

# TODO: Eng data was processed incorrectly by original pipeline!!!!!


class TestEngProcessor:
    eng_processor = EngProcessor(SafeTestPipelineConfig(), DUMMY_DOWNLINK_MANAGER)

    @pytest.mark.integration
    def test_generate_files(self):
        pr_1 = ProcessingRequest(1, "eng", dt.date(2020, 4, 23))
        generated_files = self.eng_processor.generate_files(pr_1)
        assert len(generated_files) == 1
        (generated_l1_file,) = generated_files

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ela_l1_eng_20200423_v01.cdf")

        general_utils.compare_cdf(new_cdf, expected_cdf, [], list(new_cdf.keys()), [])

    def test_generate_l0_df(self):
        # Should not raise any errors, even if no L0 data is found
        assert self.eng_processor.generate_l0_df(ProcessingRequest(1, "eng", dt.date(2018, 1, 1))).empty

        df = self.eng_processor.generate_l0_df(ProcessingRequest(1, "eng", dt.date(2018, 12, 4)))
        assert all(df["data"].apply(lambda x: x is None or type(x) == bytes))

    def test_transform_l0_df(self):
        idpu_time = dt.datetime(2022, 4, 14, 6, 51, 30)

        data_14 = bytes.fromhex("09a9015305300261079e04c9074904a100000000000000000000")
        assert self.eng_processor.extract_data(14, data_14, idpu_time) == {
            "sips_time": idpu_time,
            "sips_5v0_current": 609,
            "sips_5v0_voltage": 1950,
            "sips_input_current": 1225,
            "sips_input_temp": 1865,
            "sips_input_voltage": 1185,
        }

        data_15 = bytes.fromhex("045c07660be3")
        assert self.eng_processor.extract_data(15, data_15, idpu_time) == {
            "epd_time": idpu_time,
            "epd_biasl": 1116,
            "epd_biash": 1894,
            "epd_efe_temp": 3043,
        }

        data_16 = bytes.fromhex("003f003f003f00000000003f003f003f00810006004546490008000000000403")
        assert self.eng_processor.extract_data(16, data_16, idpu_time) == {
            "fgm_time": idpu_time,
            "fgm_8_volt": 63,
            "fgm_sh_temp": 0,
            "fgm_3_3_volt": 63,
            "fgm_analog_ground": 63,
            "fgm_eu_temp": 0,
        }

        # Bad IDPU type
        with pytest.raises(ValueError):
            self.eng_processor.extract_data(1, bytes(), idpu_time)

    def test_get_cdf_fields(self):
        prs = [
            ProcessingRequest(1, "eng", dt.date(2022, 4, 6)),
            ProcessingRequest(2, "eng", dt.date(2022, 4, 6)),
        ]

        for pr in prs:
            cdf = self.eng_processor.create_empty_cdf(self.eng_processor.make_filename(pr, 1))
            field_mapping = self.eng_processor.get_cdf_fields(pr)

            for field in field_mapping:
                assert field in cdf.keys()
