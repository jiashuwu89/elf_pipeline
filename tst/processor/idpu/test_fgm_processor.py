"""Tests for FgmProcessor and IdpuProcessor"""
import datetime as dt
import filecmp

import pandas as pd
import pytest
from spacepy import pycdf

from data_type.downlink import Downlink
from data_type.packet_info import PacketInfo
from data_type.processing_request import ProcessingRequest
from processor.idpu.fgm_processor import FgmFrequencyEnum, FgmProcessor
from util import general_utils
from util.constants import TEST_DATA_DIR
from util.dummy import DUMMY_DOWNLINK_MANAGER, SafeTestPipelineConfig


class TestFgmProcessor:

    fgm_processor = FgmProcessor(SafeTestPipelineConfig(), DUMMY_DOWNLINK_MANAGER)

    # TODO: Check old FGM files. It seems that some of their data does not fall under the
    # correct file (out of the range of the day)
    @pytest.mark.integration
    def test_generate_files(self):
        pr_1 = ProcessingRequest(1, "fgs", dt.date(2019, 7, 21))
        generated_files = self.fgm_processor.generate_files(pr_1)
        assert len(generated_files) == 2
        generated_l0_file, generated_l1_file = generated_files

        assert filecmp.cmp(generated_l0_file, f"{TEST_DATA_DIR}/pkt/ela_l0_fgs_20190721_6976.pkt")

        new_cdf = pycdf.CDF(generated_l1_file)
        expected_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ela_l1_fgs_20190721_v01.cdf")

        general_utils.compare_cdf(new_cdf, expected_cdf, ["ela_fgs"], ["ela_fgs_time"], [])

        # Clearly, no data from year 1999
        pr_2 = ProcessingRequest(1, "fgs", dt.date(1999, 1, 1))
        with pytest.raises(RuntimeError):
            self.fgm_processor.generate_files(pr_2)

    # def test_generate_l0_products(self):
    #     pr_1 = ProcessingRequest(1, "fgs", dt.date(2020, 3, 7))
    #     pr_2 = ProcessingRequest(1, "fgs", dt.date(2019, 8, 30))

    #     self.fgm_processor = FgmProcessor(SafeTestPipelineConfig())
    #     self.fgm_processor.

    def test_get_merged_dataframes(self):
        with pytest.raises(RuntimeError):
            self.fgm_processor.get_merged_dataframes([])

        packet_info = PacketInfo(1, dt.datetime(2022, 4, 1), dt.datetime(2022, 3, 31), 10)

        # Different mission IDs
        downlinks_with_different_mission_ids = [
            Downlink(
                1,
                2,
                packet_info,
                packet_info,
            ),
            Downlink(
                2,
                2,
                packet_info,
                packet_info,
            ),
        ]
        with pytest.raises(ValueError):
            self.fgm_processor.get_merged_dataframes(downlinks_with_different_mission_ids)

    def test_get_merged_dataframes_from_grouped_downlinks(self):

        packet_info = PacketInfo(1, dt.datetime(2022, 4, 1), dt.datetime(2022, 3, 31), 10)

        with pytest.raises(RuntimeError):
            self.fgm_processor._get_merged_dataframes_from_grouped_downlinks([])

        # Different mission IDs
        downlinks_with_different_mission_ids = [
            Downlink(
                1,
                2,
                packet_info,
                packet_info,
            ),
            Downlink(
                2,
                2,
                packet_info,
                packet_info,
            ),
        ]
        with pytest.raises(ValueError):
            self.fgm_processor._get_merged_dataframes_from_grouped_downlinks(downlinks_with_different_mission_ids)

        # Different IDPU types
        downlinks_with_different_idpu_types = [
            Downlink(
                1,
                2,
                packet_info,
                packet_info,
            ),
            Downlink(
                1,
                4,
                packet_info,
                packet_info,
            ),
        ]
        with pytest.raises(ValueError):
            self.fgm_processor._get_merged_dataframes_from_grouped_downlinks(downlinks_with_different_idpu_types)

    def test_drop_packets_by_freq(self):
        pr = ProcessingRequest(1, "state", dt.date(2020, 7, 1))
        with pytest.raises(ValueError):
            self.fgm_processor.drop_packets_by_freq(pr, pd.DataFrame())

        df = pd.DataFrame(
            {
                "10hz_mode": [FgmFrequencyEnum.TEN_HERTZ] * 5
                + [FgmFrequencyEnum.EIGHTY_HERTZ] * 5
                + [FgmFrequencyEnum.UNKNOWN] * 5
            }
        )

        pr = ProcessingRequest(1, "fgf", dt.date(2020, 7, 1))
        assert self.fgm_processor.drop_packets_by_freq(pr, df).index.tolist() == [5, 6, 7, 8, 9]

        pr = ProcessingRequest(1, "fgs", dt.date(2020, 7, 1))
        assert self.fgm_processor.drop_packets_by_freq(pr, df).index.tolist() == [0, 1, 2, 3, 4]

    def test_create_new_packet(self):
        # TODO: Test cases are examples pulled directly from the following ProcessingRequest, could be more rigorous:
        # ProcessingRequest(mission_id=1, data_product='fgf', date=datetime.date(2022, 3, 8))
        test_cases = [
            (([274904, -452140, 4422691], 25, 28937), "0431d8f919d4437c230000000019710900"),
            (([425272, 363060, 4646467], 26, 28937), "067d38058a3446e643000000001a710900"),
            (([576120, 1149428, 4640515], 27, 28937), "08ca781189f446cf03000000001b710900"),
        ]
        for ((axes, numerator, denominator), expected) in test_cases:
            assert self.fgm_processor.create_new_packet(axes, numerator, denominator) == expected

    def test_packets_in_compressed_packet(self):
        test_cases = [
            (dt.datetime(2018, 12, 1), 10),
            (dt.datetime(2019, 12, 1), 10),
            (dt.datetime(2020, 12, 1), 10),
            (dt.datetime(2021, 12, 1), 25),
        ]
        for idpu_time, expected in test_cases:
            assert self.fgm_processor.packets_in_compressed_packet(idpu_time) == expected

    def test_check_sampling_rate(self):
        test_cases = []
        for multiplier in range(1, 10):
            test_cases.extend(
                [
                    ((dt.timedelta(microseconds=1 / 80 * 1e6) * multiplier, multiplier), FgmFrequencyEnum.EIGHTY_HERTZ),
                    ((dt.timedelta(microseconds=1 / 10 * 1e6) * multiplier, multiplier), FgmFrequencyEnum.TEN_HERTZ),
                    ((dt.timedelta(microseconds=1 / 2 * 1e6) * multiplier, multiplier), FgmFrequencyEnum.UNKNOWN),
                ]
            )

        for ((time_gap, multiplier), expected) in test_cases:
            assert self.fgm_processor.check_sampling_rate(time_gap, multiplier) == expected

    def test_get_cdf_fields(self):
        prs = [
            ProcessingRequest(1, "fgs", dt.date(2022, 4, 6)),
            ProcessingRequest(1, "fgf", dt.date(2022, 4, 6)),
            ProcessingRequest(2, "fgs", dt.date(2022, 4, 6)),
            ProcessingRequest(2, "fgf", dt.date(2022, 4, 6)),
        ]

        for pr in prs:
            cdf = self.fgm_processor.create_empty_cdf(self.fgm_processor.make_filename(pr, 1))
            field_mapping = self.fgm_processor.get_cdf_fields(pr)

            for field in field_mapping:
                assert field in cdf.keys()
