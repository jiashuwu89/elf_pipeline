import datetime as dt
import filecmp

import pandas as pd
import pytest
from spacepy import pycdf

from data_type.processing_request import ProcessingRequest
from processor.idpu.epd_processor import EpdProcessor
from util import general_utils
from util.compression_values import EPD_HUFFMAN
from util.constants import TEST_DATA_DIR, VALID_NUM_SECTORS
from util.dummy import DUMMY_DOWNLINK_MANAGER, SafeTestPipelineConfig


# Exists because IBO-changes-era tests use non-existant data that does not work with Bogus-EPD testing
def temp_process_rejoined_data(EPD: EpdProcessor, processing_request: ProcessingRequest, df: pd.DataFrame):
    types = df["idpu_type"].values
    uncompressed = bool(set([3, 5, 22, 23]).intersection(types))
    compressed = bool(set([4, 6, 24]).intersection(types))
    survey = bool(set([19, 20]).intersection(types))
    inner = bool(set([22, 23, 24]).intersection(types))

    if uncompressed + compressed + survey > 1:
        raise ValueError("⚠️ Detected more than one kind of EPD data (uncompressed, compressed, survey).")

    if uncompressed:
        df = EPD.update_uncompressed_df(df)
    elif compressed:
        if inner:
            df = EPD.decompress_df(processing_request, df=df, num_sectors=16, table=EPD_HUFFMAN)
        else:
            df = EPD.decompress_df(processing_request, df=df, num_sectors=16, table=EPD_HUFFMAN)
    elif survey:
        df = EPD.decompress_df(processing_request, df=df, num_sectors=4, table=EPD_HUFFMAN)
    else:
        EPD.logger.warning("⚠️ Detected neither compressed nor uncompressed nor survey data.")

    return df


class TestEpdProcessor:
    @pytest.mark.integration
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
        new_cdf.close()
        expected_cdf.close()

    @pytest.mark.integration
    def test_em3_iepde_uncompressed_data(self):
        pc = SafeTestPipelineConfig()
        epd_processor = EpdProcessor(pc, DUMMY_DOWNLINK_MANAGER)

        pr = ProcessingRequest(
            1, "epdef", dt.date(2020, 12, 4)
        )  # NOTE: There are no CDFs set up for EM3, so pretending it's ELA data - IT'S NOT!
        iepde_df = pd.read_csv(f"{TEST_DATA_DIR}/csv/ibo/iepde_22_v3.csv")

        r_df = epd_processor.rejoin_data(pr, iepde_df)
        p_df = temp_process_rejoined_data(epd_processor, pr, r_df)
        # This should effectively be the final l0_df since there is only a single dataframe, no need to merge anything

        l0_fname, _ = epd_processor.generate_l0_file(pr, p_df)
        l1_fname, _ = epd_processor.generate_l1_products(pr, p_df)
        l1_cdf = pycdf.CDF(l1_fname)
        test_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ibo_cdf/uncompressed/em3_l1_epdef_20201204_v01.cdf")

        assert filecmp.cmp(
            l0_fname,
            f"{TEST_DATA_DIR}/pkt/ibo_pkt/uncompressed/em3_l0_epdef_20201204_31.pkt",
        )
        general_utils.compare_cdf(
            l1_cdf,
            test_cdf,
            ["ela_pef"],
            ["ela_pef_sectnum", "ela_pef_spinper", "ela_pef_time"],
            [],
        )
        l1_cdf.close()
        test_cdf.close()

    @pytest.mark.integration
    def test_em3_iepdi_uncompressed_data(self):
        pc = SafeTestPipelineConfig()
        epd_processor = EpdProcessor(pc, DUMMY_DOWNLINK_MANAGER)

        pr = ProcessingRequest(1, "epdif", dt.date(2020, 12, 4))
        iepdi_df = pd.read_csv(f"{TEST_DATA_DIR}/csv/ibo/iepdi_23_v3.csv")

        r_df = epd_processor.rejoin_data(pr, iepdi_df)
        p_df = temp_process_rejoined_data(epd_processor, pr, r_df)

        l0_fname, _ = epd_processor.generate_l0_file(pr, p_df)
        l1_fname, _ = epd_processor.generate_l1_products(pr, p_df)
        l1_cdf = pycdf.CDF(l1_fname)
        test_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ibo_cdf/uncompressed/em3_l1_epdif_20201204_v01.cdf")

        assert filecmp.cmp(
            l0_fname,
            f"{TEST_DATA_DIR}/pkt/ibo_pkt/uncompressed/em3_l0_epdif_20201204_14.pkt",
        )
        general_utils.compare_cdf(
            l1_cdf,
            test_cdf,
            ["ela_pif"],
            ["ela_pif_sectnum", "ela_pif_spinper", "ela_pif_time"],
            [],
        )
        l1_cdf.close()
        test_cdf.close()

    @pytest.mark.integration
    def test_em3_iepde_compressed_data(self):
        pc = SafeTestPipelineConfig()
        epd_processor = EpdProcessor(pc, DUMMY_DOWNLINK_MANAGER)

        pr = ProcessingRequest(1, "epdef", dt.date(2020, 12, 4))
        iepd_compressed_df = pd.read_csv(f"{TEST_DATA_DIR}/csv/ibo/iepd_compressed_24_v3.csv")

        r_df = epd_processor.rejoin_data(pr, iepd_compressed_df)
        p_df = temp_process_rejoined_data(epd_processor, pr, r_df)

        l0_fname, _ = epd_processor.generate_l0_file(pr, p_df)
        l1_fname, _ = epd_processor.generate_l1_products(pr, p_df)
        l1_cdf = pycdf.CDF(l1_fname)
        test_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ibo_cdf/compressed/em3_l1_epdef_20201204_v01.cdf")

        assert filecmp.cmp(
            l0_fname,
            f"{TEST_DATA_DIR}/pkt/ibo_pkt/compressed/em3_l0_epdef_20201204_31.pkt",
        )

        general_utils.compare_cdf(
            l1_cdf,
            test_cdf,
            ["ela_pef"],
            ["ela_pef_sectnum", "ela_pef_spinper", "ela_pef_time"],
            [],
        )
        l1_cdf.close()
        test_cdf.close()

    @pytest.mark.integration
    def test_em3_iepdi_compressed_data(self):
        pc = SafeTestPipelineConfig()
        epd_processor = EpdProcessor(pc, DUMMY_DOWNLINK_MANAGER)

        pr = ProcessingRequest(1, "epdef", dt.date(2020, 12, 4))
        iepd_compressed_df = pd.read_csv(f"{TEST_DATA_DIR}/csv/ibo/iepd_compressed_24_v3.csv")

        pr = ProcessingRequest(1, "epdif", dt.date(2020, 12, 4))
        r_df = epd_processor.rejoin_data(pr, iepd_compressed_df)
        p_df = temp_process_rejoined_data(epd_processor, pr, r_df)

        l0_fname, _ = epd_processor.generate_l0_file(pr, p_df)
        l1_fname, _ = epd_processor.generate_l1_products(pr, p_df)
        l1_cdf = pycdf.CDF(l1_fname)
        test_cdf = pycdf.CDF(f"{TEST_DATA_DIR}/cdf/ibo_cdf/compressed/em3_l1_epdif_20201204_v01.cdf")

        assert filecmp.cmp(
            l0_fname,
            f"{TEST_DATA_DIR}/pkt/ibo_pkt/compressed/em3_l0_epdif_20201204_14.pkt",
        )

        general_utils.compare_cdf(
            l1_cdf,
            test_cdf,
            ["ela_pif"],
            ["ela_pif_sectnum", "ela_pif_spinper", "ela_pif_time"],
            [],
        )

        l1_cdf.close()
        test_cdf.close()

    def test_calculate_center_times_for_period(self):
        pc = SafeTestPipelineConfig()
        epd_processor = EpdProcessor(pc, DUMMY_DOWNLINK_MANAGER)

        spin_period = 16 * 80  # Time for 1 spin
        time_captured = dt.timedelta(seconds=0)  # The time it was captured (Baseline/offset)
        num_sectors = 16  # Num of times each spin is split into
        data_type = 24  # IBO
        spin_integration_factor = 4  # Number of spins in a spin frame

        assert epd_processor.calculate_center_times_for_period(
            spin_period, time_captured, num_sectors, data_type, spin_integration_factor
        ) == [dt.timedelta(seconds=(0 + 1 * i + 0.5 + 24)) for i in range(16)]

        data_type = 3

        assert epd_processor.calculate_center_times_for_period(
            spin_period, time_captured, num_sectors, data_type, spin_integration_factor
        ) == [dt.timedelta(seconds=(0 + 1 * i + 0.5)) for i in range(16)]

    def test_find_lossy_idx(self):
        b = bytes.fromhex(
            "032203081534f84400e4ab59634b381607000000000000000000000f1208020000000000000000000000000603010000000000000"
            "000000000000000000000000000000000000000000000020200000000000000000001000000000402020000000000000000000000"
            "0000151f0d0701000000000000000000000064704f4012050000000000000000000061654b3c1504000000000000000000000e140"
            "c03000101000000000000000000050202000000000000000000000000000102000000000000000000000000000004010000000000"
            "00000000000000000100060200000100000000000000000000182b0d06010001000000000000000000646f5341180601000000000"
            "000000000"
        )
        assert EpdProcessor.find_lossy_idx(b, 10) == 1

    def test_get_sign(self):
        assert EpdProcessor.get_sign("001") == (+1, "1")
        assert EpdProcessor.get_sign("011") == (-1, "1")

        with pytest.raises(ValueError):
            EpdProcessor.get_sign("101")

        with pytest.raises(ValueError):
            EpdProcessor.get_sign("111")

    def test_get_sector_iterator(self):
        for num_sectors in VALID_NUM_SECTORS:
            assert len(EpdProcessor.get_sector_iterator(num_sectors)) == num_sectors

        with pytest.raises(ValueError):
            EpdProcessor.get_sector_iterator(10)

    def test_get_cdf_fields(self):
        epd_processor = EpdProcessor(SafeTestPipelineConfig(), DUMMY_DOWNLINK_MANAGER)
        # TODO: survey mode mastercdfs missing some fields like nsectors, nspinsinsum
        prs = [
            ProcessingRequest(1, "epdef", dt.date(2022, 4, 6)),
            # ProcessingRequest(1, "epdes", dt.date(2022, 4, 6)),
            ProcessingRequest(1, "epdif", dt.date(2022, 4, 6)),
            # ProcessingRequest(1, "epdis", dt.date(2022, 4, 6)),
            ProcessingRequest(2, "epdef", dt.date(2022, 4, 6)),
            # ProcessingRequest(2, "epdes", dt.date(2022, 4, 6)),
            ProcessingRequest(2, "epdif", dt.date(2022, 4, 6)),
            # ProcessingRequest(2, "epdis", dt.date(2022, 4, 6)),
        ]

        for pr in prs:
            cdf = epd_processor.create_empty_cdf(epd_processor.make_filename(pr, 1))
            field_mapping = epd_processor.get_cdf_fields(pr)

            for field in field_mapping:
                assert field in cdf.keys()
