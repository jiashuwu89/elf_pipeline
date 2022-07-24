import datetime as dt
from typing import Dict, List, Tuple

import pandas as pd
from elfin.common import models
from intervaltree import IntervalTree
from spacepy import pycdf
from sqlalchemy import func, text
from sqlalchemy.orm import Query

from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from output.downlink.downlink_manager import DownlinkManager
from processor.idpu.idpu_processor import IdpuProcessor
from util import byte_tools
from util.compression_values import EPD_HUFFMAN, EPD_LOSSY_VALS
from util.constants import (
    BAD_EPD_DATA_RANGES,
    BIN_COUNT,
    BOGUS_EPD_DATERANGE,
    EPD_CALIBRATION_DIR,
    EPD_PRODUCTS,
    IBO_DATA_BYTE,
    IBO_TYPES,
    INSTRUMENT_CLK_FACTOR,
    SURVEY_TYPES,
    VALID_NUM_SECTORS,
)
from util.science_utils import handle_adjacent_sectors

# EPD_ENERGIES = [[50., 70., 110., 160., 210., 270., 345., 430., 630., 900., 1300., 1800., 2500., 3000., 3850., 4500.]]


class EpdProcessor(IdpuProcessor):
    """This class processes level 0/1 epdef, epdes, epdif, and epdis data.

    The EpdProcessor class handles several things, including:
        * Decompression of epd
        * Filtering of data based on collection frequency

    Parameters
    ----------
    pipeline_config : PipelineConfig
    downlink_manager : DownlinkManager
    """

    def __init__(self, pipeline_config: PipelineConfig, downlink_manager: DownlinkManager):
        super().__init__(pipeline_config, downlink_manager)

    def process_rejoined_data(self, processing_request: ProcessingRequest, df: pd.DataFrame) -> pd.DataFrame:
        """Provided a DataFrame of rejoined data, perform needed processing.

        This method expects that all data is of the same IDPU type, but
        performs checking, just in case. This is an IMPORTANT invariant!

        NOTE: Both regularly-compressed data and survey data must be
        decompressed - the only difference is the number of sectors.

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame

        Returns
        -------
        pd.DataFrame
            A DataFrame of processed data
        """
        # TODO: unify with fgm_processor
        types = df["idpu_type"].values
        # TODO: Update to use mapping of IDPU types to labels (each Dataframe has one IDPU type)
        uncompressed = bool(set([3, 5, 7, 22, 23]).intersection(types))
        compressed = bool(set([4, 6, 8, 24]).intersection(types))
        survey = bool(set(SURVEY_TYPES).intersection(types))
        inner = bool(set(IBO_TYPES).intersection(types))

        if uncompressed + compressed + survey > 1:
            raise ValueError("⚠️ Detected more than one kind of EPD data (uncompressed, compressed, survey).")

        if uncompressed:
            df = self.update_uncompressed_df(df)
        elif compressed:
            num_sectors = 32 if bool(set([7, 8]).intersection(types)) else 16
            df = self.decompress_df(processing_request, df=df, num_sectors=num_sectors, table=EPD_HUFFMAN)
        elif survey:
            df = self.decompress_df(processing_request, df=df, num_sectors=4, table=EPD_HUFFMAN)
        else:
            self.logger.warning("⚠️ Detected neither compressed nor uncompressed nor survey data.")

        if 4 in types:
            df = self.filter_start_4_bogus_epd(processing_request, df)

        # At this point, idpu time should hold collection time, including decompressed data
        if (
            pd.to_datetime(df.idpu_time.min()) <= BOGUS_EPD_DATERANGE[1]
            and pd.to_datetime(df.idpu_time.max()) >= BOGUS_EPD_DATERANGE[0]
            and inner
        ):
            self.logger.info("Detected dataframe in Bogus EPD Time Range")
            df = self.remove_start_end_spin_periods(df)

        # Omit bad data
        for bad_epd_range in BAD_EPD_DATA_RANGES:
            if bad_epd_range.mission_id != processing_request.mission_id:
                continue

            bad_data = df["idpu_time"].between(bad_epd_range.start_time, bad_epd_range.end_time)
            if any(bad_data):
                self.logger.warning(
                    f"Found bad data relating to {bad_epd_range} (Number of rows to omit: {bad_data.sum()})"
                )
                df = df.loc[~bad_data]

        return df

    def update_uncompressed_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Updates the IDPU time of a dataframe of uncompressed data.

        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame of uncompressed data (IDPU types 3 or 5)

        Returns
        -------
        pd.DataFrame
            A DataFrame with updated IDPU times
        """
        self.logger.debug("Updating a dataframe of uncompressed EPD data")
        df["idpu_time"] = (
            df["data"]
            .apply(lambda x: None if pd.isnull(x) else bytes.fromhex(x))
            .apply(lambda x: byte_tools.raw_idpu_bytes_to_datetime(x[2:10]) if x else None)
        )
        df["spin_integration_factor"] = 1  # Spin integration factor defaults to 1

        return df

    def decompress_df(
        self, processing_request: ProcessingRequest, df: pd.DataFrame, num_sectors: int, table: Dict[str, int]
    ) -> pd.DataFrame:
        """Decompresses a DataFrame of compressed packets

        How EPD Compression Works:
        * Each sector has 16 bins associated with it (0 - 15)
        * One period is 16 sectors
        * Each packet corresponds to one period
        * Think of packets in groups of 10: 1 Header packet, which holds the
        actual values; and 9 Non-header packets, which don't store the actual
        values but the change that needs to be applied to the previous packet
        in order to get the value (ex. if the actual value is 5, and the
        previous value is 3, then 2 will be stored)
        * For ALL packets, these 'delta' values aren't actually stored. The
        sign is either + or -, which is stored for NON-HEADER packets. There
        is a table of 255 values. Instead of storing the actual values or
        deltas (depending on if it's a header or non-header), ALL PACKETS hold
        the indices of the closest values. These indexes are further reduced
        in size through Huffman Compression. Find the table and the Huffman
        Compression decoder in parse_log.py

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame
            DataFrame of EPD data to be decompressed
        num_sectors : int
            The number of sectors to be used in calculations
        table : dict
            The table to be referenced for Huffman compression

        Returns
        -------
        pd.DataFrame
            A DataFrame of decompressed data
        """

        def get_measured_values_if_valid(packet_num, cur_data, header_marker):
            """Uses data from a header packet to obtain new measured values.

            Returns
            -------
            Tuple[bool, Union[List[None], List[int]]]
                The first item represents whether the packet should be
                ignored, whereas the second item represents the new value of
                measured_values. The new measured_values will contain all None
                if a problem is found (ie. the packet should be ignored)
            """
            values = cur_data[header_marker + 1 :]
            if len(values) != BIN_COUNT * num_sectors:
                self.logger.warning(f"⚠️ Header at packet number {packet_num} didn't have all reference bins")
                return True, [None] * BIN_COUNT * num_sectors
            return False, list(values)

        def update_measured_values_if_valid(measured_values, packet_num, cur_data):
            """Use a non-header and previous measured_values to get new values.

            TODO: Modifies measured_values in place, but maybe it's better to
            just return it.

            Returns
            -------
            Tuple[bool, Union[List[None], List[int]]]
                The first item represents whether the packet should be
                ignored, whereas the second item represents the new value of
                measured_values. The new measured_values will contain all None
                if a problem is found (ie. the packet should be ignored)
            """
            if None in measured_values:
                return True, [None] * BIN_COUNT * num_sectors

            bitstring = byte_tools.bin_string(cur_data[10:])
            for i in range(BIN_COUNT * num_sectors):  # Updating each of the values in the period
                try:
                    sign, bitstring = self.get_sign(bitstring)
                except ValueError as e:
                    self.logger.warning(f"⚠️ Bad Sign: {e}, ind: {i}, packet {packet_num}")
                    return True, [None] * BIN_COUNT * num_sectors

                try:
                    delta1, bitstring = byte_tools.get_huffman(bitstring, table)
                    delta2, bitstring = byte_tools.get_huffman(bitstring, table)
                except IndexError as e:
                    self.logger.warning(f"⚠️ Not enough bytes in continuation packet: {e}, packet {packet_num}")
                    return True, [None] * BIN_COUNT * num_sectors

                measured_values[i] += sign * ((delta1 << 4) + delta2)  # These values could be unbound

                if not 0 <= measured_values[i] <= 255:
                    self.logger.warning(f"⚠️ measured_values went out of range [0, 255]: {measured_values[i]}")
                    return True, [None] * BIN_COUNT * num_sectors

            return False, measured_values

        # IDPU 24 (IBO compressed) has a different bit layout, so data is offset by 1
        is_ibo_data = 24.0 in df["idpu_type"].values
        ibo_offset = int(is_ibo_data)
        header_marker_byte = 10 + ibo_offset

        # l0_df: holds finished periods
        # measured values: For one period, hold value from Huffman table and later is put into period_df
        data = df["data"].apply(lambda x: None if pd.isnull(x) else bytes.fromhex(x))
        l0_df = pd.DataFrame(
            columns=[
                "mission_id",
                "idpu_type",
                "idpu_time",
                "numerator",
                "denominator",
                "data",
                "spin_integration_factor",
            ]
        )
        measured_values = [None] * BIN_COUNT * num_sectors

        # Set the index to initially point to the first header
        packet_num = self.find_first_header(data=data, header_marker_byte=header_marker_byte)

        if packet_num == data.shape[0]:
            return l0_df

        # TODO: Check if spin integration factor is consistent throughout downlink
        if is_ibo_data:
            # Akhil says only possible values are 2**n, 0<=n<=15
            spin_integration_factor = 2 ** (data.iloc[packet_num][IBO_DATA_BYTE] & 0x0F)
        else:
            spin_integration_factor = 1

        lossy_idx = self.find_lossy_idx(data.iloc[packet_num], header_marker_byte)
        marker = 0xAA + lossy_idx  # Determines if a packet is a header
        consecutive_nonheader_packets = 0
        ignore_packet = True  # Determine if we need to search for a new header, ex. bc of problem with current packet
        while packet_num < data.shape[0]:
            cur_data = data.iloc[packet_num]
            if cur_data[header_marker_byte] == marker:  # Current packet is a header (reference frame)
                if ibo_offset:  # Special case of compressed IBO data

                    data_type = "epdif" if cur_data[IBO_DATA_BYTE] >> 4 == 1 else "epdef"  # Top 4 bits are 0001
                    correct_data_type = data_type == processing_request.data_product
                else:  # All other types will always be true
                    correct_data_type = True

                if correct_data_type:
                    ignore_packet, measured_values = get_measured_values_if_valid(
                        packet_num, cur_data, header_marker_byte
                    )
                else:
                    ignore_packet, measured_values = True, [None] * BIN_COUNT * num_sectors

                consecutive_nonheader_packets = 0
            else:  # We get a non-header/non-reference frame/continuation frame
                ignore_packet, measured_values = update_measured_values_if_valid(measured_values, packet_num, cur_data)
                consecutive_nonheader_packets += 1

            # Append the period (found from either the header or non-header packet) to l0_df
            if not ignore_packet:
                period_df = self.get_period_df(
                    measured_values, EPD_LOSSY_VALS[lossy_idx], cur_data, df.iloc[packet_num], spin_integration_factor
                )

                l0_df = l0_df.append(period_df, sort=True)

            # Get the next needed packet (perhaps a header is needed if something was wrong with a previous packet)
            packet_num += 1
            if (
                packet_num >= data.shape[0]
                or not data.iloc[packet_num]
                or ignore_packet
                or consecutive_nonheader_packets >= 9
            ):
                packet_num += self.find_first_header(data=data.iloc[packet_num:], header_marker_byte=header_marker_byte)

        return l0_df.reset_index(drop=True)

    @staticmethod
    def find_lossy_idx(data_packet_in_bytes: bytes, header_marker_byte: int) -> int:
        return data_packet_in_bytes[header_marker_byte] - 0xAA

    @staticmethod
    def find_first_header(data: pd.Series, header_marker_byte: int) -> int:
        """Given EPD data, find the first header.

        We have found a header when the row's data is not null and when the
        row passes the check of AND-ing the header_marker_byte with 0xA0

        Parameters
        ----------
        data : pd.Series
        header_marker_byte : int
        Returns
        -------
        int
            The index of the first header, or the length of the data if no
            headers are found
        """

        packet_num = 0
        while packet_num < data.shape[0]:
            if data.iloc[packet_num] is None or data.iloc[packet_num][header_marker_byte] & 0xA0 != 0xA0:
                packet_num += 1
            else:
                return packet_num
        return packet_num  # Went out of bounds, no header found

    @staticmethod
    def get_sign(bitstring: str) -> Tuple[int, str]:
        """From the front of the bitstring, determine the sign of the delta.

        If the bitstring does not begin with 00 (positive) or 01 (negative),
        a ValueError is raised. This indicates that either the parsing was
        done improperly, or that the delta was too large to be stored in the
        packet (this fact needs to be double checked).

        Parameters
        ----------
        bitstring : str

        Returns
        -------
        Tuple[int, str]
            An integer (either 1 or -1) representing the sign of the delta,
            and the remaining bitstring after the sign indicator
        """
        if bitstring[:2] == "00":
            return 1, bitstring[2:]
        if bitstring[:2] == "01":
            return -1, bitstring[2:]
        raise ValueError(f"Got {bitstring[:2]} instead of '00' or '01'")

    def get_period_df(self, measured_values, lossy_vals, cur_data, row, spin_integration_factor):
        """
        Using the indices found, as well as the table of 255 values, find the values
        for a period, and return it as a DataFrame. Basically, this is used to create new
        'uncompressed' packets that get added to the level 0 DataFrame

        lossy_vals is the table of 255 potential values
        loss_val_idx is one of the indices that were found from the compressed packets
        lossy_val is the value found using lossy_vals and lossy_val_idx

        Update 2022-01-13: The sector number byte will now always be constant
        (0xFF chosen arbitrarily). The function to convert from L0 to L1
        (parse_periods) has and will continue to generate sector numbers
        programmatically, rather than read sector numbers from the data. This
        avoids having to rely on this byte.
        """
        num_sectors = len(measured_values) / 16
        if num_sectors not in VALID_NUM_SECTORS:
            raise ValueError(f"Bad Number of Sectors: {num_sectors}")

        self.logger.debug("Formatting period to level 0 df")
        spin_period_bytes, collection_time_bytes = cur_data[8:10], cur_data[:8]
        sector_num_byte = int.to_bytes(0xFF, 1, "little")

        bytes_data = spin_period_bytes + collection_time_bytes + sector_num_byte

        num_bins = 0
        for loss_val_idx in measured_values:
            if num_bins == 16:  # increment the sector_num and prepare for a new sector
                bytes_data += sector_num_byte
                num_bins = 0
            lossy_val = lossy_vals[loss_val_idx]
            bytes_data += byte_tools.get_two_unsigned_bytes(lossy_val & 0xFFFF)  # least significant word first
            bytes_data += byte_tools.get_two_unsigned_bytes(lossy_val >> 16)  # most significant word next
            num_bins += 1

        data = bytes_data.hex()

        period_df = pd.DataFrame(
            data={
                "idpu_time": byte_tools.raw_idpu_bytes_to_datetime(collection_time_bytes),
                "data": data,
                "mission_id": row["mission_id"],
                "idpu_type": row["idpu_type"],
                "numerator": row["numerator"],
                "denominator": row["denominator"],
                "spin_integration_factor": spin_integration_factor,
            },
            index=[0],
        )
        return period_df

    def transform_l0_df(self, processing_request: ProcessingRequest, l0_df: pd.DataFrame) -> pd.DataFrame:
        """Processes a level 0 DataFrame to create a level 1 DataFrame.

        Parameters
        ----------
        processing_request : ProcessingRequest
        l0_df : pd.DataFrame
            A DataFrame of level 0 EPD data, to be transformed

        Returns
        -------
        pd.DataFrame
            A DataFrame of level 1 EPD data
        """
        l1_df = self.parse_periods(processing_request, l0_df)
        if processing_request.data_product in ("epdef", "epdif"):  # TODO: Find a way to avoid applying to inner belt
            self.logger.info("Handling adjacent sectors")
            l1_df = handle_adjacent_sectors(l1_df)
        l1_df = self.format_for_cdf(l1_df)

        return l1_df

    def parse_periods(self, processing_request: ProcessingRequest, df: pd.DataFrame) -> pd.DataFrame:
        """Parse EPD bin readings into a pandas DataFrame.

        Explanation:
            * Loop through each row, each is a full revolution. Convert data
            to bytes using function, then read in bins 0-15 for sectors 0-F
            * Return same DataFrame structure as before

        Update 2022-01-13: This function has, for a while, programmatically
        generated sector numbers rather than reading them from data. This
        update is to explicitly state that the function will not rely on
        sector numbers written in data. Note that this is motivated partially
        due to the changing format of the byte as a result of 32-sector EPD.

        Parameters
        ----------
        processing_request: ProcessingRequest
        df: pd.DataFrame
            A DataFrame of level 0 EPD data

        Returns
        -------
        pd.DataFrame
            A DataFrame containing bin count information from parsing periods
        """

        def get_num_sectors(data_product: str, idpu_type: int):
            if data_product in ("epdes", "epdis"):
                return 4
            return 16 if idpu_type not in (7, 8) else 32  # data_product in ("epdef", "epdif")

        if processing_request.data_product not in EPD_PRODUCTS:
            raise ValueError(f"Bad data product name: {processing_request.data_product}")

        bins: List[List[int]] = [[] for i in range(16)]
        all_sec_num = []
        all_num_sectors = []  # Total sectors per period
        all_idpu_times = []
        all_spin_periods = []
        all_spin_integration_factors = []

        if df.empty:
            return pd.DataFrame(
                columns=[
                    "idpu_time",
                    "spin_period",
                    "spin_integration_factor",
                    "sec_num",
                    "numsectors",
                    "bin00",
                    "bin01",
                    "bin02",
                    "bin03",
                    "bin04",
                    "bin05",
                    "bin06",
                    "bin07",
                    "bin08",
                    "bin09",
                    "bin10",
                    "bin11",
                    "bin12",
                    "bin13",
                    "bin14",
                    "bin15",
                ]
            )

        for _, row in df.iterrows():
            # Due to 32-sector EPD, num_sectors can now vary for a single data product
            num_sectors = get_num_sectors(processing_request.data_product, row["idpu_type"])
            cur_spin_period, cur_time_captured, bin_data = self.get_context(row["data"])

            all_idpu_times.extend(
                self.calculate_center_times_for_period(
                    cur_spin_period, cur_time_captured, num_sectors, row["idpu_type"], row["spin_integration_factor"]
                )
            )
            all_spin_periods.extend([cur_spin_period for _ in range(num_sectors)])
            all_spin_integration_factors.extend([row["spin_integration_factor"] for _ in range(num_sectors)])

            for i in self.get_sector_iterator(num_sectors):
                all_sec_num.append(i)
                all_num_sectors.append(num_sectors)

                for j in range(1, 65, 4):
                    bins[j // 4].append(
                        (bin_data[j] << 8) + (bin_data[j + 1]) + (bin_data[j + 2] << 24) + (bin_data[j + 3] << 16)
                    )
                bin_data = bin_data[65:]

        return pd.DataFrame(
            {
                "idpu_time": all_idpu_times,
                "spin_period": all_spin_periods,
                "spin_integration_factor": all_spin_integration_factors,
                "sec_num": all_sec_num,
                "numsectors": all_num_sectors,
                "bin00": bins[0],
                "bin01": bins[1],
                "bin02": bins[2],
                "bin03": bins[3],
                "bin04": bins[4],
                "bin05": bins[5],
                "bin06": bins[6],
                "bin07": bins[7],
                "bin08": bins[8],
                "bin09": bins[9],
                "bin10": bins[10],
                "bin11": bins[11],
                "bin12": bins[12],
                "bin13": bins[13],
                "bin14": bins[14],
                "bin15": bins[15],
            }
        )

    def get_context(self, data):
        """For a given row of data, finds (from parsing the data) and
        returns a tuple of: (spin period, time, and data for bins)
        """
        self.logger.debug(f"Getting context for {data}")
        data = str(data)
        spin_period = int(data[0:4], 16)
        time_bytes = bytes.fromhex(data[4:20])
        bin_data = bytes.fromhex(data[20:])
        return spin_period, byte_tools.raw_idpu_bytes_to_datetime(time_bytes), bin_data

    def calculate_center_times_for_period(
        self, spin_period, time_captured, num_sectors, data_type, spin_integration_factor
    ):
        """Interpolates center times for in between sectors and converts to tt2000"""
        seconds_per_sector = dt.timedelta(seconds=(spin_period / INSTRUMENT_CLK_FACTOR) / num_sectors)
        center_time_offset = seconds_per_sector / 2

        spin_integration_offset = dt.timedelta(seconds=0)
        # Set IBO Time to find center of all the spins instead of the beginning
        if data_type in IBO_TYPES:
            spin_integration_offset = dt.timedelta(
                seconds=(spin_integration_factor / 2 - 0.5) * (spin_period / INSTRUMENT_CLK_FACTOR)
            )

        self.logger.debug(
            f"seconds per sector = {seconds_per_sector},"
            f" center time offset = {center_time_offset},"
            f" spin_integration_offset = {spin_integration_offset}"
        )

        # List of times at each sector
        return [
            (time_captured + seconds_per_sector * i + center_time_offset + spin_integration_offset)
            for i in self.get_sector_iterator(num_sectors)
        ]

    @staticmethod
    def get_sector_iterator(num_sectors: int) -> range:
        if num_sectors == 16:
            return range(16)
        elif num_sectors == 32:
            return range(32)
        elif num_sectors == 4:
            return range(0, 16, 4)
        else:
            raise ValueError(f"Invalid number of sectors: {num_sectors}")

    @staticmethod
    def format_for_cdf(df: pd.DataFrame) -> pd.DataFrame:
        """Gets the columns corresponding to bins, in preparation for the CDF."""
        df["data"] = df[
            [
                "bin00",
                "bin01",
                "bin02",
                "bin03",
                "bin04",
                "bin05",
                "bin06",
                "bin07",
                "bin08",
                "bin09",
                "bin10",
                "bin11",
                "bin12",
                "bin13",
                "bin14",
                "bin15",
            ]
        ].values.tolist()
        return df

    def fill_cdf(
        self, processing_request: ProcessingRequest, cdf: pycdf.CDF, df: pd.DataFrame, cdf_fields: Dict[str, str]
    ) -> None:
        """Fills a CDF with the relevant EPD information from a DatFrame.

        On top of the base functionality, this method also includes EPD energies.

        Parameters
        ----------
        processing_request : ProcessingRequest
        cdf : pycdf.CDF
        df : pd.DataFrame
        cdf_fields: Dict[str, str]

        Returns
        -------
        None
            The given CDF is directly modified
        """
        super().fill_cdf(processing_request, cdf, df, cdf_fields)

        prefix = f"{processing_request.probe}_p{processing_request.data_product[-2:]}"
        e_or_i = processing_request.data_product[-2:-1]

        with open(f"{EPD_CALIBRATION_DIR}/{processing_request.probe}_cal_epd{e_or_i}.txt", "r") as f:
            lines = f.readlines()

            idx = 0
            categories_filled = 0
            while idx < len(lines):
                current = lines[idx]

                if "ebins_logmean:" in current:
                    energies_midpoint = [float(lines[i].split()[0]) for i in range(idx + 1, idx + 17)]
                    cdf[f"{prefix}_energies_mean"] = energies_midpoint

                    idx += 17
                    categories_filled += 1

                elif "ebins_minmax:" in current:
                    minmax = [lines[i].split() for i in range(idx + 1, idx + 17)]
                    min_list = [float(i[0][:-1]) for i in minmax]
                    max_list = [float(i[1]) for i in minmax]

                    cdf[f"{prefix}_energies_min"] = min_list
                    cdf[f"{prefix}_energies_max"] = max_list

                    idx += 17
                    categories_filled += 2

                else:
                    idx += 1

                if categories_filled == 3:
                    break

        if categories_filled != 3:
            self.logger.warning("Issues with Inserting Energy Information!!")

    def get_cdf_fields(self, processing_request: ProcessingRequest) -> Dict[str, str]:
        """Gets a map of relevant CDF fields for EPD data to DF column names.

        Parameters
        ----------
        processing_request : ProcessingRequest

        Returns
        -------
        Dict[str, str]
            A Python dictionary mapping CDF field names to DataFrame column
            names, based on the probe and data product specified in the
            processing_request
        """
        prefix = f"{processing_request.probe}_p{processing_request.data_product[-2:]}"
        return {
            f"{prefix}": "data",
            f"{prefix}_time": "idpu_time",
            f"{prefix}_sectnum": "sec_num",
            f"{prefix}_nsectors": "numsectors",
            f"{prefix}_spinper": "spin_period",
            f"{prefix}_nspinsinsum": "spin_integration_factor",  # Used spin_integration_factor because I like it more
        }

    def filter_start_4_bogus_epd(self, processing_request: ProcessingRequest, df: pd.DataFrame) -> pd.DataFrame:
        """Filters out epd data of type 4 that is appearing during IBO collection times. This does not filter out
        bogus EPD of type 24 which was also observed in the beginning and ends of collections

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame

        Returns
        -------
        pd.DataFrame
            A copy of the input DataFrame without data in IBO collection
        """

        if df.empty:
            return df

        def get_collection_interval(df: pd.DataFrame):
            # One extra minute is being added to both sides as a buffer time
            return (
                pd.to_datetime(df.start_time) - dt.timedelta(minutes=1),
                pd.to_datetime(df.end_time) + dt.timedelta(minutes=1),
            )

        self.logger.info("Filtering out bogus EPD data")

        # Querying for IBO collections (outer belt collections should all be < 8 minutes)
        intents_query = (
            Query([models.TimeIntervals, models.Intent.intent_type])
            .filter(
                models.TimeIntervals.mission_id == processing_request.mission_id,
                func.timestampdiff(text("second"), models.TimeIntervals.start_time, models.TimeIntervals.end_time)
                > 60 * 8,
                (
                    (models.TimeIntervals.interval_type == "ExecutionTime")
                    & (models.Intent.intent_type == "ScienceCollection")
                ),
            )
            .join(models.Intent, models.TimeIntervals.intent_id == models.Intent.id)
            .join(models.Allocation, models.TimeIntervals.allocation_id == models.Allocation.id)
        )

        intents_df = pd.read_sql_query(intents_query.statement, self.session.bind)

        self.logger.info(
            f"Found {len(intents_df) if not intents_df.empty else 0} intents/allocations longer than 8 minutes"
        )

        ibo_intervals = IntervalTree.from_tuples(intents_df.apply(lambda x: get_collection_interval(x), axis=1).values)

        # Checking if any idpu_time is in an ibo collection interval
        df["bogus"] = df.apply(lambda x: len(ibo_intervals.at(x.idpu_time)) != 0, axis=1)

        removed_dfs = df[df.bogus]
        self.logger.info(
            "Removed Bogus data from the following dates:\n\t"
            + "\n\t".join(str(date) for date in removed_dfs["idpu_time"].values)
            + "\n\t"
            + f"Total = {len(removed_dfs)}"
            + "\n"
        )

        df = df[~df.bogus]
        del df["bogus"]

        return df

    def remove_start_end_spin_periods(self, df: pd.DataFrame) -> pd.DataFrame:
        """Removes the first and last spin period of each science zone

        Parameters
        ----------
        df : pd.DataFrame

        Returns
        -------
        df : pd.DataFrame
            DataFrame with first and last entries removed.
            If there are <= 2 entries, it returns the original df

        """
        self.logger.info("Removing the first and last entries of science zones")
        science_zones = []

        # Note iloc[0:1] and iloc[i:i+1] are used so that a DataFrame is returned instead of a Series
        current_science_zone = [df.iloc[0:1]]
        discarded_science_zone_times = []
        for i in range(1, df.shape[0]):
            # Split the DataFrame if we're looking at a different collection
            if df.iloc[i].idpu_time - df.iloc[i - 1].idpu_time > dt.timedelta(minutes=20):
                if len(current_science_zone) > 2:
                    science_zones.append(pd.concat(current_science_zone[1:-1]))

                    discarded_science_zone_times.extend(
                        [current_science_zone[0].idpu_time.values[0], current_science_zone[-1].idpu_time.values[0]]
                    )

                    # self.logger.info(
                    #   f"Removed the following two spin periods: \n\t"
                    #  + f"{str(current_science_zone[0].idpu_time.values[0])}\n\t"
                    # + f"{str(current_science_zone[-1].idpu_time.values[0])}"
                    # )
                else:
                    self.logger.info(
                        f"<=2 spin periods in sz starting at {str(current_science_zone[0].idpu_time.values[0])}"
                    )
                current_science_zone = [df.iloc[i : i + 1]]

            # Keep adding spin periods into the DataFrame
            else:
                current_science_zone.append(df.iloc[i : i + 1])

        if len(current_science_zone) >= 2:
            science_zones.append(pd.concat(current_science_zone[1:-1]))
            discarded_science_zone_times.extend(
                [current_science_zone[0].idpu_time.values[0], current_science_zone[-1].idpu_time.values[0]]
            )
        else:
            self.logger.info(f"<2 spin periods in szstarting at {str(current_science_zone[0].idpu_time.values[0])}")

        self.logger.info(
            "Removed Bogus data from the following dates:\n\t"
            + "\n\t".join(str(date) for date in discarded_science_zone_times)
            + "\n\t"
            + f"Total = {len(discarded_science_zone_times)}"
            + "\n"
        )

        if len(science_zones) >= 1:
            df = pd.concat(science_zones).reset_index()
        else:
            self.logger.info("No science zones in dataframe with > 2 spin periods")

        return df
