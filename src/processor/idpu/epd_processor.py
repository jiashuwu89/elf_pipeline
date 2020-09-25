"""This class processes level 0/1 epdef, epdes, epdif, and epdis data.

The epd_processor class handles several things, including:
* Decompression of epd
* Filtering of data based on collection frequency
* Formatting of epd related data into a database.
"""
# TODO: This is just copied from the original epd_processor.py. Needs to be refactored

import datetime as dt

import pandas as pd

from data_type.completeness_config import EpdeCompletenessConfig, EpdiCompletenessConfig
from metric.completeness import CompletenessUpdater
from processor.idpu.idpu_processor import IdpuProcessor
from util import byte_tools
from util.compression_values import EPD_HUFFMAN, EPD_LOSSY_VALS
from util.constants import BIN_COUNT, EPD_CALIBRATION_DIR, VALID_NUM_SECTORS

# EPD_ENERGIES = [[50., 70., 110., 160., 210., 270., 345., 430., 630., 900., 1300., 1800., 2500., 3000., 3850., 4500.]]


class EpdProcessor(IdpuProcessor):
    def __init__(self, pipeline_config):
        super().__init__(pipeline_config)

        self.epde_completeness_updater = CompletenessUpdater(pipeline_config.session, EpdeCompletenessConfig)
        self.epdi_completeness_updater = CompletenessUpdater(pipeline_config.session, EpdiCompletenessConfig)

    def process_rejoined_data(self, processing_request, df):
        """
        The logic is as follows: Given an EPD dataframe...
            - If uncompressed data, go to update_uncompressed_df
            - If compressed data, decompress the data
            - Otherwise, something went wrong
        """
        df["data"] = df["data"].apply(lambda x: None if pd.isnull(x) else bytes.fromhex(x))

        # TODO: unify with fgm_processor
        types = df["idpu_type"].values
        uncompressed = 3 in types or 5 in types
        compressed = 4 in types or 6 in types
        survey = 19 in types or 20 in types

        if uncompressed + compressed + survey > 1:
            raise ValueError("⚠️ Detected more than one kind of EPD data (uncompressed, compressed, survey).")

        if uncompressed:
            df = self.update_uncompressed_df(df)
        elif compressed:
            df = self.decompress_df(df=df, num_sectors=16, table=EPD_HUFFMAN)
        elif survey:
            df = self.decompress_df(df=df, num_sectors=4, table=EPD_HUFFMAN)
        else:
            self.logger.warning("⚠️ Detected neither compressed nor uncompressed nor survey data.")

        return df

    def update_uncompressed_df(self, df):
        """ For a dataframe of uncompressed data, update the idpu_time field to None if appropriate """
        self.logger.debug("Updating a dataframe of uncompressed EPD data")
        df["idpu_time"] = df["data"].apply(lambda x: byte_tools.raw_idpu_bytes_to_datetime(x[2:10]) if x else None)
        return df

    def decompress_df(self, df, num_sectors, table):
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

        def get_measured_values_if_valid(packet_num, cur_data):
            """Returns a list to replace measured_values, will be empty if bad cur_data"""
            values = cur_data[11:]
            if len(values) != BIN_COUNT * num_sectors:
                self.logger.warning(f"⚠️ Header at packet number {packet_num} didn't have all reference bins")
                return True, []
            return False, list(values)

        def update_measured_values_if_valid(measured_values, packet_num, cur_data):
            """TODO: Modifies measured_values in place, but maybe it's better to just return it"""
            bitstring = byte_tools.bin_string(cur_data[10:])
            ignore_packet = False
            for i in range(BIN_COUNT * num_sectors):  # Updating each of the 256 values in the period
                try:
                    sign, bitstring = self.get_sign(bitstring)
                except ValueError as e:
                    self.logger.warning(f"⚠️ Bad Sign: {e}, ind: {i}, packet {packet_num}")
                    ignore_packet = True

                try:
                    delta1, bitstring = byte_tools.get_huffman(bitstring, table)
                    delta2, bitstring = byte_tools.get_huffman(bitstring, table)
                except IndexError as e:
                    self.logger.warning(f"⚠️ Not enough bytes in continuation packet: {e}, packet {packet_num}")
                    ignore_packet = True

                measured_values[i] += sign * ((delta1 << 4) + delta2)

                if not 0 <= measured_values[i] <= 255:
                    self.logger.warning(f"⚠️ measured_values went out of range [0, 255]: {measured_values[i]}")
                    ignore_packet = True

                if ignore_packet:
                    break

            return ignore_packet, measured_values

        # l0_df: holds finished periods
        # measured values: For one period, hold value from Huffman table and later is put into period_df
        data = df["data"]
        l0_df = pd.DataFrame(columns=["mission_id", "idpu_type", "idpu_time", "numerator", "denominator", "data"])
        measured_values = [None] * BIN_COUNT * num_sectors

        # Set the index to initially point to the first header
        packet_num = self.find_first_header(data)
        if packet_num == data.shape[0]:
            return l0_df

        lossy_idx = self.find_lossy_idx(data.iloc[packet_num])
        marker = 0xAA + lossy_idx  # Determines if a packet is a header
        consecutive_nonheader_packets = 0
        ignore_packet = False  # Determine if we need to search for a new header, ex. bc of problem with current packet

        while packet_num < data.shape[0]:
            cur_data = data.iloc[packet_num]
            if cur_data[10] == marker:  # Current packet is a header (reference frame)
                ignore_packet, measured_values = get_measured_values_if_valid(packet_num, cur_data)
            else:  # We get a non-header/non-reference frame/continuation frame
                ignore_packet, measured_values = update_measured_values_if_valid(measured_values, packet_num, cur_data)
                consecutive_nonheader_packets += 1

            # Append the period (found from either the header or non-header packet) to l0_df
            l0_df = (
                l0_df.append(
                    self.get_period_df(measured_values, EPD_LOSSY_VALS[lossy_idx], cur_data, df.iloc[packet_num]),
                    sort=True,
                )
                if not ignore_packet
                else l0_df
            )

            # Get the next needed packet (perhaps a header is needed if something was wrong with a previous packet)
            packet_num += 1
            if (
                packet_num >= data.shape[0]
                or not data.iloc[packet_num]
                or ignore_packet
                or consecutive_nonheader_packets >= 9
            ):
                packet_num += self.find_first_header(data.iloc[packet_num:])
                consecutive_nonheader_packets = 0

        return l0_df.reset_index(drop=True)

    @staticmethod
    def find_lossy_idx(data_packet_in_bytes):
        return data_packet_in_bytes[10] - 0xAA

    @staticmethod
    def find_first_header(data):
        packet_num = 0
        while packet_num < data.shape[0]:
            if data.iloc[packet_num] is None or data.iloc[packet_num][10] & 0xA0 != 0xA0:
                packet_num += 1
            else:
                return packet_num
        return packet_num  # Went out of bounds, no header found

    @staticmethod
    def get_sign(bitstring):
        if bitstring[:2] == "00":
            return 1, bitstring[2:]
        if bitstring[:2] == "01":
            return -1, bitstring[2:]
        raise ValueError(f"Got {bitstring[:2]} instead of '00' or '01'")

    def get_period_df(self, measured_values, lossy_vals, cur_data, row):
        """
        Using the indices found, as well as the table of 255 values, find the values
        for a period, and return it as a DataFrame. Basically, this is used to create new
        'uncompressed' packets that get added to the level 0 DataFrame

        lossy_vals is the table of 255 potential values
        loss_val_idx is one of the indices that were found from the compressed packets
        lossy_val is the value found using lossy_vals and lossy_val_idx
        """
        self.logger.debug("Formatting period to level 0 df")
        spin_period_bytes, collection_time_bytes = cur_data[8:10], cur_data[:8]

        # Add the time
        bytes_data = spin_period_bytes + collection_time_bytes
        num_sectors = len(measured_values) / 16
        if num_sectors not in VALID_NUM_SECTORS:
            raise ValueError(f"Bad Number of Sectors: {num_sectors}")

        num_bins = 0
        sector_num = 0x0F
        bytes_data += bytes([sector_num])

        for loss_val_idx in measured_values:
            if num_bins == 16:  # increment the sector_num and prepare for a new sector
                sector_num += 0x10 if num_sectors == 16 else 0x40
                bytes_data += bytes([sector_num])
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
            },
            index=[0],
        )
        return period_df

    def transform_l0_df(self, processing_request, l0_df):
        """
        Does the necessary processing on a level 0 df to create a level 1 dataframe

        NOTE: collection_date is an unused parameter for EPD's transform_level_0, but
        it could be used in other processors (so don't delete it)
        """
        l1_df = self.parse_periods(processing_request, l0_df)
        l1_df = self.format_for_cdf(l1_df)

        return l1_df

    def parse_periods(self, processing_request, df):
        """Parse EPD bin readings into a pandas DataFrame.

        Explanation:
        * Loop through each row, each is a full revolution. Convert data to
        bytes using function, then read in bins 0-15 for sectors 0-F
        * Return same DataFrame structure as before
        """

        bins = [[] for i in range(16)]
        all_sec_num = []
        all_idpu_times = []
        all_spin_periods = []

        # Determine the number of sectors
        # TODO: The way this works could be much better, but it was written befor survey mode
        # was fully supported. The formula for num_sectors could be (data[20:] / 16) - 1, but
        # not completely sure
        if processing_request.data_product[-1] == "f":
            num_sectors = 16
        elif processing_request.data_product[-1] == "s":
            self.logger.warning("Survey Mode handling is still in progress")
            num_sectors = 4
        else:
            raise ValueError(f"Bad data product name: {processing_request.data_product}")

        for _, row in df.iterrows():
            cur_spin_period, cur_time_captured, bin_data = self.get_context(row["data"])
            all_spin_periods.extend([cur_spin_period for i in range(num_sectors)])
            all_idpu_times.extend(
                self.calculate_center_times_for_period(cur_spin_period, cur_time_captured, num_sectors)
            )

            for i in range(0, 16, 16 // num_sectors):
                all_sec_num.append(i)
                for j in range(1, 65, 4):
                    bins[j // 4].append(
                        (bin_data[j] << 8) + (bin_data[j + 1]) + (bin_data[j + 2] << 24) + (bin_data[j + 3] << 16)
                    )
                bin_data = bin_data[65:]

        return pd.DataFrame(
            {
                "idpu_time": all_idpu_times,
                "spin_period": all_spin_periods,
                "sec_num": all_sec_num,
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

    def calculate_center_times_for_period(self, spin_period, time_captured, num_sectors):
        """ Interpolates center times for in between sectors and converts to tt2000 """
        seconds_per_sector = dt.timedelta(seconds=(spin_period / 80) / 16)
        center_time_offset = seconds_per_sector / 2
        self.logger.debug(f"seconds per sector = {seconds_per_sector}, center time offset = {center_time_offset}")
        return [(time_captured + seconds_per_sector * i + center_time_offset) for i in range(0, 16, 16 // num_sectors)]

    def format_for_cdf(self, df):
        """ Gets the columns corresponding to the bins, in preparation for the CDF """
        self.logger.debug("Format EPD df for CDF")
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

    #####################
    # Utility Functions #
    #####################
    def fill_cdf(self, processing_request, df, cdf):
        """ Same as base class's fill_CDF except this function also includes EPD energies """
        super().fill_cdf(processing_request, df, cdf)

        prefix = f"{processing_request}_p{processing_request.data_product[-2:]}"
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

    def get_completeness_updater(self, processing_request):
        if processing_request.data_product[-2] == "e":
            return self.epde_completeness_updater
        if processing_request.data_product[-2] == "i":
            return self.epdi_completeness_updater
        raise ValueError(f"Bad data_product: {processing_request.data_product}")

    def get_cdf_fields(self, processing_request):
        probe = processing_request.probe
        data_product_type = f"p{processing_request.data_product[-2:]}"
        return {
            f"{probe}_{data_product_type}": "data",
            f"{probe}_{data_product_type}_time": "idpu_time",
            f"{probe}_{data_product_type}_sectnum": "sec_num",
            f"{probe}_{data_product_type}_spinper": "spin_period",
        }
