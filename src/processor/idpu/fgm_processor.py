"""Class to generate FGM files"""
import datetime as dt
from enum import Enum

import numpy as np
import pandas as pd

from data_type.completeness_config import FgmCompletenessConfig
from metric.completeness import CompletenessUpdater
from processor.idpu.idpu_processor import IdpuProcessor
from util import byte_tools
from util.compression_values import FGM_HUFFMAN
from util.constants import BITS_IN_BYTE
from util.science_utils import hex_to_int

# TODO: Hardcoded values -> Constants


class FgmRow:
    def __init__(self, idpu_time, axes, frequency, numerator):
        self.idpu_time = idpu_time
        self.axes = axes
        self.frequency = frequency
        self.numerator = numerator


class FgmFrequencyEnum(Enum):
    """Stores the period as a float

    TODO: UNKNOWN should probably not have period 0.0125, but this avoids an
    error with line ~254, with the statement

    idpu_time += dt.timedelta(seconds=(frequency.value))

    See older commits for original code
    """

    TEN_HERTZ = 0.1
    EIGHTY_HERTZ = 0.0125
    UNKNOWN = 0.0125


class FgmProcessor(IdpuProcessor):
    def __init__(self, pipeline_config):
        super().__init__(pipeline_config)

        self.completeness_updater = CompletenessUpdater(pipeline_config.session, FgmCompletenessConfig)

    def process_rejoined_data(self, processing_request, df):
        """
        Return a dataframe corresponding to correctly formatted data products
        df of query from database.
        - For compressed data, decompress
        - For uncompressed data, select data only if it is at the correct sampling rate

        NOTE: idpu_time is formatted as a datetime
        """
        df = df.dropna(subset=["idpu_time", "data"])
        df = df.reset_index(drop=True)

        # Checking for compressed and uncompressed data
        uncompressed = df["idpu_type"].isin([1, 17]).any()
        compressed = df["idpu_type"].isin([2, 18]).any()

        if uncompressed and compressed:
            self.logger.warning("⚠️ Detected both compressed and uncompressed data. This should never happen...")
            return df[df["idpu_type"].isin([1, 17])]

        if compressed:
            df = self.decompress_df(processing_request, df)
        elif uncompressed:
            df["sampling_rate"] = self.find_diff(df)
            df["10hz_mode"] = df["sampling_rate"].apply(
                lambda x: self.check_sampling_rate(x.to_pytimedelta(), compressed=False)
            )
            df = self.drop_packets_by_freq(processing_request, df)
            df = df[["mission_id", "idpu_type", "idpu_time", "numerator", "denominator", "data", "10hz_mode"]]
        else:
            self.logger.warning("⚠️ Detected neither compressed nor uncompressed data.")

        return df

    def find_diff(self, df):
        """
        Helper Function to find the time difference between two packets
        Used to help determine sampling rate
        """
        self.logger.debug("Finding diff")
        final = pd.DataFrame(columns=["sampling_rate"])
        for idx, _ in df.iloc[1:].iterrows():
            cur_time = df["idpu_time"].iloc[idx]
            prev_time = df["idpu_time"].iloc[idx - 1]
            cur_num = df["numerator"].iloc[idx]
            prev_num = df["numerator"].iloc[idx - 1]

            final.loc[idx] = (cur_time - prev_time) / (cur_num - prev_num)
        first = pd.DataFrame(data={"sampling_rate": [final["sampling_rate"].iloc[0]]})
        final = pd.concat([first, final])
        return final

    def transform_l0_df(self, processing_request, l0_df):
        """
        Does the necessary processing on a level 0 df to create a level 1 df
        ** collection_date is unused
        """
        l1 = {
            "idpu_type": l0_df["idpu_type"],
            "idpu_time": l0_df["idpu_time"],
            "ax1": l0_df.data.str.slice(0, 6).apply(hex_to_int),
            "ax2": l0_df.data.str.slice(6, 12).apply(hex_to_int),
            "ax3": l0_df.data.str.slice(12, 18).apply(hex_to_int),
            "status": l0_df.data.str.slice(18, 24).apply(hex_to_int),
        }

        l1_df = pd.DataFrame(l1)
        l1_df["ax1"] = l1_df["ax1"].astype(np.float32)
        l1_df["ax2"] = l1_df["ax2"].astype(np.float32)
        l1_df["ax3"] = l1_df["ax3"].astype(np.float32)
        l1_df["data"] = l1_df[["ax1", "ax2", "ax3"]].values.tolist()

        return l1_df

    def is10hz_sampling_rate(self, first_ts, second_ts):
        """
        This function actually checks if this is 80 hz or 10 hz.
        If it is 10hz, returns True, if is 80 hz, returns False,
        if it's neither, return None
        SHOULD ONLY BE USED FOR COMPRESSED
        TODO: probably a better way to write this lol
        """
        time_gap = second_ts - first_ts
        return self.check_sampling_rate(time_gap, compressed=True)

    def check_sampling_rate(self, time_gap, compressed=True):
        """
        Returns True if 10 hz, False if 80 hz, and None if neither
        compressed is 10 decompressed packets per packet, so expect 1/freq*10 per compressed packet
        """
        multiplier = 10 if compressed else 1
        self.logger.debug(f"When checking sampling rate, using multiplier={multiplier}")

        # check for 80 hz    microseconds in 81 hz < time_gap < microseconds in 79 hz
        if (
            dt.timedelta(microseconds=1 / 81 * 1e6) * multiplier
            <= time_gap
            <= dt.timedelta(microseconds=1 / 79 * 1e6) * multiplier
        ):
            return FgmFrequencyEnum.EIGHTY_HERTZ

        # check for 10 hz    microseconds in 11 hz < time_gap < microseconds in 9 hz
        # technically it should be 125000 but just give it some leeway
        if (
            dt.timedelta(microseconds=1 / 11 * 1e6) * multiplier
            <= time_gap
            <= dt.timedelta(microseconds=1 / 9 * 1e6) * multiplier
        ):
            return FgmFrequencyEnum.TEN_HERTZ

        return FgmFrequencyEnum.UNKNOWN

    def decompress_df(self, processing_request, df):
        """Decompresses a compressed df of FGM data.

        Best attempt to explain what's going on:
        * Prepare DataFrame for decompression
        * Loop through each row of the DataFrame (First Loop). Each row starts
        off with the 'header', or original values. Then, we get 'non-header'
        packets that were appended to the row. To get the values for non-
        headers, we need to find reassemble the deltas and apply them to the
        packet before, similarly to EPD (we know the time difference because
        FGM is collected at either 10 Hz or 80 Hz)
        * Once we have all the data from the DataFrame, put it into a new
        DataFrame to return, where the new DataFrame mimics a DataFrame of
        uncompressed Data (Second Loop)

        NOTE: The above explanation may have slightly incorrect syntax because
        I didn't write this and because I was looking through EPD
        decompression before this

        Parameters
        ----------
        processing_request : ProcessingRequest
            An object specifying details about the requested processing
        df : pd.DataFrame
            A DataFrame of compressed FGM data

        Returns
        -------
        pd.DataFrame
            A DataFrame of FGM data, as though it were never compressed
        """

        # Preparing the DataFrame that was received
        df["data"] = df["data"].apply(bytes.fromhex)
        df["idpu_time"] = df["data"].apply(lambda x: byte_tools.raw_idpu_bytes_to_datetime(x[:8]))
        df = df.sort_values(by=["idpu_time"]).drop_duplicates(["idpu_time", "data"])

        decompressed_rows = []

        prev_time = None
        frequency = FgmFrequencyEnum.UNKNOWN  # Default is 10 hz, but checking is done for each

        for _, row in df.iterrows():
            idpu_time = row["idpu_time"]

            # find sampling rate based off of difference in time between 2 downlinked packets
            if prev_time:
                if self.is10hz_sampling_rate(prev_time, idpu_time) != FgmFrequencyEnum.UNKNOWN:
                    frequency = self.is10hz_sampling_rate(prev_time, idpu_time)
            prev_time = idpu_time

            axes_values = [
                byte_tools.get_signed(row["data"][8:11]),
                byte_tools.get_signed(row["data"][11:14]),
                byte_tools.get_signed(row["data"][14:17]),
            ]

            # Create the initial decompressed row, later decompressed rows will follow
            decompressed_rows.append(FgmRow(idpu_time, list(axes_values), frequency, row["numerator"]))

            # Store remaining data so the code can look for more values
            bs = byte_tools.bin_string(row["data"][17:])

            # Calculate the delta, and apply it to the appropriate axis (based on axis_index)
            axis_index = 0
            while len(bs) > BITS_IN_BYTE:  # TODO: Should this be >= ?, want to double check this
                delta, bs = self.get_delta(bs, idpu_time)
                if delta is None:
                    break

                axes_values[axis_index] += delta

                if axis_index == 2:  # Flush row
                    idpu_time += dt.timedelta(seconds=(frequency.value))
                    decompressed_rows.append(FgmRow(idpu_time, axes_values.copy(), frequency, row["numerator"]))

                axis_index = (axis_index + 1) % 3

        return self.create_decompressed_df_from_rows(processing_request, decompressed_rows)

    def get_delta(self, bs, idpu_time):
        # Handling Sign: Getting 11 does not necessarily mean the science processing code
        # is broken - the idpu code is set up to insert 11 if the the delta is too high
        # to be encoded (creating a 'marker' for that error). If you have the permissions
        # necessary, and want to view the idpu_code, the link is:
        # https://elfin-dev1.igpp.ucla.edu/repos/eng/FPGA/elfin_ns8/idpu_em/source/branches/akhil_branch/embedded/idpu_3
        if bs[:2] == "11":
            self.logger.debug(f"⚠️  Got sign bits '11' with {len(bs)} bytes remaining in current row")
            return None, bs
        sign = -1 if bs[:2] == "01" else 1
        bs = bs[2:]

        try:
            hexit16, bs = byte_tools.get_huffman(bs, table=FGM_HUFFMAN)
            hexit12, bs = byte_tools.get_huffman(bs, table=FGM_HUFFMAN)
            hexit8 = int(bs[0:4], 2)
            hexit4 = int(bs[4:8], 2)
            bs = bs[8:]
        except IndexError:
            self.logger.warning("⚠️ Unable to decompress correctly")
            return None, bs

        return sign * self.calculate_delta_magnitude(idpu_time, hexit16, hexit12, hexit8, hexit4), bs

    @staticmethod
    def calculate_delta_magnitude(idpu_time, hexit16, hexit12, hexit8, hexit4):
        if idpu_time.strftime("%Y-%m-%d") < "2019-03-25":  # Apparently something changed in late March 2019?
            return (hexit16 << 16) + (hexit12 << 12) + (hexit8 << 8) + (hexit4 << 4)
        return (hexit16 << 16) + (hexit12 << 12) + (hexit8 << 8) + (hexit4 << 4) << 1

    @staticmethod
    def create_new_packet(axes, numerator, denominator):
        ax1, ax2, ax3 = axes

        pb = byte_tools.get_three_signed_bytes(ax1)  # length = 16
        pb += byte_tools.get_three_signed_bytes(ax2)  # length = 19
        pb += byte_tools.get_three_signed_bytes(ax3)  # length = 22
        pb += b"\x00\x00\x00"  # fake HSKP, length: 25
        pb += byte_tools.get_two_unsigned_bytes(numerator)  # numerator, length: 27
        pb += byte_tools.get_two_unsigned_bytes(denominator)  # denominator, length: 29
        pb += b"\x00"  # fake CRC

        return pb.hex()

    def create_decompressed_df_from_rows(self, processing_request, decompressed_rows):
        new_denom = len(decompressed_rows) - 1

        df_dict = {
            "mission_id": [],
            "idpu_type": [],
            "idpu_time": [],
            "numerator": [],
            "denominator": [],
            "data": [],
            "10hz_mode": [],
        }

        # This loops through all decompressed rows, adding to the lists to prepare for creating the returned DataFrame
        for i, row in enumerate(decompressed_rows):
            if row.frequency != FgmFrequencyEnum.UNKNOWN:
                df_dict["mission_id"].append(processing_request.mission_id)
                df_dict["idpu_type"].append(
                    0x1 if row.frequency == FgmFrequencyEnum.TEN_HERTZ else 0x17
                )  # This is how the code keeps track of type
                df_dict["idpu_time"].append(row.idpu_time)
                df_dict["numerator"].append(
                    row.numerator
                )  # Numerator of the frame that this packet came from. Useful for data tracking
                df_dict["denominator"].append(new_denom)
                df_dict["data"].append(self.create_new_packet(row.axes, i, new_denom))
                df_dict["10hz_mode"].append(row.frequency)  # This is how the code keeps track of frequency

        # Forming the final DF, preparing it (keep the correct frequency), then returning it
        df = pd.DataFrame(df_dict)
        df = self.drop_packets_by_freq(processing_request, df)

        return df

    def drop_packets_by_freq(self, processing_request, df):
        """ Returns a DataFrame with either 10 hz (fgs) or 80 hz (fgf) """
        if processing_request.data_product == "fgf":
            self.logger.debug("DataFrame contains fgf data")
            df = df[df["10hz_mode"] == FgmFrequencyEnum.EIGHTY_HERTZ]
        elif processing_request.data_product == "fgs":
            self.logger.debug("DataFrame contains fgs data")
            df = df[df["10hz_mode"] == FgmFrequencyEnum.TEN_HERTZ]
        else:
            raise ValueError("Invalid data_product_name")

        return df

    def merge_processed_dataframes(self, dfs, idpu_types):
        """
        FGM-specific version of merge_processed_dataframes (Times are rounded to
        nearest millisecond to determine if packets are the same)
        Given a list of dataframes of identical format (decompressed/raw, level 0),
        merge them in a way such that duplicate frames are removed.

        Preference is given in the same order as which IDPU_TYPEs appear in the list
        self.idpu_types. Keeping the first item means that the first/earlier
        idpu_type will be preserved. idpu_type is ordered in the same order as
        self.idpu_types
        """
        df = pd.concat(dfs)
        df["idpu_type"] = df["idpu_type"].astype("category").cat.set_categories(idpu_types, ordered=True)

        df = df.dropna(subset=["data", "idpu_time"])
        df = df.sort_values(["idpu_time", "idpu_type"])

        rounded_idpu_time = pd.DatetimeIndex(df["idpu_time"])
        rounded_idpu_time = rounded_idpu_time.round("ms")  # pylint: disable=no-member
        df["rounded_idpu_time"] = rounded_idpu_time
        df = df.drop_duplicates("rounded_idpu_time", keep="first")
        df = df[["mission_id", "idpu_type", "idpu_time", "numerator", "denominator", "data", "10hz_mode"]]

        return df.reset_index()

    def get_completeness_updater(self, processing_request):
        return self.completeness_updater

    def get_cdf_fields(self, processing_request):
        probe = processing_request.probe
        data_product = processing_request.data_product
        return {f"{probe}_{data_product}": "data", f"{probe}_{data_product}_time": "idpu_time"}
