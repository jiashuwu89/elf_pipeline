"""Class to generate ENG files

Classes:

    FgmProcessor
"""
import datetime as dt

import numpy as np
import pandas as pd

from metric.completeness import CompletenessUpdater, FgmCompletenessConfig
from processor.idpu.idpu_processor import IdpuProcessor
from util import byte_tools
from util.compression_values import FGM_HUFFMAN
from util.science_utils import hex_to_int

# TODO: Hardcoded values -> Constants


class FgmProcessor(IdpuProcessor):
    def __init__(self, session, output_dir, processor_name):
        super().__init__(session, output_dir, processor_name)

        # TODO: Fix this
        self.cdf_fields = {data_product: "data", data_product + "_time": "idpu_time"}

        self.completeness_updater = CompletenessUpdater(FgmCompletenessConfig)

    def process_rejoined_data(self, df):
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
            self.log.warning("⚠️ Detected both compressed and uncompressed data. This should never happen...")
            return df[df["idpu_type"].isin([1, 17])]

        if compressed:
            df = self.decompress_df(df)
        elif uncompressed:
            df["sampling_rate"] = self.find_diff(df)

            to_add = []
            for _, row in df.iterrows():
                to_check = row["sampling_rate"]
                to_check = to_check.to_pytimedelta()
                to_add.append((self.check_sampling_rate(to_check, compressed=False)))

            df["10hz_mode"] = to_add
            df = self.drop_packets_by_freq(df)
            df = df[
                ["mission_id", "idpu_type", "idpu_time", "numerator", "denominator", "data", "10hz_mode", "packet_id"]
            ]
        else:
            self.log.warning("⚠️ Detected neither compressed nor uncompressed data.")

        return df

    def find_diff(self, df):
        """
        Helper Function to find the time difference between two packets
        Used to help determine sampling rate
        """
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

    def transform_level_0(self, df, collection_date):
        """
        Does the necessary processing on a level 0 df to create a level 1 df
        ** collection_date is unused
        """
        level1 = {
            "idpu_type": df["idpu_type"],
            "idpu_time": df["idpu_time"],
            "ax1": df.data.str.slice(0, 6).apply(hex_to_int),
            "ax2": df.data.str.slice(6, 12).apply(hex_to_int),
            "ax3": df.data.str.slice(12, 18).apply(hex_to_int),
            "status": df.data.str.slice(18, 24).apply(hex_to_int),
            "packet_id": df["packet_id"],
        }

        level1_df = pd.DataFrame(level1)
        level1_df["ax1"] = level1_df["ax1"].astype(np.float32)
        level1_df["ax2"] = level1_df["ax2"].astype(np.float32)
        level1_df["ax3"] = level1_df["ax3"].astype(np.float32)
        level1_df["data"] = level1_df[["ax1", "ax2", "ax3"]].values.tolist()

        return level1_df

    def process_level_2(self, df):
        pass

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

        # check for 80 hz    microseconds in 81 hz < time_gap < microseconds in 79 hz
        if (
            dt.timedelta(microseconds=1 / 81 * 1e6) * multiplier
            <= time_gap
            <= dt.timedelta(microseconds=1 / 79 * 1e6) * multiplier
        ):
            return False

        # check for 10 hz    microseconds in 11 hz < time_gap < microseconds in 9 hz
        # technically it should be 125000 but just give it some leeway
        if (
            dt.timedelta(microseconds=1 / 11 * 1e6) * multiplier
            <= time_gap
            <= dt.timedelta(microseconds=1 / 9 * 1e6) * multiplier
        ):
            return True

        return None

    def decompress_df(self, df):
        """
        Decompresses a compressed df,
        returns a DataFrame of the data as though it were never compressed

        Best attempt to explain what's going on:
        - Prepare DataFrame for decompression
        - Loop through each row of the DataFrame (First Loop)
            - Each row starts off with the 'header', or original values
            - Then, we get 'non-header' packets that were appended to the row
                - To get the values for non-headers, we need to find reassemble the deltas and
                apply them to the packet before, similarly to EPD
                - We know the time difference because FGM is collected at either 10 Hz or 80 Hz
        - Once we have all the data from the DataFrame, put it into a new DataFrame to return,
        where the new DataFrame mimics a DataFrame of uncompressed Data (Second Loop)

        NOTE: The above explanation may have slightly incorrect syntax because I didn't
        write this and because I was looking through EPD Decompression before this
        """

        # Preparing the DataFrame that was received
        df["data"] = df["data"].apply(bytes.fromhex)
        df["idpu_time"] = df["data"].apply(lambda x: byte_tools.raw_idpu_bytes_to_datetime(x[:8]))
        df = df.sort_values(by=["idpu_time"])
        df = df.drop_duplicates(["idpu_time", "data"])

        # Holds the values obtained by decompressing each row.
        # Later is used to create the final DataFrame
        # vals is a list of tuples: (time, list of ax1 and ax2 and ax 3, bool indicating compression mode, packet_id)
        vals = []

        # Variables that keep track of last time, frequency
        prev_time = None
        is_10hz = None  # Default is 10 hz, but checking is done for each
        # is_10hz: True -> 10hz, False -> 80hz, None -> Neither (always dropped)

        for _, row in df.iterrows():
            data = row["data"]
            ts = row["idpu_time"]  # ts == timestamp == current time
            num = row["numerator"]
            packet_id = [x for x in row["packet_id"]]

            # find sampling rate based off of difference in time between 2 downlinked packets
            if prev_time:
                current_measure = self.is10hz_sampling_rate(prev_time, ts)
                if current_measure is not None:
                    is_10hz = current_measure
            prev_time = ts

            # Parsing the beginning of the data (time, ax1, ax2, ax3)
            start = [
                byte_tools.get_signed(data[8:11]),
                byte_tools.get_signed(data[11:14]),
                byte_tools.get_signed(data[14:17]),
            ]

            # Now, we've received the first decompressed row
            vals.append((ts, list(start), is_10hz, packet_id, num))

            # The rest of the data is put into bs so the code can look for more values
            bs = byte_tools.bin_string(data[17:])

            # hexits:
            #  20|16  12|8  4|0
            # Now parsing through the data that hasn't been looked at yet
            ind = 0  # Goes 0->1->2->0..., Basically says where to apply the delta in the 'start' list
            while len(bs) > 8:  # at least a byte
                signb = bs[:2]
                bs = bs[2:]

                # Handling Sign: Getting 11 does not necessarily mean the science processing code
                # is broken - the idpu code is set up to insert 11 if the the delta is too high
                # to be encoded (creating a 'marker' for that error). If you have the permissions
                # necessary, and want to view the idpu_code, the link is:
                # https://elfin-dev1.igpp.ucla.edu/repos/eng/FPGA/elfin_ns8/idpu_em/source/branches/akhil_branch/embedded/idpu_3
                if signb == "11":
                    self.log.debug(
                        f"⚠️\tProblem with Sign: Got 11, skipping packet at idpu_time {row['idpu_time']}, numerator {row['numerator']}"
                    )
                    break
                sign = -1 if signb == "01" else 1

                # Getting the deltas from the data, then applying them to get the actual values
                try:
                    hexit16, bs = byte_tools.get_huffman(bs, table=FGM_HUFFMAN)
                    hexit12, bs = byte_tools.get_huffman(bs, table=FGM_HUFFMAN)
                    hexit8 = int(bs[0:4], 2)
                    hexit4 = int(bs[4:8], 2)
                    bs = bs[8:]

                except IndexError:
                    self.log.warning("⚠️ Unable to decompress correctly")
                    continue

                # Apparently something changed in late March 2019?
                if ts.strftime("%Y-%m-%d") < "2019-03-25":
                    diff = sign * ((hexit16 << 16) + (hexit12 << 12) + (hexit8 << 8) + (hexit4 << 4))
                else:
                    diff = sign * ((hexit16 << 16) + (hexit12 << 12) + (hexit8 << 8) + (hexit4 << 4) << 1)

                # Update the appropriate value in start, and add a new item to vals if start if fully updated
                start[ind] += diff
                if ind == 2:
                    ts += dt.timedelta(seconds=(0.1 if is_10hz else 0.0125))  # packets are 100ms apart (10Hz)
                    vals.append(
                        (ts, list(start), is_10hz, [], num)
                    )  # packet_id # TODO: Find a better way to handle packets, not just the first one
                ind = (ind + 1) % 3

        # These lists become the columns for the final DataFrame,
        # each val provides a new item in each list
        new_ptypes = []
        new_time = []
        orig_num = []
        new_num = []
        new_denom = len(vals) - 1
        new_packets = []
        packet_ids = []
        track10hz = []

        # This loops through all of vals, adding to the lists to prepare for creating the returned DataFrame
        for i, val in enumerate(vals):
            ts, [ax1, ax2, ax3], is_10hz, p_id, num = val

            # Do this only if it is 10 or 80 hz (a valid frequency)
            if isinstance(is_10hz, bool):
                pb = byte_tools.get_three_signed_bytes(ax1)  # length = 16
                pb += byte_tools.get_three_signed_bytes(ax2)  # length = 19
                pb += byte_tools.get_three_signed_bytes(ax3)  # length = 22
                pb += b"\x00\x00\x00"  # fake HSKP, length: 25
                pb += byte_tools.get_two_unsigned_bytes(i)  # numerator, length: 27
                pb += byte_tools.get_two_unsigned_bytes(new_denom)  # denominator, length: 29
                pb += b"\x00"  # fake CRC
                new_packets.append(pb.hex())  # This is the 'data' column

                new_ptypes.append(0x1 if is_10hz else 0x17)  # This is how the code keeps track of type
                orig_num.append(num)  # Numerator of the frame that this packet came from. Useful for data tracking
                new_num.append(i)  # 'New' Numerator (packet-wise rather than frame-wise). Was original numerator but
                #     since it doesn't seem to be used, I've left it out of the DF that's returned
                new_time.append(val[0])  # Is this just ts?
                track10hz.append(val[2])  # This is how the code keeps track of frequency
                packet_ids.append(p_id)

        # Forming the final DF, preparing it (keep the correct frequency), then returning it
        df = pd.DataFrame(
            data={
                "mission_id": self.mission_id,
                "idpu_type": new_ptypes,
                "idpu_time": new_time,
                "numerator": orig_num,
                "denominator": new_denom,
                "data": new_packets,
                "packet_id": packet_ids,
                "10hz_mode": track10hz,
            }
        )
        df = self.drop_packets_by_freq(df)

        return df

    def drop_packets_by_freq(self, df):
        """ Returns a DataFrame with either 10 hz (fgs) or 80 hz (fgf) """
        if self.data_product_name == "fgf":
            df = df[df["10hz_mode"] is False]
        elif self.data_product_name == "fgs":
            df = df[df["10hz_mode"] is True]
        else:
            raise ValueError("Invalid data_product_name")

        return df

    def merge_processed_dataframes(self, dataframes):
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

        df = pd.concat(dataframes)
        df["idpu_type"] = df["idpu_type"].astype("category").cat.set_categories(self.idpu_types, ordered=True)

        df = df.dropna(subset=["data", "idpu_time"])
        df = df.sort_values(["idpu_time", "idpu_type"])

        rounded_idpu_time = pd.DatetimeIndex(df["idpu_time"])
        rounded_idpu_time = rounded_idpu_time.round("ms")
        df["rounded_idpu_time"] = rounded_idpu_time
        df = df.drop_duplicates("rounded_idpu_time", keep="first")
        df = df[["mission_id", "idpu_type", "idpu_time", "numerator", "denominator", "data", "10hz_mode", "packet_id"]]

        return df.reset_index()
