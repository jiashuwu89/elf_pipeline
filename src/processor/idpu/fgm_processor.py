"""Class to generate FGM files"""
import datetime as dt
from enum import Enum
from typing import Dict, List, Tuple, Union

import numpy as np
import pandas as pd
import requests
from spacepy import pycdf

from data_type.completeness_config import COMPLETENESS_CONFIG_MAP, FGM_COMPLETENESS_CONFIG
from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from output.downlink.downlink_manager import DownlinkManager
from output.metric.completeness import CompletenessUpdater
from processor.idpu.idpu_processor import IdpuProcessor
from util import byte_tools
from util.compression_values import FGM_HUFFMAN
from util.constants import BITS_IN_BYTE, SP_SERVER_URL
from util.science_utils import hex_to_int

# TODO: Hardcoded values -> Constants


class FgmRow:
    def __init__(self, idpu_time, idpu_type, axes, frequency, numerator):
        self.idpu_time = idpu_time
        self.idpu_type = idpu_type
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
    UNKNOWN = -1


class FgmProcessor(IdpuProcessor):
    """This class processes level 0/1 fgf and fgs data.

    The FgmProcessor class handles several things, including:
        * Decompression of fgm
        * Filtering of data based on collection frequency

    Parameters
    ----------
    pipeline_config : PipelineConfig
    downlink_manager : DownlinkManager
    """

    def __init__(self, pipeline_config: PipelineConfig, downlink_manager: DownlinkManager):
        super().__init__(pipeline_config, downlink_manager)

    def process_rejoined_data(self, processing_request: ProcessingRequest, df: pd.DataFrame) -> pd.DataFrame:
        """Return a dataframe corresponding to correctly formatted data products

        For uncompressed data, select data only if it is at the correct
        sampling rate. For compressed data, decompress the data.

        NOTE: idpu_time is formatted as a datetime

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame
            A DataFrame of rejoined data

        Returns
        -------
        pd.DataFrame
            A DataFrame of correctly formatted data products
        """
        df = df.dropna(subset=["idpu_time", "data"])
        df = df.reset_index(drop=True)

        # Checking for compressed and uncompressed data
        uncompressed = df["idpu_type"].isin([1, 17]).any()
        compressed = df["idpu_type"].isin([2, 18]).any()

        if uncompressed and compressed:  # TODO: Handle both uncompressed and compressed data if found together
            self.logger.warning("⚠️ Detected both compressed and uncompressed data. This should never happen...")
            return df[df["idpu_type"].isin([1, 17])]

        if uncompressed:
            df = self.update_uncompressed_df(processing_request, df)
        elif compressed:
            df = self.decompress_df(processing_request, df)
        else:
            self.logger.warning("⚠️ Detected neither compressed nor uncompressed data.")

        return df

    # TODO: Add ProcessingRequest as parameter to EPD Processor, standardize!
    # TODO: Standardize process_rejoined_data
    def update_uncompressed_df(self, processing_request: ProcessingRequest, df: pd.DataFrame) -> pd.DataFrame:
        """Obtains a DataFrame of FGM data with the correct sampling rate.

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame
            A DataFrame of uncompressed data

        Returns
        -------
        pd.DataFrame
            A DataFrame of FGM data with the correct sampling rate
        """
        df["sampling_rate"] = self.find_diff(df)
        df["10hz_mode"] = df["sampling_rate"].apply(
            lambda x: self.check_sampling_rate(x.to_pytimedelta(), multiplier=1)
        )
        df = self.drop_packets_by_freq(processing_request, df)

        return df[["mission_id", "idpu_type", "idpu_time", "numerator", "denominator", "data", "10hz_mode"]]

    @staticmethod
    def find_diff(df: pd.DataFrame) -> pd.DataFrame:
        """Given a DataFrame of FGM Data, find the time deltas between rows.

        This serves primarily as a helper function to find the time
        difference between two packets. It is used to help determine sampling
        rate.

        TODO: This is implemented kind of strangely

        Parameters
        ----------
        df : pd.DataFrame
            A DataFrame of uncompressed FGM data

        Returns
        -------
        pd.DataFrame
            A copy of the provided DataFrame, with a new column
            "sampling_rate" to indicate the time difference from the previous
            row
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

    def generate_l1_products(
        self, processing_request: ProcessingRequest, l0_df: pd.DataFrame = None
    ) -> Tuple[str, pd.DataFrame]:
        """Generates level 1 products related to the ProcessingRequest.

        Parameters
        ----------
        processing_request: ProcessingRequest
        l0_df : pd.DataFrame
            A DataFrame of finalized level 0 data corresponding to the
            ProcessingRequest. If it is not provided, the method will
            generate the level 0 DataFrame internally

        Returns
        -------
        Tuple[str, pd.DatatFrame]
            A tuple of the generated file's name, and a DataFrame of level 1
            data corresponding to the ProcessingRequest
        """
        self.logger.info(f"🟢  Generating Level 1 products for {str(processing_request)}")
        l1_df = self.generate_l1_df(processing_request, l0_df)
        l1_file_name, _ = self.generate_l1_file(processing_request, l1_df.copy())

        # Adds fsp data to the CDF
        # NOTE: This is a bit hacky to avoid too many changes, but probably
        # should be incorporated into generate_l1_file (which would require
        # many more changes to handle multiple dfs)
        temp_l1_df = l1_df.copy()
        temp_l1_df["idpu_time"] = temp_l1_df["idpu_time"].apply(pycdf.lib.tt2000_to_datetime)
        fsp_df = self.generate_fsp_df(processing_request, temp_l1_df)
        if not fsp_df.empty:
            cdf = pycdf.CDF(l1_file_name)
            cdf.readonly(False)
            self.fill_cdf(
                processing_request,
                cdf,
                fsp_df,
                {
                    f"{processing_request.probe}_fgs_fsp_time": "fgs_fsp_time",
                    f"{processing_request.probe}_fgs_fsp_res_dmxl": "fgs_fsp_res_dmxl",
                    f"{processing_request.probe}_fgs_fsp_res_dmxl_trend": "fgs_fsp_res_dmxl_trend",
                    f"{processing_request.probe}_fgs_fsp_res_gei": "fgs_fsp_res_gei",
                    f"{processing_request.probe}_fgs_fsp_igrf_dmxl": "fgs_fsp_igrf_dmxl",
                    f"{processing_request.probe}_fgs_fsp_igrf_gei": "fgs_fsp_igrf_gei",
                },
            )
            cdf.close()

        return l1_file_name, l1_df

    def transform_l0_df(self, processing_request: ProcessingRequest, l0_df: pd.DataFrame) -> pd.DataFrame:
        """Does the necessary processing on a l0 df to create a l1 df.

        Parameters
        ----------
        processing_request : ProcessingRequest
        l0_df : pd.DataFrame
            A DataFrame of completely processed level 0 FGM data

        Returns
        -------
        pd.DataFrame
            A DataFrame of level 1 FGM data, based on the given DataFrame
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

    def generate_fsp_df(self, processing_request: ProcessingRequest, l1_df: pd.DataFrame) -> pd.DataFrame:
        """Creates a DataFrame of FSP-related products

        Parameters
        ----------
        processing_request : ProcessingRequest
        l1_df : pd.DataFrame
            A DataFrame of completely processed level 1 FGM data

        Returns
        -------
        pd.DataFrame
            A DataFrame of FSP-related products
        """

        # TODO: Only set up for fgs right now, is this right?
        if processing_request.data_product != "fgs":
            return l1_df

        cu = CompletenessUpdater(self.session, COMPLETENESS_CONFIG_MAP)  # Used to split science zones, a bit hacky

        fsp_df = pd.DataFrame(
            columns=[
                "fgs_fsp_time",
                "fgs_fsp_res_dmxl",
                "fgs_fsp_res_dmxl_trend",
                "fgs_fsp_res_gei",
                "fgs_fsp_igrf_dmxl",
                "fgs_fsp_igrf_gei",
            ]
        )
        Gthphi_columns = ['start_time','end_time','G1', 'G2', 'G3', 'th1','th2','th3', 'ph1','ph2','ph3', 'O1/G1','O2/G2','O3/G3']
        Bpara_columns = ['start_time','end_time','G11', 'G12', 'G13', 'O1', 'G21','G22','G23', 'O2', 'G31','G32','G33', 'O3']

        B_parameter = pd.DataFrame(columns = Bpara_columns)
        Gthphi_parameter = pd.DataFrame(columns = Gthphi_columns)

        science_zone_groups = cu.split_science_zones(processing_request, FGM_COMPLETENESS_CONFIG, l1_df["idpu_time"])
        for science_zone_group in science_zone_groups:
            science_zone_df = l1_df.loc[l1_df["idpu_time"].between(science_zone_group[0], science_zone_group[-1])]

            # NOTE: Dependency on `sp-server`
            response = requests.post(
                f"{SP_SERVER_URL}/fgm_calib/fgm_calib",
                headers={"Content-Type": "application/json", "accept": "application/json"},
                json={
                    "mission_id": processing_request.mission_id,
                    "fgs_time": [t.isoformat() for t in science_zone_df["idpu_time"]],
                    "fgs": list(science_zone_df["data"]),
                },
            )

            if response.status_code != 200:
                self.logger.warning(
                    f"Bad response for science zone between {science_zone_group[0]} and {science_zone_group[-1]}. "
                    + f"Status Code: {response.status_code}, Reason: {response.reason}"
                )
                continue

            response_json = response.json()

            to_add_df = pd.DataFrame(
                {
                    "fgs_fsp_time": response_json["fgs_fsp_time"],
                    "fgs_fsp_res_dmxl": response_json["fgs_fsp_res_dmxl"],
                    "fgs_fsp_res_dmxl_trend": response_json["fgs_fsp_res_dmxl_trend"],
                    "fgs_fsp_res_gei": response_json["fgs_fsp_res_gei"],
                    "fgs_fsp_igrf_dmxl": response_json["fgs_fsp_igrf_dmxl"],
                    "fgs_fsp_igrf_gei": response_json["fgs_fsp_igrf_gei"],
                }
            )
            to_add_df["fgs_fsp_time"] = to_add_df["fgs_fsp_time"].apply(
                lambda x: pycdf.lib.datetime_to_tt2000(dt.datetime.fromisoformat(x))
            )

            fsp_df = pd.concat([fsp_df, to_add_df], axis=0, ignore_index=True)
	    
            #generate calib parameter df

            if response_json["fgs_fsp_time"] != []:
                starttime_str = dt.datetime.fromisoformat(response_json["fgs_fsp_time"][0]).strftime('%Y-%m-%d/%H:%M:%S.%f')
                endtime_str = dt.datetime.fromisoformat(response_json["fgs_fsp_time"][-1]).strftime('%Y-%m-%d/%H:%M:%S.%f')
                B_df = pd.DataFrame([[starttime_str, endtime_str] + response_json["B_parameter"]], columns=Bpara_columns)
                B_parameter = pd.concat([B_parameter, B_df], axis=0, ignore_index=True)
                Gthphi_df = pd.DataFrame([[starttime_str, endtime_str] + response_json["Gthphi_parameter"]], columns=Gthphi_columns)
                Gthphi_parameter = pd.concat([Gthphi_parameter, Gthphi_df], axis=0, ignore_index=True)

        #output parameter df to csv file
        startime_str_allzone = science_zone_groups[0][0].strftime('%Y-%m-%d/%H:%M:%S')
        endtime_str_allzone = science_zone_groups[-1][-1].strftime('%Y-%m-%d/%H:%M:%S')
        mission = 'ela' if processing_request.mission_id == 1 else 'elb'
        B_parameter.to_csv(f"/home/elfin/fgm-testing/calibpara_csv/{startime_str_allzone[0:10]}_{startime_str_allzone[11:13]}{startime_str_allzone[14:16]}_{endtime_str_allzone[0:10]}_{endtime_str_allzone[11:13]}{endtime_str_allzone[14:16]}_{mission}_Bpara.csv", index=False)
        Gthphi_parameter.to_csv(f"/home/elfin/fgm-testing/calibpara_csv/{startime_str_allzone[0:10]}_{startime_str_allzone[11:13]}{startime_str_allzone[14:16]}_{endtime_str_allzone[0:10]}_{endtime_str_allzone[11:13]}{endtime_str_allzone[14:16]}_{mission}_Gthphi.csv", index=False)

        return fsp_df


    def is10hz_sampling_rate(
        self, first_ts: pd.Timestamp, second_ts: pd.Timestamp, multiplier: int
    ) -> FgmFrequencyEnum:
        """Checks the sampling rate between two times.

        This function actually checks if this is 80 hz or 10 hz.
        If it is 10hz, returns True, if is 80 hz, returns False,
        if it's neither, return None
        SHOULD ONLY BE USED FOR COMPRESSED

        TODO: probably a better way to write this lol

        Parameters
        ----------
        first_ts : pd.Timestamp
        second_ts : pd.Timestamp
        multiplier: int
            The number of packets compressed into a compressed packet. Can
            be calculated with method packets_in_compressed_packet. If not
            compressed, multiplier should probably be 1.

        Returns
        -------
        FgmFrequencyEnum
        """
        time_gap = second_ts - first_ts
        return self.check_sampling_rate(time_gap, multiplier)

    @staticmethod
    def check_sampling_rate(time_gap: Union[pd.Timedelta, dt.timedelta], multiplier: int) -> FgmFrequencyEnum:
        """Given a time delta, determine the frequency.

        compressed is 10 decompressed packets per packet, so expect 1/freq*10
        per compressed packet

        Parameters
        ----------
        time_gap : Union[pd.Timedelta, dt.timedelta]
            The time between two packets. One TODO is to eliminate the usage
            of two distinct representations of time and timedelta (pandas
            and datetime), as this just introduces unnecessary confusion
        multiplier: int
            The number of packets compressed into a compressed packet. Can
            be calculated with method packets_in_compressed_packet. If not
            compressed, multiplier should be 1.

        Returns
        -------
        FgmFrequencyEnum
            An enum with possible values of 10 hz, 80 hz, or neither
        """
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

    @staticmethod
    def packets_in_compressed_packet(idpu_time: Union[pd.Timestamp, dt.datetime]) -> int:
        """Determines the number of packets in a compressed packet.

        Used primarily in method check_sample_rate. From the start of the
        mission until 2021-02-26 18:00:00, there were 10 packets compressed
        into a compressed packet. Due to IBO, FGM changes were made such that
        compressed packets after 2021-02-26 18:00:00 contain 25 packets.

        TODO: An alternate approach to be explored is to empirically determine
        the number of packets by counting as we parse each packet. This should
        be implemented when this code is refactored.

        Parameters
        ----------
        idpu_time: Union[pd.Timedelta, dt.timedelta]
            Compression time of packet

        Returns
        -------
        int
            The number of decompressed packets in a compressed packet.
        """
        return 10 if idpu_time < dt.datetime(2021, 2, 26, 18) else 25

    def decompress_df(self, processing_request: ProcessingRequest, df: pd.DataFrame) -> pd.DataFrame:
        """Decompresses a compressed df of FGM data.

        Best attempt to explain what's going on:
            * Prepare DataFrame for decompression
            * Loop through each row of the DataFrame (First Loop). Each row
            starts off with the 'header', or original values. Then, we get
            'non-header' packets that were appended to the row. To get the
            values for non-headers, we need to find reassemble the deltas and
            apply them to the packet before, similarly to EPD (we know the
            time difference because FGM is collected at either 10 Hz or 80 Hz)
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
        df["multiplier"] = df["idpu_time"].apply(self.packets_in_compressed_packet)
        df["idpu_time"] = df["data"].apply(lambda x: byte_tools.raw_idpu_bytes_to_datetime(x[:8]))
        df = df.sort_values(by=["idpu_time"]).drop_duplicates(["idpu_time", "data"])

        decompressed_rows = []

        prev_time = None
        frequency = FgmFrequencyEnum.UNKNOWN  # Default is 10 hz, but checking is done for each

        for _, row in df.iterrows():
            idpu_time = row["idpu_time"]

            # find sampling rate between current and previous packets
            if prev_time:
                if self.is10hz_sampling_rate(prev_time, idpu_time, row["multiplier"]) != FgmFrequencyEnum.UNKNOWN:
                    frequency = self.is10hz_sampling_rate(prev_time, idpu_time, row["multiplier"])
            prev_time = idpu_time

            axes_values = [
                byte_tools.get_signed(row["data"][8:11]),
                byte_tools.get_signed(row["data"][11:14]),
                byte_tools.get_signed(row["data"][14:17]),
            ]

            # Create the initial decompressed row, later decompressed rows will follow
            decompressed_rows.append(
                FgmRow(idpu_time, row["idpu_type"], axes_values.copy(), frequency, row["numerator"])
            )

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
                    decompressed_rows.append(
                        FgmRow(idpu_time, row["idpu_type"], axes_values.copy(), frequency, row["numerator"])
                    )

                axis_index = (axis_index + 1) % 3

        return self.create_decompressed_df_from_rows(processing_request, decompressed_rows)

    def get_delta(self, bs: str, idpu_time: pd.Timestamp) -> Tuple[Union[int, None], str]:
        """Attempts to obtain a delta from packet data.

        This is a helper function to help with decompression. The delta should
        be applied to the previous corresponding axis value (see usage in
        decompress_df).

        This delta is obtained by looking at the beginning of 'bs' and
        consuming enough data to get the sign and magnitude information. The
        remaining data in 'bs' contains additional deltas that can be found
        later by calling get_delta again.

        Parameters
        ----------
        bs : str
        idpu_time : pd.Timestamp

        Returns
        -------
        Tuple[Union[int, None], str]
            If a delta can be found, returns the delta and the remaining data.
            Otherwise, returns None and the remaining data.
        """
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
    def calculate_delta_magnitude(idpu_time: pd.Timestamp, hexit16: int, hexit12: int, hexit8: int, hexit4: int) -> int:
        """Calculates the magnitude of the delta to be applied to an axis.

        Parameters
        ----------
        idpu_time : pd.Timestamp
        hexit16 : int
        hexit12 : int
        hexit8 : int
        hexit4: int

        Returns
        -------
        int
            An integer representing the magnitude of the delta that should be
            applied.
        """
        if idpu_time.strftime("%Y-%m-%d") < "2019-03-25":  # Apparently something changed in late March 2019?
            return (hexit16 << 16) + (hexit12 << 12) + (hexit8 << 8) + (hexit4 << 4)
        return (hexit16 << 16) + (hexit12 << 12) + (hexit8 << 8) + (hexit4 << 4) << 1

    def create_decompressed_df_from_rows(self, processing_request, decompressed_rows):
        """Convert rows of decompressed FGM data to a DataFrame.

        # TODO: Typing is too annoying for me to update, but type annotations
        # should be added

        Parameters
        ----------
        processing_request : ProcessingRequest
        decompressed_rows

        Returns
        -------
        pd.DataFrame
            A DataFrame of decompressed FGM data
        """
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
        # idpu_type was initially found with: 0x1 if row.frequency == FgmFrequencyEnum.TEN_HERTZ else 0x17
        # However, it doesn't seem to be used elsewhere, so preserve original idpu_type to help with completeness
        for i, row in enumerate(decompressed_rows):
            if row.frequency != FgmFrequencyEnum.UNKNOWN:
                df_dict["mission_id"].append(processing_request.mission_id)
                df_dict["idpu_type"].append(row.idpu_type)
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

    @staticmethod
    def create_new_packet(axes: List[int], numerator: int, denominator: int) -> str:
        """Recreate a packet as if it were onboard ELA/ELB

        Parameters
        ----------
        axes : List[int]
            A list of three integers, representing values for X, Y, and Z
        numerator : int
        denominator : int

        Returns
        -------
        str
            A string representing a packet formatted as though it were
            uncompressed data still on ELA/ELB
        """
        ax1, ax2, ax3 = axes

        pb = byte_tools.get_three_signed_bytes(ax1)  # length = 16
        pb += byte_tools.get_three_signed_bytes(ax2)  # length = 19
        pb += byte_tools.get_three_signed_bytes(ax3)  # length = 22
        pb += b"\x00\x00\x00"  # fake HSKP, length: 25
        pb += byte_tools.get_two_unsigned_bytes(numerator)  # numerator, length: 27
        pb += byte_tools.get_two_unsigned_bytes(denominator)  # denominator, length: 29
        pb += b"\x00"  # fake CRC

        return pb.hex()

    @staticmethod
    def drop_packets_by_freq(processing_request: ProcessingRequest, df: pd.DataFrame) -> pd.DataFrame:
        """Returns a DataFrame with either 10 hz (fgs) or 80 hz (fgf) data.

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame

        Returns
        -------
        pd.DataFrame
        """
        if processing_request.data_product == "fgf":
            return df[df["10hz_mode"] == FgmFrequencyEnum.EIGHTY_HERTZ]
        if processing_request.data_product == "fgs":
            return df[df["10hz_mode"] == FgmFrequencyEnum.TEN_HERTZ]

        raise ValueError("Invalid data_product_name")

    def merge_processed_dataframes(self, dfs: List[pd.DataFrame], idpu_types: List[int]) -> pd.DataFrame:
        """FGM-specific version of merge_processed_dataframes.

        Times are rounded to nearest millisecond to determine if packets are
        the same.

        Given a list of dataframes of identical format (decompressed/raw, level 0),
        merge them in a way such that duplicate frames are removed.

        Preference is given in the same order as which IDPU_TYPEs appear in the list
        self.idpu_types. Keeping the first item means that the first/earlier
        idpu_type will be preserved. idpu_type is ordered in the same order as
        self.idpu_types

        Parameters
        ----------
        dfs : List[pd.DataFrame]
            A List of DataFrames to be merged
        idpu_types : List[int]

        Returns
        -------
        pd.DataFrame
            A DataFrame of merged FGM data
        """
        df = pd.concat(dfs)
        df["idpu_type"] = df["idpu_type"].astype("category").cat.set_categories(idpu_types, ordered=True)

        df = df.dropna(subset=["data", "idpu_time"])
        df = df.sort_values(["idpu_time", "idpu_type"])

        df["rounded_idpu_time"] = pd.DatetimeIndex(df["idpu_time"]).round("ms")
        df = df.drop_duplicates("rounded_idpu_time", keep="first")
        df = df[["mission_id", "idpu_type", "idpu_time", "numerator", "denominator", "data", "10hz_mode"]]

        return df.reset_index()

    def get_cdf_fields(self, processing_request: ProcessingRequest) -> Dict[str, str]:
        """Gets a map of relevant CDF fields for FGM data to DF column names.

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
        probe = processing_request.probe
        data_product = processing_request.data_product

        return {
            f"{probe}_{data_product}": "data",
            f"{probe}_{data_product}_time": "idpu_time",
        }
