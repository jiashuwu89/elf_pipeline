"""Base processor for all processors of IDPU data

IDPU data processors usually need some methods to perform decompression
"""
import datetime as dt
from abc import abstractmethod

import numpy as np
import pandas as pd
from elfin.libelfin.utils import compute_crc
from spacepy import pycdf

from processor.science_processor import ScienceProcessor
from request.downlink_manager import DownlinkManager
from util import downlink_utils
from util.science_utils import dt_to_tt2000, s_if_plural


class IdpuProcessor(ScienceProcessor):
    def __init__(self, pipeline_config):
        super().__init__(pipeline_config)
        self.cdf_fields = []

        self.downlink_manager = DownlinkManager(pipeline_config.session)

    def generate_files(self, processing_request):
        l0_file_name, l0_df = self.generate_l0_products(processing_request)
        l1_file_name, _ = self.generate_l1_products(processing_request, l0_df)

        return [l0_file_name, l1_file_name]

    def generate_l0_products(self, processing_request):
        self.logger.info(">>> Generating Level 0 Products...")
        l0_df = self.generate_l0_df(processing_request)
        self.update_completeness_table(processing_request, l0_df)
        l0_file_name = self.generate_l0_file(processing_request, l0_df.copy())
        return l0_file_name, l0_df

    def generate_l0_df(self, processing_request):
        """Generate a dataframe of processed level 0 data given a specific collection date.

        All relevant downlinks are fetched, merged, and concatenated, and then
        passed separately (as a list) through process_l0.
        Finally, the individual dataframes are merged and duplicates/empty packets dropped.
        """
        self.logger.info(f"Creating level 0 DataFrame: {str(processing_request)}")

        dl_list = self.downlink_manager.get_relevant_downlinks(processing_request)  # TODO: By COLLECTION Time

        self.logger.info("Relevant downlinks:")
        self.downlink_manager.print_downlinks(dl_list)

        self.logger.info(f"Initial merging of {len(dl_list)} downlink{s_if_plural(dl_list)}...")
        merged_dfs = self.get_merged_dataframes(dl_list)
        self.logger.info(f"✔️ Merged to {len(merged_dfs)} downlink{s_if_plural(merged_dfs)}")

        self.logger.info("Rejoining frames into packets...")
        rejoined_dfs = [self.rejoin_data(processing_request, df) for df in merged_dfs]
        self.logger.info("✔️ Done rejoining frames")

        self.logger.info("Processing Level 0 packets...")
        dfs = [self.process_rejoined_data(processing_request, df) for df in rejoined_dfs]
        self.logger.info("✔️ Done processing Level 0 packets")

        self.logger.info("Final merge...")
        df = self.merge_processed_dataframes(dfs, processing_request.idpu_types)
        self.logger.info("✔️ Done with final merge")

        if df.empty:
            raise RuntimeError(f"Final Dataframe is empty: {str(processing_request)}")

        return df

    def update_completeness_table(self, processing_request, l0_df):
        self.logger.info("Updating completeness table")
        df = l0_df.copy()
        df = df[["idpu_time", "data"]].drop_duplicates().dropna()
        df_times = df["idpu_time"]

        completeness_updater = self.get_completeness_updater(processing_request)
        completeness_updater.update_completeness_table(df_times)  # TODO: Change EPD to EPDE or EPDI

    @abstractmethod
    def get_completeness_updater(self, processing_request):
        pass

    def generate_l0_file(self, processing_request, l0_df):
        # Filter fields and duplicates
        l0_df = l0_df[["idpu_time", "data"]]
        l0_df = l0_df.drop_duplicates().dropna()

        # Select Data belonging only to a certain day
        l0_df = l0_df[
            (l0_df["idpu_time"] >= processing_request.date)
            & (l0_df["idpu_time"] < processing_request.date + dt.timedelta(days=1))
        ]

        # TT2000 conversion
        l0_df["idpu_time"] = l0_df["idpu_time"].apply(pycdf.lib.datetime_to_tt2000)

        if l0_df.empty:
            raise RuntimeError(f"Empty level 0 DataFrame: {str(processing_request)}")

        # Generate L0 file
        fname = self.make_filename(processing_request.date, 0, l0_df.shape[0])
        l0_df.to_csv(fname, index=False)

        return fname, l0_df

    def generate_l1_products(self, processing_request, l0_df=None):
        """
        Generates level 1 CDFs for given collection time, optionally provided
        a level 0 dataframe (otherwise, generate_l0_df will be called again).

        Returns the path and name of the generated level 1 file.
        """
        l1_df = self.generate_l1_df(processing_request, l0_df)
        l1_file_name = self.generate_l1_file(processing_request, l1_df.copy())
        return l1_file_name, l1_df

    def generate_l1_df(self, processing_request, l0_df):
        if l0_df is None:
            l0_df = self.generate_l0_df(processing_request.date)

        # Allow derived class to transform data
        l1_df = self.transform_l0_df(processing_request, l0_df)

        # Timestamp conversion
        try:
            l1_df = l1_df[
                (l1_df["idpu_time"] >= processing_request.date)
                & (l1_df["idpu_time"] < processing_request.date + dt.timedelta(days=1))
            ]
            l1_df["idpu_time"] = l1_df["idpu_time"].apply(dt_to_tt2000)
            if l1_df.empty:
                raise RuntimeError(f"Final Dataframe is empty: {str(processing_request)}")

        except KeyError:
            self.logger.debug("The column 'idpu_time' does not exist, but it's probably OK")

        return l1_df

    @abstractmethod
    def transform_l0_df(self, processing_request, l0_df):
        pass

    def generate_l1_file(self, processing_request, l1_df):
        fname = self.make_filename(processing_request, 1)
        cdf = self.create_cdf(fname)
        self.fill_cdf(processing_request, cdf, l1_df)
        cdf.close()

        return fname, l1_df

    def get_merged_dataframes(self, downlinks):
        """Merge a list of downlinks, and retrieve their associated dataframes.

        Downlinks are merged only if they refer to the same physical range of
        packets onboard the MSP (they have matching IDPU_TYPE and overlapping IDPU_TIME)

        The resulting dataframe is sorted by:
        1. Packet Type
        2. IDPU Time

        Parameters:
        - downlinks: a tuple of the form `(mission_id, packet_type, first_time, last_time, denom,
            first_id, last_id, first_collect_time, last_collect_time)`

        Returns:
        - a tuple of: (concatenated dataframe, list of merged dataframes)
        """

        # TODO: Sort Downlinks by Downlink Time, and then by size
        if not downlinks:
            raise RuntimeError("No Downlinks to merge!")

        merged_downlinks = []
        first_dl = downlinks[0]
        m_idpu_type = first_dl.idpu_type
        m_first_time = first_dl.first_packet_info.idpu_time
        m_last_time = first_dl.last_packet_info.idpu_time
        m_df = self.downlink_manager.get_df_from_downlink(first_dl)

        for _, downlink in enumerate(downlinks[1:]):
            idpu_type = downlink.idpu_type
            first_time = downlink.first_packet_info.idpu_time
            last_time = downlink.last_packet_info.idpu_time
            df = self.downlink_manager.get_df_from_downlink(downlink)

            # Merge if we have found a good offset (downlink overlaps with the current one and packet type matches)
            offset = downlink_utils.calculate_offset(m_df, df)
            if idpu_type == m_idpu_type and offset is not None and m_first_time <= first_time <= m_last_time:
                m_last_time = max(m_last_time, last_time)
                m_df = downlink_utils.merge_downlinks(m_df, df, offset)
            else:
                merged_downlinks.append(m_df)
                m_first_time = first_time
                m_last_time = last_time
                m_idpu_type = idpu_type
                m_df = df

        merged_downlinks.append(m_df)

        return merged_downlinks

    def rejoin_data(self, processing_request, d):
        """
        Converts a dataframe of frames received from ELFIN into a dataframe of
        packets onboard the MSP's filesystem. This involves identifying the length
        of each packet, and concatenating consecutive frames into their respective packets.

        Frames partially composing an incomplete packet will be dropped, and a blank
        row will be inserted to identify that a packet is missing.
        """

        data = d["data"].apply(lambda x: None if pd.isnull(x) else bytes.fromhex(x))
        frames = d["packet_data"].apply(lambda x: None if pd.isnull(x) else bytes.fromhex(x))

        missing_numerators = []
        idpu_type = None
        denominator = 0

        final_df = pd.DataFrame()
        idx = 0

        while idx < d.shape[0]:
            numerator = d["numerator"].iloc[idx]

            if not data.iloc[idx]:
                self.logger.debug(f"Dropping idx={idx}: Empty data")
                missing_numerators.append(numerator)
                idx += 1
                continue

            if not idpu_type:  # get default data to give to missing frames
                idpu_type = d["idpu_type"].iloc[idx]
                denominator = d["denominator"].iloc[idx]

            cur_data = data.iloc[idx]
            cur_frame = frames.iloc[idx]
            cur_row = d.iloc[idx].copy()

            current_length = len(cur_data)

            # TODO: make sure this is ok (see orig implementation)
            # Making sure this frame has a header
            if compute_crc(0xFF, cur_frame[1:12]) != cur_frame[12]:
                self.logger.debug(f"Dropping idx={idx}: Probably not a header\n")
                missing_numerators.append(numerator)
                idx += 1
                continue
            expected_length = int.from_bytes(cur_frame[1:3], "little", signed=False) // 2 - 12

            try:
                while current_length < expected_length:
                    idx += 1
                    cur_data += data.iloc[idx]
                    current_length = len(cur_data)
            except Exception as e:  # Missing packet (or something else)
                self.logger.debug(f"Dropping idx={idx}: Empty continuation (Exception={e})\n")
                missing_numerators.append(numerator)
                idx += 1
                continue

            if current_length != expected_length:
                self.logger.debug(f"Dropping idx={idx}: Cur len {current_length} != Expected len {expected_length}\n")
                missing_numerators.append(numerator)
                idx += 1
                continue

            # Add good row to final_df
            cur_row.loc["data"] = cur_data.hex()
            final_df = final_df.append(cur_row)

            idx += 1

        missing_frames = {
            "id": None,
            "mission_id": processing_request.mission_id,
            "idpu_type": idpu_type,
            "idpu_time": None,
            "data": None,
            "numerator": pd.Series(missing_numerators),
            "denominator": denominator,
            "packet_data": None,
            "timestamp": None,
        }

        final_df = (
            final_df.append(pd.DataFrame(data=missing_frames), sort=False)
            .sort_values("numerator")
            .reset_index(drop=True)
        )

        return final_df[["timestamp", "mission_id", "idpu_type", "idpu_time", "numerator", "denominator", "data"]]

    @abstractmethod
    def process_rejoined_data(self, processing_request, df):
        pass

    def merge_processed_dataframes(self, dfs, idpu_types):
        """
        Given a list of dataframes of identical format (decompressed/raw, level 0),
        merge them in a way such that duplicate frames are removed.

        Preference is given in the same order as which IDPU_TYPEs
        appear in the list self.idpu_types.
        """
        self.logger.debug("Merging processed dataframes")
        df = pd.concat(dfs)

        df["idpu_type"] = df["idpu_type"].astype("category").cat.set_categories(idpu_types, ordered=True)
        df = df.dropna(subset=["data", "idpu_time"])
        df = df.sort_values(["idpu_time", "idpu_type"])

        # Keeping the first item means that the first/earlier idpu_type will be preserved
        # idpu_type is ordered in the same order as self.idpu_types
        df = df.drop_duplicates("idpu_time", keep="first")

        return df.reset_index()

    def fill_cdf(self, processing_request, df, cdf):
        """Inserts data from df into a CDF file

        Parameters
        ==========
        df
        cdf
        """
        for key in self.get_cdf_fields(processing_request):
            df_field_name = self.cdf_fields[key]
            cdf_field_name = f"{processing_request.probe}_{key}"

            if cdf_field_name in cdf.keys() and df_field_name in df.columns:
                data = df[df_field_name].values
                # numpy array with lists need to be converted to a multi-dimensional numpy array of numbers
                if isinstance(data[0], list):
                    data = np.stack(data)

                cdf[cdf_field_name] = data

    @abstractmethod
    def get_cdf_fields(self, processing_request):
        pass
