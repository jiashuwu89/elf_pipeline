from abc import abstractmethod
from collections import defaultdict
from typing import List, Tuple

import numpy as np
import pandas as pd
from elfin.libelfin.utils import compute_crc
from spacepy import pycdf

from data_type.completeness_config import COMPLETENESS_CONFIG_MAP
from data_type.downlink import Downlink
from data_type.exception import EmptyException
from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from output.downlink import downlink_utils
from output.downlink.downlink_manager import DownlinkManager
from output.metric.completeness import CompletenessUpdater
from processor.science_processor import ScienceProcessor
from util.constants import ONE_DAY_DELTA
from util.science_utils import dt_to_tt2000, s_if_plural


class IdpuProcessor(ScienceProcessor):
    """A processor that serves as the base for processors of IDPU data.

    NOTE: IDPU data processors usually need some methods to perform
    decompression.

    Parameters
    ----------
    pipeline_config : PipelineConfig
        An object storing user-defined settings for the pipeline
    downlink_manager : DownlinkManager
        An object that calculates and obtains Downlinks
    """

    def __init__(self, pipeline_config: PipelineConfig, downlink_manager: DownlinkManager):
        super().__init__(pipeline_config)

        self.downlink_manager = downlink_manager
        self.completeness_updater = CompletenessUpdater(pipeline_config.session, COMPLETENESS_CONFIG_MAP)

    def generate_files(self, processing_request: ProcessingRequest) -> List[str]:
        """Creates level 0 and 1 files for the given processing request.

        Parameters
        ----------
        processing_request : ProcessingRequest

        Returns
        -------
        List[str]
            A list of two filenames, representing the level 0 and 1 files
        """
        l0_file_name, l0_df = self.generate_l0_products(processing_request)
        l1_file_name, _ = self.generate_l1_products(processing_request, l0_df)

        return [l0_file_name, l1_file_name]

    def generate_l0_products(self, processing_request: ProcessingRequest) -> Tuple[str, pd.DataFrame]:
        """Creates the level 0 products associated the processing request.

        Parameters
        ----------
        processing_request : ProcessingRequest

        Returns
        -------
        (str, pd.DataFrame)
            A tuple of the level 0 filename, and a DataFrame of level 0 data
        """
        self.logger.info(f"🔴  Generating Level 0 products for {str(processing_request)}")
        l0_df = self.generate_l0_df(processing_request)
        self.update_completeness_table(processing_request, l0_df)
        l0_file_name, _ = self.generate_l0_file(processing_request, l0_df.copy())
        return l0_file_name, l0_df

    def generate_l0_df(self, processing_request: ProcessingRequest) -> pd.DataFrame:
        """Generate a DataFrame of level 0 data given a processing_request.

        All relevant downlinks are fetched, merged, and concatenated, and then
        passed separately (as a list) through process_l0. Finally, the
        individual dataframes are merged and duplicates/empty packets dropped.

        Parameters
        ----------
        processing_request : ProcessingRequest

        Returns
        -------
        pd.DataFrame
            A DataFrame of level 0 data relating to the given processing
            request
        """
        self.logger.info(f"🟠  Generating Level 0 DataFrame for {str(processing_request)}")
        dl_list = self.downlink_manager.get_relevant_downlinks(processing_request)  # TODO: By COLLECTION Time
        self.downlink_manager.print_downlinks(dl_list, "Relevant downlinks")

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
            raise EmptyException(f"Final Dataframe is empty: {str(processing_request)}")

        return df

    def get_merged_dataframes(self, downlinks: List[Downlink]) -> List[pd.DataFrame]:
        """Merges a list of downlinks and retrieves associated dataframes.

        Downlinks are merged only if they refer to the same physical range of
        packets onboard the MSP (they have matching IDPU_TYPE and overlapping
        IDPU_TIME)

        The resulting dataframe is sorted by Packet Type, then IDPU Time

        Parameters
        ----------
        downlinks : List[Downlink]
            A list of Downlink objects, all with the same mission id

        Returns
        -------
        List[pd.DataFrame]
            A list of DataFrames of data that was merged
        """

        downlinks = [
            dl for dl in downlinks if dl.first_packet_info.science_packet_id and dl.last_packet_info.science_packet_id
        ]
        downlinks = sorted(downlinks)  # TODO: See if sorting should be updated, and see if we can avoid omitting dls
        if not downlinks:
            raise RuntimeError("No Downlinks to merge!")

        mission_ids = {dl.mission_id for dl in downlinks}
        if len(mission_ids) != 1:
            raise ValueError(f"Expected all downlinks to have the same mission_id, got {mission_ids}")

        # Avoid merging downlinks with differing mission_id or idpu_type
        grouped_downlinks = defaultdict(list)
        for dl in downlinks:
            grouped_downlinks[(dl.mission_id, dl.idpu_type)].append(dl)

        return [
            df for dls in grouped_downlinks.values() for df in self._get_merged_dataframes_from_grouped_downlinks(dls)
        ]

    def _get_merged_dataframes_from_grouped_downlinks(self, downlinks: List[Downlink]) -> List[pd.DataFrame]:
        """Helper for get_merged_dataframes.

        Parameters
        ----------
        downlinks : List[Downlink]
            A non-empty list of Downlink objects, all with the same mission id
            and idpu type

        Returns
        -------
        List[pd.DataFrame]
            A list of DataFrames of data that was merged
        """
        if len(downlinks) == 0:
            raise RuntimeError("No Downlinks to merge!")

        mission_ids = {dl.mission_id for dl in downlinks}
        if len(mission_ids) != 1:
            raise ValueError(f"Expected all downlinks to have the same mission_id, got {mission_ids}")

        idpu_types = {dl.idpu_type for dl in downlinks}
        if len(idpu_types) != 1:
            raise ValueError(f"Expected all downlinks to have the same idpu type, got {idpu_types}")

        downlinks = sorted(downlinks)

        merged_downlinks = []
        first_dl = downlinks[0]
        m_first_time = first_dl.first_packet_info.idpu_time
        m_last_time = first_dl.last_packet_info.idpu_time
        m_df = self.downlink_manager.get_df_from_downlink(first_dl)

        for _, downlink in enumerate(downlinks[1:]):
            first_time = downlink.first_packet_info.idpu_time
            last_time = downlink.last_packet_info.idpu_time
            df = self.downlink_manager.get_df_from_downlink(downlink)

            # Merge if we have found a good offset (downlink overlaps with the current one and packet type matches)
            offset = downlink_utils.calculate_offset(m_df, df)
            if offset is not None and m_first_time <= first_time <= m_last_time:
                m_last_time = max(m_last_time, last_time)
                m_df = downlink_utils.merge_downlinks(m_df, df, offset)
            else:
                merged_downlinks.append(m_df)
                m_first_time = first_time
                m_last_time = last_time
                m_df = df

        merged_downlinks.append(m_df)

        return merged_downlinks

    def rejoin_data(self, processing_request: ProcessingRequest, d: pd.DataFrame) -> pd.DataFrame:
        """Converts frames from ELFIN into packets on the MSP's file system.

        Performs a best effort conversion, as some frames may be missing (and
        other frames may need to be dropped). This method attempts to use the
        given frames to recreate the packets as they existed in the MSP.

        This involves identifying the length of each packet, and concatenating
        consecutive frames into their respective packets. Frames partially
        composing an incomplete packet will be dropped, and a blank row will
        be inserted to identify that a packet is missing.

        Parameters
        ----------
        processing_request : ProcessingRequest
        d : pd.DataFrame
            A DataFrame of frames, as obtained from get_merged_dataframes

        Returns
        -------
        pd.DataFrame
            A DataFrame of packets
        """

        data = d["data"].apply(lambda x: None if pd.isnull(x) else bytes.fromhex(x))
        frames = d["packet_data"].apply(lambda x: None if pd.isnull(x) else bytes.fromhex(x))

        missing_numerators = []
        idpu_type = None
        denominator = 0

        final_df = pd.DataFrame()
        idx = 0

        while idx < d.shape[0]:
            if not data.iloc[idx]:
                self.logger.debug(f"Dropping idx={idx}: Empty data")
                missing_numerators.append(d["numerator"].iloc[idx])
                idx += 1
                continue

            if not idpu_type:  # get default data to give to missing frames
                idpu_type = d["idpu_type"].iloc[idx]
                denominator = d["denominator"].iloc[idx]

            cur_data = data.iloc[idx]
            cur_row = d.iloc[idx].copy()

            # Making sure this frame has a header
            try:
                # make sure the CRC is ok, then remove header
                if compute_crc(0xFF, frames.iloc[idx][1:12]) != frames.iloc[idx][12]:
                    raise Exception(f"Bad CRC at {idx}")
                expected_length = int.from_bytes(frames.iloc[idx][1:3], "little", signed=False) // 2 - 12

            except Exception as e:
                self.logger.debug(f"Dropping idx={idx}: Probably not a header - {e}\n")
                missing_numerators.append(d["numerator"].iloc[idx])
                idx += 1
                continue

            try:
                while len(cur_data) < expected_length:
                    idx += 1
                    cur_data += data.iloc[idx]
            except IndexError:
                self.logger.debug("End of DataFrame!")
                missing_numerators.append(d["numerator"].iloc[-1])
                continue
            except Exception as e:  # Missing packet (or something else)
                self.logger.debug(f"Dropping idx={idx}: Empty continuation (Exception={e})\n")
                missing_numerators.append(d["numerator"].iloc[idx])
                idx += 1
                continue

            if len(cur_data) != expected_length:
                self.logger.debug(f"Dropping idx={idx}: Cur len {len(cur_data)} != Expected len {expected_length}\n")
                missing_numerators.append(d["numerator"].iloc[idx])
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
            "numerator": pd.Series(missing_numerators, dtype=np.float64),
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
    def process_rejoined_data(self, processing_request: ProcessingRequest, df: pd.DataFrame) -> pd.DataFrame:
        """A method that allows the implementer to process rejoined packets.

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame
            A DataFrame of packets, as obtained from rejoin_data

        Returns
        -------
        pd.DataFrame
            A DataFrame on which some transformations have been applied
        """
        raise NotImplementedError

    def merge_processed_dataframes(self, dfs: List[pd.DataFrame], idpu_types: List[int]) -> pd.DataFrame:
        """Merges processed DataFrames and eliminates duplicate packets

        Given a list of dataframes of identical format (decompressed/raw, level 0),
        merge them in a way such that duplicate frames are removed.

        Preference is given in the same order as which IDPU_TYPEs
        appear in the list self.idpu_types.

        Parameters
        ----------
        dfs : List[pd.DataFrame]
            DataFrames of packets, as obtained from process_rejoined_data
        idpu_types : List[int]
            Relevant IDPU types

        Returns
        -------
        pd.DataFrame
            A DataFrame representing the final level 0 data
        """
        self.logger.debug("Merging processed dataframes")
        df = pd.concat(dfs)
        df["idpu_type"] = df["idpu_type"].astype("category").cat.set_categories(idpu_types, ordered=True)

        # Keeping the first item means that the first/earlier idpu_type will be preserved
        # idpu_type is ordered in the same order as self.idpu_types
        return (
            df.dropna(subset=["data", "idpu_time"])
            .sort_values(["idpu_time", "idpu_type"])
            .drop_duplicates("idpu_time", keep="first")
            .reset_index()
        )

    def update_completeness_table(self, processing_request: ProcessingRequest, l0_df: pd.DataFrame) -> None:
        """Attempts to update the completeness table with the given data.

        If no completeness updater is provided, completeness will not be
        calculated or uploaded.

        Parameters
        ----------
        processing_request : ProcessingRequest
        l0_df : pd.DataFrame
            A DataFrame of level 0 data corresponding to the processing
            request, as obtained from generate_l0_df

        Returns
        -------
        None
        """
        self.logger.info(f"❓  Updating completeness table for {str(processing_request)}")
        l0_df_copy = l0_df.copy()
        l0_df_copy["times"] = l0_df_copy["idpu_time"]
        df = l0_df_copy[["times", "data", "idpu_type"]].drop_duplicates().dropna()[["times", "idpu_type"]]

        self.completeness_updater.update_completeness_table(processing_request, df, self.update_db)

    def generate_l0_file(self, processing_request: ProcessingRequest, l0_df: pd.DataFrame) -> Tuple[str, pd.DataFrame]:
        """From the given level 0 DataFrame, create a level 0 csv.

        TODO: Why are more transformations applied here?

        Parameters
        ----------
        processing_request : ProcessingRequest
        l0_df : pd.DataFrame
            A DataFrame of level 0 data corresponding to the given request

        Returns
        -------
        Tuple[str, pd.DataFrame]
            A tuple of the filename of the created csv, and a DataFrame of
            the transformed data used to populate the csv
        """
        self.logger.info(f"🟡  Generating Level 0 file for {str(processing_request)}")
        # Filter fields and duplicates
        l0_df = l0_df[["idpu_time", "data"]].drop_duplicates().dropna()

        # Select Data belonging only to a certain day
        l0_df = l0_df[
            (l0_df["idpu_time"] >= np.datetime64(processing_request.date))
            & (l0_df["idpu_time"] < np.datetime64(processing_request.date + ONE_DAY_DELTA))
        ]

        # TT2000 conversion
        l0_df["idpu_time"] = l0_df["idpu_time"].apply(pycdf.lib.datetime_to_tt2000)

        if l0_df.empty:
            raise EmptyException(f"Empty level 0 DataFrame: {str(processing_request)}")

        # Generate L0 file
        fname = self.make_filename(processing_request, 0, l0_df.shape[0])
        l0_df.to_csv(fname, index=False)

        return fname, l0_df

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
        return l1_file_name, l1_df

    def generate_l1_df(self, processing_request: ProcessingRequest, l0_df: pd.DataFrame) -> pd.DataFrame:
        """Generates a level 1 DataFrame related to the ProcessingRequest.

        Parameters
        ----------
        processing_request: ProcessingRequest
        l0_df : pd.DataFrame
            A DataFrame of finalized level 0 data corresponding to the
            ProcessingRequest. If it is not provided, the method will
            generate the level 0 DataFrame internally

        Returns
        -------
        pd.DataFrame
        """
        self.logger.info(f"🔵  Generating Level 1 DataFrame for {str(processing_request)}")
        if l0_df is None:
            self.logger.info("Still need a Level 0 DataFrame, generating now")
            l0_df = self.generate_l0_df(processing_request)

        # Allow derived class to transform data
        l1_df = self.transform_l0_df(processing_request, l0_df)

        # Timestamp conversion
        # TODO: Move this to transform_l0_df
        try:
            l1_df = l1_df[
                l1_df["idpu_time"].isna()
                | ((l1_df["idpu_time"] >= np.datetime64(processing_request.date))
                & (l1_df["idpu_time"] < np.datetime64(processing_request.date + ONE_DAY_DELTA)))
            ]
            l1_df["idpu_time"] = l1_df["idpu_time"].apply(dt_to_tt2000)
            if l1_df.empty:
                raise EmptyException(f"Final Dataframe is empty: {str(processing_request)}")

        except KeyError:
            self.logger.debug("The column 'idpu_time' does not exist, but it's probably OK")

        return l1_df

    @abstractmethod
    def transform_l0_df(self, processing_request: ProcessingRequest, l0_df: pd.DataFrame) -> pd.DataFrame:
        """A method for further transformations to the l0 DataFrame.

        Should be overriden in derived classes.

        Parameters
        ----------
        processing_request : ProcessingRequest
        l0_df : pd.DataFrame

        Returns
        -------
        pd.DataFrame
        """
        raise NotImplementedError

    def generate_l1_file(self, processing_request: ProcessingRequest, l1_df: pd.DataFrame) -> Tuple[str, pd.DataFrame]:
        """Generates a level 1 file related to the ProcessingRequest.

        Parameters
        ----------
        processing_request: ProcessingRequest
        l1_df : pd.DataFrame
            A DataFrame of finalized level 1 data corresponding to the
            ProcessingRequest

        Returns
        -------
        Tuple[str, pd.DataFrame]
            The name of the generated CDF, and the provided DataFrame
        """
        self.logger.info(f"🟣  Generating Level 1 DataFrame for {str(processing_request)}")
        fname = self.make_filename(processing_request, 1)
        cdf = self.create_empty_cdf(fname)
        self.fill_cdf(processing_request, l1_df, cdf)
        cdf.close()

        return fname, l1_df
