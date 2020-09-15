"""Contains class to manage/handle downlinks"""
import datetime as dt
import logging

import pandas as pd
from elfin.common import models

from data_type.downlink import Downlink
from data_type.packet_info import PacketInfo
from util import byte_tools, science_utils
from util.constants import COMPRESSED_TYPES

# TODO: packet_id vs id Check

# TODO: Unify Downlink class and Downlink dataframe?

# TODO: Group of DLs should be a set, not a list?

# TODO: Where can this be relocated to?

# TODO: Separate into Downlink Creator and Downlink getter


class DownlinkManager:
    """A class that provides utilities involving downlinks"""

    def __init__(self, pipeline_config):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.session = pipeline_config.session
        self.update_db = pipeline_config.update_db

    def print_downlinks(self, downlinks, prefix="Downlinks:"):
        """Prints the collection of downlinks given, in a formatted fashion"""
        if downlinks:
            msg = f"{prefix}\nGot {len(downlinks)} Total Downlink{science_utils.s_if_plural(downlinks)}\n" + "\n".join(
                [str(d) for d in downlinks]
            )
        else:
            msg = f"{prefix} No downlinks!"
        self.logger.info(msg)

    def get_downlinks_by_collection_time(self, query):
        """
        Fetch all science downlinks whose collection data overlaps with range of dates

        Returns:
            A list of downlinks, of the format
            (mission_id, packet_type, first_time, last_time, denom,
            first_id, last_id, first collection time, last collection time)
        """
        sql_query = self.session.query(models.ScienceDownlink).filter(
            models.ScienceDownlink.idpu_type.in_(query.data_products),
            models.ScienceDownlink.mission_id.in_(query.mission_ids),
            models.ScienceDownlink.first_collection_time <= query.end_time,
            models.ScienceDownlink.last_collection_time >= query.start_time,
        )

        downlinks = []
        for dl in sql_query:
            first_packet_info = PacketInfo(None, dl.first_packet, dl.first_time, dl.first_collection_time)
            last_packet_info = PacketInfo(None, dl.last_packet, dl.last_time, dl.last_collection_time)
            downlinks.append(Downlink(dl.mission_id, dl.idpu_type, dl.denominator, first_packet_info, last_packet_info))

        return downlinks

    def get_downlinks_by_downlink_time(self, pipeline_query):
        """
        Calculate new science downlinks by scanning through the
        list of science packets received within a range of dates

        - start_time and end_time are downlink times (called 'timestamp' in table)
        - All Mission IDs, any idpu types

        Returns:
            A list of downlinks
        """
        downlinks = []
        for mission_id in pipeline_query.mission_ids:
            self.logger.info(f"Getting Downlink for mission {mission_id}")
            cur_mission_downlinks = self.calculate_new_downlinks_by_mission_id(
                mission_id, pipeline_query.start_time, pipeline_query.end_time
            )
            self.logger.info(
                f"Got {len(cur_mission_downlinks)} "
                + f"Downlink{science_utils.s_if_plural(cur_mission_downlinks)} for mission {mission_id}"
            )
            downlinks += cur_mission_downlinks

        # TODO: can't compare idpu_type directly against product name!
        return [
            dl
            for dl in downlinks
            if dl.idpu_type in pipeline_query.data_products_to_idpu_types(pipeline_query.data_products)
        ]

    # HELPER FOR get_downlinks_by_downlink_time, should be private
    def calculate_new_downlinks_by_mission_id(self, mission_id, start_time, end_time):
        """All downlinks for a specific mission, between two times"""
        downlinks = []

        query = self.session.query(models.SciencePacket).filter(
            models.SciencePacket.timestamp >= start_time,
            models.SciencePacket.timestamp <= end_time,
            models.SciencePacket.mission_id == mission_id,
        )

        # Information about Packets to be Tracked
        cur_num = 0
        cur_denom = 0
        cur_packet_type = None
        first_collection_time = None
        last_collection_time = None
        first_idpu_time = None
        last_idpu_time = None
        first_id = None
        last_id = None

        prev_idpu_time = None

        for science_packet in query:
            # If we get a packet without an idpu_type, can't be sure if it
            # belongs to this downlink or the next. Code will include it if
            # the next packet is part of the initial downlink, or just ignore
            # it completely if the next packet begins a new Downlink
            if science_packet.idpu_type == -1:
                continue

            # Criteria for New Downlink:
            # - This is the very first packet:
            # - Packet type shifts
            # - Numerator shifts
            # - Denominator shifts
            if (
                first_id is None
                or science_packet.idpu_type != cur_packet_type
                or science_packet.numerator < cur_num
                or science_packet.denominator != cur_denom
            ):

                # flush the existing downlink
                if cur_packet_type is not None and first_idpu_time is not None and last_idpu_time is not None:
                    first_packet_info = PacketInfo(None, first_id, first_idpu_time, first_collection_time)
                    last_packet_info = PacketInfo(None, last_id, last_idpu_time, last_collection_time)
                    downlinks.append(
                        Downlink(mission_id, cur_packet_type, cur_denom, first_packet_info, last_packet_info)
                    )

                cur_packet_type = None
                first_idpu_time = None
                last_idpu_time = None
                first_collection_time = None
                last_collection_time = None
                first_id = science_packet.id
                last_id = None

            # Update Current packet num/denom, and packet type (if not set)
            cur_num = science_packet.numerator
            cur_denom = science_packet.denominator
            if cur_packet_type is None:
                cur_packet_type = science_packet.idpu_type

            # update first/last timestamps, if we have a timestamp to use
            if science_packet.idpu_time:

                # Compressed packet, and idpu_time changed: collection_time comes from the data
                if science_packet.idpu_type in COMPRESSED_TYPES and (
                    (not prev_idpu_time) or prev_idpu_time != science_packet.idpu_time
                ):
                    try:
                        collection_time = byte_tools.raw_idpu_bytes_to_datetime(bytes.fromhex(science_packet.data[:16]))
                    except ValueError as e:
                        # Start a New Downlink if this seems to be a bad packet
                        self.logger.warning(
                            f"⚠️ New Downlink, skipping current packet (ID: {science_packet.id}) due to unreadable \
                                datetime {science_packet.data[:16]}: {e}"
                        )
                        first_id = None
                        continue

                # Not compressed packet: collection_time comes from filesystem
                elif science_packet.idpu_type not in COMPRESSED_TYPES:
                    collection_time = science_packet.idpu_time

                idpu_time = science_packet.idpu_time
                prev_idpu_time = idpu_time

                # Set first_idpu_time if not yet set
                if not first_idpu_time:
                    first_idpu_time = idpu_time
                    first_collection_time = collection_time

                # Always update last_idpu_time
                last_idpu_time = idpu_time
                last_collection_time = collection_time

            # always update last ID
            last_id = science_packet.id

        # flush the final downlink
        if cur_packet_type is not None and first_idpu_time is not None and last_idpu_time is not None:
            first_packet_info = PacketInfo(None, first_id, first_idpu_time, first_collection_time)
            last_packet_info = PacketInfo(None, last_id, last_idpu_time, last_collection_time)
            downlinks.append(Downlink(mission_id, cur_packet_type, cur_denom, first_packet_info, last_packet_info))

        downlinks.sort(key=lambda x: x.idpu_type)

        if self.update_db:
            self.logger.info(
                f"Updating science_downlink table with {len(downlinks)}"
                + f"calculated Downlink{science_utils.s_if_plural(downlinks)}"
            )
            self.upload_downlink_entries(downlinks)

        return downlinks

    # HELPER FOR get_downlinks_by_downlink_time, should be private
    def upload_downlink_entries(self, downlinks):
        """
        Uploads a list of downlink entries to the database as
        ScienceDownlink objects. Duplicate entries are ignored.
        """
        for d in downlinks:
            q = self.session.query(models.ScienceDownlink.id).filter(
                models.ScienceDownlink.mission_id == d.mission_id,
                models.ScienceDownlink.idpu_type == d.idpu_type,
                models.ScienceDownlink.first_packet == d.first_packet_info.id,
                models.ScienceDownlink.last_packet == d.last_packet_info.id,
                models.ScienceDownlink.first_time == d.first_packet_info.idpu_time,
                models.ScienceDownlink.last_time == d.last_packet_info.idpu_time,
                models.ScienceDownlink.first_collection_time == d.first_packet_info.collection_time,
                models.ScienceDownlink.last_collection_time == d.last_packet_info.collection_time,
            )
            if self.session.query(q.exists()).scalar():
                continue

            # TODO: consider updating old entries to merge, if applicable

            entry = models.ScienceDownlink(
                mission_id=d.mission_id,
                idpu_type=d.idpu_type,
                denominator=d.denominator,
                first_packet=d.first_packet_info.id,
                last_packet=d.last_packet_info.id,
                first_time=d.first_packet_info.idpu_time,
                last_time=d.last_packet_info.idpu_time,
                first_collection_time=d.first_packet_info.collection_time,
                last_collection_time=d.last_packet_info.collection_time,
            )

            self.session.add(entry)

        self.session.flush()
        self.session.commit()

    def get_relevant_downlinks(self, processing_request):
        """Get Downlinks that are relevant to the processing request"""
        query = self.session.query(models.ScienceDownlink).filter(
            models.ScienceDownlink.mission_id == processing_request.mission_id,
            models.ScienceDownlink.idpu_type == processing_request.data_product,
            models.ScienceDownlink.first_collection_time < processing_request.date + dt.timedelta(days=1),
            models.ScienceDownlink.last_collection_time >= processing_request.date,
        )

        downlinks = []

        for row in query:  # TODO: Check first_packet/last_packet
            # TODO: Bad Nones
            first_packet_info = PacketInfo(None, row.first_packet, row.first_time, row.first_collection_time)
            last_packet_info = PacketInfo(None, row.last_packet, row.last_time, row.last_collection_time)
            downlink = Downlink(row.mission_id, row.idpu_type, row.denominator, first_packet_info, last_packet_info)
            downlinks.append(downlink)

        if not downlinks:
            raise RuntimeError(f"No Downlinks found for processing request {processing_request}")

        return downlinks

    def get_df_from_downlink(self, downlink) -> pd.DataFrame:
        """
        This function takes in a downlink, and returns a DataFrame corresponding to the processed data.
        It gets the data, fixes the data, then formats it
        """

        # Fetching Downlink Data
        q = (
            self.session.query(
                models.SciencePacket, models.Packet.data.label("packet_data"), models.Packet.source.label("source")
            )
            .filter(
                models.SciencePacket.mission_id == downlink.mission_id,
                # May need to delete this in order to get packets with no idpu_type
                models.SciencePacket.idpu_type == downlink.idpu_type,
                models.SciencePacket.id >= downlink.first_packet_info.id,
                models.SciencePacket.id <= downlink.last_packet_info.id,
            )
            .join(models.Packet)
        )
        df = pd.read_sql_query(q.statement, q.session.bind)

        # Given a dataframe of data from fetch_downlink_data function, checks if data needs to
        # repaired, and applies the appropriate fixes. Currently, this handles issues relating to
        # having multiple commanders open, which causes data to be duplicated in the database (eg.
        # there are two packets that are identical except for packet_id)
        df = df.drop_duplicates(subset=["data", "idpu_type", "numerator", "denominator"])

        # Formatting DataFrame
        df_columns = [
            "id",
            "mission_id",
            "idpu_type",
            "idpu_time",
            "data",
            "numerator",
            "denominator",
            "packet_id",
            "packet_data",
            "timestamp",
            "source",
        ]
        reduced_df = pd.DataFrame(df, columns=df_columns)

        existing_numerators = reduced_df["numerator"]
        max_numerator = existing_numerators.max()
        denominator = reduced_df.loc[0]["denominator"]
        if max_numerator > denominator:
            self.logger.debug(f"⚠️ Maximum numerator {max_numerator} exceeds denominator {denominator}")
        all_numerators = pd.Series(range(max(max_numerator, denominator) + 1))
        missing_numerators = all_numerators[~all_numerators.isin(existing_numerators)]

        missing_frames = {
            "mission_id": downlink.mission_id,
            "idpu_type": downlink.idpu_type,
            "numerator": missing_numerators,
            "denominator": denominator,
            "id": None,
            "packet_id": None,
            "timestamp": None,
            "idpu_time": None,
            "data": None,
            "packet_data": None,
            "source": None,
        }
        missing_df = pd.DataFrame(data=missing_frames)

        formatted_df = reduced_df.append(missing_df, sort=False)
        formatted_df = reduced_df.sort_values("numerator").reset_index(drop=True)

        return formatted_df
