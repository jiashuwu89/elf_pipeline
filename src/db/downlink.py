import logging

import pandas as pd

from common import models

# TODO: packet_id vs id Check

# TODO: Unify Downlink class and Downlink dataframe?


class PacketInfo:
    def __init__(self, packet_id, id, idpu_time, collection_time):
        self.packet_id = packet_id
        self.id = id  # ID in science_packets table
        self.idpu_time = idpu_time
        self.collection_time = collection_time

    def to_string(self):
        return f"PacketInfo(packet_id={self.packet_id}, id={self.id}, \
                idpu_time={self.idpu_time}, collection_time={self.collection_time})"


class Downlink:
    def __init__(self, mission_id, idpu_type, denominator, first_packet_info, last_packet_info):
        self.mission_id = mission_id
        self.idpu_type = idpu_type
        self.denominator = denominator

        self.first_packet_info = first_packet_info
        self.last_packet_info = last_packet_info

    def to_string(self):
        return f"Downlink:\n\
                \tmission_id={self.mission_id}, idpu_type={self.idpu_type}, denominator={self.denominator}\n\
                \tFirst: {self.first_packet_info.to_string()}\n\
                \tSecond: {self.second_packet_info.to_string()}"


class DownlinkManager:
    def __init__(self, session):
        self.session = session
        self.logger = logging.getLogger("DownlinkManager")

    def print_downlinks(self, downlinks):
        msg = "Downlinks:\n" + "\n".join([d.to_string() for d in downlinks])
        self.logger.info(msg)

    def update_science_downlink_table(self, ____):
        new_downlinks = self.calculate_new_downlinks(___)
        self.upload_downlink_entries(new_downlinks)

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

    def get_downlinks(self, ____):
        pass

    def get_formatted_df_from_downlink(self, downlink) -> pd.DataFrame:
        """
        This function takes in a downlink, and returns a DataFrame corresponding to the processed data.
        It gets the data, fixes the data, then formats it

        TODO: Move this to Downlink?
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

        """
        Given a dataframe of data from fetch_downlink_data function, checks if data needs to 
        repaired, and applies the appropriate fixes. Currently, this handles issues relating to
        having multiple commanders open, which causes data to be duplicated in the database (eg.
        there are two packets that are identical except for packet_id)
        """
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

    # NEW STUFF for Akhil Tests
    # TODO: Fix this

    def fetch_downlinks_from_range(self, mission_id, packet_type, first_time, last_time):
        pass

    def fetch_downlinks_from_downlink_range(self, downlink_range):
        downlinks = []

        query = self.session.query(models.ScienceDownlink).filter(
            models.ScienceDownlink.mission_id == downlink_range.mission_id,
            models.ScienceDownlink.first_collection_time <= downlink_range.last_collection_time,
            models.ScienceDownlink.last_collection_time >= downlink_range.first_collection_time,
        )

        for row in query:
            # TODO: Bad Nones
            first_packet_info = PacketInfo(None, row.first_packet, row.first_time, row.first_collection_time)
            last_packet_info = PacketInfo(None, row.last_packet, row.last_time, row.last_collection_time)
            downlink = Downlink(row.mission_id, row.idpu_type, row.denominator, first_packet_info, last_packet_info)
            downlinks.append(downlink)

        return downlinks
