import datetime as dt
import logging

import sqlalchemy
from sqlalchemy.sql import func

from common import models
from request.downlink.downlink_manager import DownlinkManager
from request.processing_request import ProcessingRequest
from util.constants import LOOK_BEHIND_DELTA


class RequestManager:
    """An object to determine mission/time/product combinations to process"""

    def __init__(self, session, calculate, update_db):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.session = session
        self.calculate = calculate
        self.update_db = update_db  # TODO: Synchronize update_db and upload_to_db in Coordinator

        self.downlink_manager = DownlinkManager(session, update_db)

    def get_processing_requests(self, mission_ids, data_products, times, start_time, end_time):
        # TODO: "times" is just for idpu products?
        processing_requests = set()

        selected_idpu_products = []
        selected_mrm_products = []

        if selected_idpu_products:
            general_processing_requests = self.get_general_processing_requests(
                mission_ids, selected_idpu_products, times, start_time, end_time
            )
            processing_requests.update(general_processing_requests)
        if selected_mrm_products:
            processing_requests.update(self.get_mrm_processing_requests(selected_mrm_products, start_time, end_time))
        if "eng" in data_products:
            processing_requests.update(self.get_eng_processing_requests(start_time, end_time))
        if "state" in data_products:
            processing_requests.update(self.get_state_processing_requests(start_time, end_time))

        return processing_requests

    def get_general_processing_requests(self, mission_ids, data_products, times, start_time, end_time):
        """FGM and EPD Processing Requests

        We want to get downlinks that fit the criteria, then find the
        corresponding collection times.

        Logic:
        - If we want requests by downlink time, we ALWAYS need to calculate
        new downlinks
            - Science Downlinks Table does not store downlink times, only IDPU
            times and collection times, so it cannot be used
            - Once we have our Downlinks, refer to update_db to determine if
            we should put the downlinks onto the Science Downlinks Table
        - If we want requests by collection time, we must use the Science
        Downlinks Table
            - If calculate option is specified, we can calculate new
            downlinks for the past 5 hours
                - TODO: Could try to calculate new downlinks within the last 5
                hours
            - If not specified, we can't calculate new downlinks for the
            specified times, as we can't query the Science Packets Table by
            collection time
                - If we get here, just use the get_downlinks_by_collection_time
                to query the downlinks table
        """
        if times == "downlink_time":
            dl_list = self.get_filtered_downlinks(mission_ids, data_products, start_time, end_time)
        elif times == "collection_time":  # TODO: Fix this
            if self.calculate:
                cur_time = dt.datetime(*dt.datetime.utcnow().timetuple()[:4])
                cur_time_minus_delta = self.end_time - LOOK_BEHIND_DELTA
                dl_list = self.get_filtered_downlinks(mission_ids, data_products, cur_time_minus_delta, cur_time)
            else:
                dl_list = self.downlink_manager.get_downlinks_by_collection_time(
                    mission_ids, data_products, times, start_time, end_time
                )

        self.logger.info(f"Obtained Downlinks:\n{self.downlink_manager.print_downlinks(dl_list)}")
        return self.get_requests_from_downlinks(dl_list)

    def get_filtered_downlinks(self, mission_ids, data_products, start_time, end_time):
        """Calculate Downlinks (by downlink time) with desired mission ids and idpu types"""

        def valid_downlink(downlink):
            return downlink.mission_id in mission_ids and downlink.idpu_type in data_products

        calculated_dls = self.downlink_manager.calculate_new_downlinks(start_time, end_time)
        return [downlink for downlink in calculated_dls if valid_downlink(downlink)]

    def get_requests_from_downlinks(self, dl_list):
        """Helper Function for get_general_processing_requests

        Given a list of downlinks, get processing requests with the
        appropriate collection times

        Returns a set
        """
        general_processing_requests = set()
        for dl in dl_list:
            start_date = dl.first_collection_time.date()
            end_date = dl.last_collection_time.date()
            delta = dt.timedelta(days=1)
            while start_date <= end_date:
                pr = ProcessingRequest(dl.mission_id, dl.data_product, start_date)
                general_processing_requests.add(pr)
                start_date += delta
        return general_processing_requests

    def get_mrm_processing_requests(self, mission_ids, mrm_types, start_time, end_time):
        return {
            ProcessingRequest(res.mission_id, res.mrm_type, dt.date(res.date))
            for res in self.session.query(sqlalchemy.distinct(func.date(models.MRM.timestamp)))
            .filter(
                models.Packet.mission_id.in_(mission_ids),
                models.Packet.timestamp >= start_time,
                models.Packet.timestamp <= end_time,
                models.MRM.mrm_type.in_(mrm_types),
            )
            .join(models.Packet)
            if res.date is not None
        }

    def get_eng_processing_requests(self):
        pass

    def get_state_processing_requests(self):
        pass
