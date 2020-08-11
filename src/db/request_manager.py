import datetime as dt
import logging

import sqlalchemy
from sqlalchemy.sql import func

from common import models
from db.downlink import DownlinkManager
from db.processing_request import ProcessingRequest


class RequestManager:
    def __init__(self, session):
        self.session = session
        self.downlink_manager = DownlinkManager(session)
        self.logger = logging.getLogger("RequestManager")

    def get_processing_requests(self, mission_ids, data_products, times, start_time, end_time, calculate, update_db):
        # TODO: "times" is just for idpu products?
        processing_requests = set()

        selected_idpu_products = []
        selected_mrm_products = []

        if selected_idpu_products:
            processing_requests.update(
                self.get_general_processing_requests(
                    mission_ids, selected_idpu_products, times, start_time, end_time, calculate, update_db
                )
            )
        if selected_mrm_products:
            processing_requests.update(self.get_mrm_processing_requests(selected_mrm_products, start_time, end_time))
        if "eng" in data_products:
            processing_requests.update(self.get_eng_processing_requests(start_time, end_time))
        if "state" in data_products:
            processing_requests.update(self.get_state_processing_requests(start_time, end_time))

        return processing_requests

    def get_general_processing_requests(
        self, mission_ids, data_products, times, start_time, end_time, calculate, update_db
    ):
        """FGM and EPD Processing Requests

        Logic:
        - If we want requests by downlink time, we ALWAYS need to calculate new downlinks
            - Science Downlinks Table does not store downlink times, only IDPU times and collection times, so it cannot be used
            - Once we have our Downlinks, refer to update_db to determine if we should put the downlinks onto the Science Downlinks Table
        - If we want requests by collection time, we must use the Science Downlinks Table
            - We can't calculate new downlinks for the specified times, as we can't query the Science Packets Table by collection time
                - By extension, no point in updating DB without having any new downlinks
            - However, if calculate option is specified, we can calculate new downlinks for the past 5 hours
                - Update DB based on update_db
                - TODO: Could try to calculate new downlinks within the last 5 hours
        """
        if times == "downlink_time":
            # Always need to calculate
            calculated_dls = self.downlink_manager.calculate_new_downlinks(start_time, end_time, upload_db)
            dl_list = list(
                filter(lambda dl: dl.mission_id in mission_ids and dl.idpu_type in data_products, calculated_dls)
            )
        elif times == "collection_time":
            if calculate:
                cur_time = dt.datetime(*dt.datetime.utcnow().timetuple()[:4])
                cur_time_minus_delta = self.end_time - dt.timedelta(hours=5)
                calculated_dls = self.downlink_manager.calculate_new_downlinks(
                    cur_time_minus_delta, cur_time, upload_db
                )
            dl_list = self.downlink_manager.get_downlinks_by_collection_time(
                mission_ids, data_products, times, start_time, end_time
            )
        else:
            raise ValueError(f"Bad times value: {times}")

        self.logger.info(f"Obtained Downlinks:\n{self.downlink_manager.print_downlinks(dl_list)}")
        return self.get_requests_from_downlinks(dl_list)

    def get_requests_from_downlinks(self, dl_list):
        """ Helper Function for geet_general_processing_requests """
        general_processing_requests = set()
        for dl in dl_list:
            start_date = dl.first_collection_time.date()
            end_date = dl.last_collection_time.date()
            delta = dt.timedelta(days=1)
            while start_date <= end_date:
                pr = ProcessingRequest(dl.mission_id, dl.data_product, start_date)
                general_processing_requests.add(pr)
                start_date += delta
        return list(general_processing_requests)

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
