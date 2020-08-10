import datetime as dt

import sqlalchemy
from sqlalchemy.sql import func

from common import models
from db.downlink import DownlinkManager
from processor.processing_request import ProcessingRequest


class RequestManager:
    def __init__(self, session):
        self.session = session
        self.downlink_manager = DownlinkManager(session)

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
        """ FGM and EPD Processing Requests
        start_time and end_time are downlink times

        calculate [True|False] should only be True if daily
        """

        if calculate:
            dl_list = self.downlink_manager.calculate_new_downlinks(start_time, end_time)
            if update_db:
                self.downlink_manager.upload_downlink_entries(dl_list)
            dl_list = list(filter(lambda dl: dl.mission_id in mission_ids and dl.idpu_type in data_products, dl_list))
        else:
            dl_list = self.downlink_manager.get_downlinks(mission_ids, data_products, times, start_time, end_time)

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
