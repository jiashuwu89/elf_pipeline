import datetime as dt
import sqlalchemy

from processor.processing_request import ProcessingRequest
from util.downlink import DownlinkManager
from common import models


class RequestManager:

    def __init__(self, session):
        self.session = session
        self.downlink_manager = DownlinkManager(session)

    def get_processing_requests(self, mission_ids, times, start_time, end_time, calculate):
        processing_requests = set()

        processing_requests.update(self.get_general_processing_requests())
        processing_requests.update(self.get_mrm_processing_requests())
        processing_requests.update(self.get_eng_processing_requests())
        processing_requests.update(self.get_state_processing_requests())

        return processing_requests

    def get_general_processing_requests(self, mission_ids, times, start_time, end_time, calculate):
        """ FGM and EPD Processing Requests """

        if calculate == "yes":
            self.downlink_manager.update_science_downlink_table(mission_ids, start_time, end_time)
        dl_list = self.downlink_manager.get_downlinks(mission_ids, times, start_time, end_time)

        return self.get_requests_from_downlinks(dl_list)

    def get_requests_from_downlinks(self, dl_list):
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
                .filter(models.Packet.mission_id.in_(mission_ids),
                        models.Packet.timestamp >= start_time,
                        models.Packet.timestamp <= end_time,
                        models.MRM.mrm_type.in_(mrm_types))
                .join(models.Packet)
            if res.date is not None
        }

    def get_eng_processing_requests(self):
        pass

    def get_state_processing_requests(self):
        pass
