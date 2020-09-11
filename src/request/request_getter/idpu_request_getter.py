import datetime as dt

from request.downlink.downlink_manager import DownlinkManager
from request.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter
from util.constants import LOOK_BEHIND_DELTA


class IdpuRequestGetter(RequestGetter):
    def __init__(self, session, mission_ids, data_products, times, calculate, update_db):
        super().__init__(session)

        self.mission_ids = mission_ids
        self.data_products = data_products
        self.times = times
        self.calculate = calculate

        self.downlink_manager = DownlinkManager(session, update_db)

    def get(self, start_time, end_time):
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
        if self.times == "downlink_time":
            dl_list = self.get_filtered_downlinks(self.mission_ids, self.data_products, start_time, end_time)
        elif self.times == "collection_time":  # TODO: Fix this
            if self.calculate:
                cur_time = dt.datetime(*dt.datetime.utcnow().timetuple()[:4])
                cur_time_minus_delta = self.end_time - LOOK_BEHIND_DELTA
                dl_list = self.get_filtered_downlinks(cur_time_minus_delta, cur_time)
            else:
                dl_list = self.downlink_manager.get_downlinks_by_collection_time(
                    self.mission_ids, self.data_products, self.times, start_time, end_time
                )

        self.logger.info(f"Obtained Downlinks:\n{self.downlink_manager.print_downlinks(dl_list)}")
        return self.get_requests_from_downlinks(dl_list)

    def get_filtered_downlinks(self, start_time, end_time):
        """Calculate Downlinks (by downlink time) with desired mission ids and idpu types"""

        def valid_downlink(downlink):
            return downlink.mission_id in self.mission_ids and downlink.idpu_type in self.data_products

        calculated_dls = self.downlink_manager.calculate_new_downlinks(self.mission_ids, start_time, end_time)
        return [downlink for downlink in calculated_dls if valid_downlink(downlink)]

    def get_requests_from_downlinks(self, dl_list):
        """Helper Function for get_general_processing_requests

        Given a list of downlinks, get processing requests with the
        appropriate collection times

        Returns a set
        """
        delta = dt.timedelta(days=1)

        general_processing_requests = set()
        for dl in dl_list:
            start_date = dl.first_collection_time.date()
            end_date = dl.last_collection_time.date()
            while start_date <= end_date:
                pr = ProcessingRequest(dl.mission_id, dl.data_product, start_date)
                general_processing_requests.add(pr)
                start_date += delta

        self.logger.debug(f"Got {len(general_processing_requests)} requests from downlinks")
        return general_processing_requests
