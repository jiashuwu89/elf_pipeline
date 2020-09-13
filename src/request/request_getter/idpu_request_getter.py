import datetime as dt

from request.downlink_manager import DownlinkManager
from types.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter
from util.constants import IDPU_TYPES, SCIENCE_TYPES


class IdpuRequestGetter(RequestGetter):
    def __init__(self, pipeline_config):
        super().__init__(pipeline_config)

        self.downlink_manager = DownlinkManager(pipeline_config)

    def get(self, pipeline_query):
        """FGM and EPD Processing Requests

        We want to get downlinks that fit the criteria, then find the
        corresponding collection times.

        Logic:
        - If querying by downlink time, we MUST calculate new downlinks, can't
        use science downlinks table bc only stores collection time
        - If querying by collection time, we CANNOT calculate new downlinks
        because packets in the science packets table do not contain accessible
        collection time data necessarily (compressed packets). We MUST refer
        refer to science downlinks table
        """
        idpu_products = set()
        for product in pipeline_query.data_products:
            if product in IDPU_TYPES:
                idpu_products.update(SCIENCE_TYPES[product])
        if not idpu_products:
            return set()

        if pipeline_query.times == "downlink":
            dl_list = self.get_filtered_downlinks(pipeline_query)
        elif pipeline_query.times == "collection":  # TODO: Fix this
            dl_list = self.downlink_manager.get_downlinks_by_collection_time(pipeline_query)
        else:
            raise ValueError(f"Expected 'downlink' or 'collection', got {pipeline_query.times}")

        self.logger.info(f"Obtained Downlinks:\n{self.downlink_manager.print_downlinks(dl_list)}")
        return self.get_requests_from_downlinks(dl_list)

    def get_filtered_downlinks(self, pipeline_query):
        """Calculate Downlinks (by downlink time) with desired mission ids and idpu types"""

        def valid_downlink(downlink):
            return (
                downlink.mission_id in pipeline_query.mission_ids and downlink.idpu_type in pipeline_query.data_products
            )

        calculated_dls = self.downlink_manager.get_downlinks_by_downlink_time(pipeline_query)
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
