from typing import List, Set, Type

from data_type.Downlink import Downlink
from data_type.pipeline_config import PipelineConfig
from data_type.pipeline_query import PipelineQuery
from data_type.processing_request import ProcessingRequest
from data_type.time_type import TimeType
from request.downlink_manager import DownlinkManager
from request.request_getter.request_getter import RequestGetter
from util.constants import ONE_DAY_DELTA, PACKET_MAP, SCIENCE_TYPES


class IdpuRequestGetter(RequestGetter):
    """A RequestGetter that uses a DownlinkManager to get ProcessingRequests.

    ProcessingRequests are created for data that corresponds to the
    PipelineQuery provided as a parameter to the `get` method. We find this
    data by utilizing the `science_downlink` table (which the pipeline
    maintains via DownlinkManagers). Downlinks (best effort at grouping
    packets together) that have times, idpu types, and mission ids matching
    the PipelineQuery will require the creation of a ProcessingRequest.

    Parameters
    ----------
    pipeline_config : Type[PipelineConfig]
    downlink_manager : DownlinkManager
    """

    def __init__(self, pipeline_config: Type[PipelineConfig], downlink_manager: DownlinkManager):
        super().__init__(pipeline_config)

        self.downlink_manager = downlink_manager

    def get(self, pipeline_query: Type[PipelineQuery]) -> Set[ProcessingRequest]:
        """Gets FGM, EPD, and ENG Processing Requests via a DownlinkManager

        Using the DownlinkManager, we want to get downlinks that fit the
        criteria of the pipeline_query, then create processing requests with
        the corresponding information about collection times, data type, and
        mission id.

        The logic determining if new downlinks must be calculated is as
        follows: If querying by downlink time, we MUST calculate new
        downlinks, we can't use science downlinks table because it only stores
        collection time. If querying by collection time, we CANNOT calculate
        new downlinks because packets in the science packets table do not
        contain accessible collection time data necessarily (compressed
        packets). Therefore, we MUST refer to science downlinks table.

        Parameters
        ----------
        pipeline_query : Type[PipelineQuery]

        Returns
        -------
        Set[ProcessingRequest]
            A set of EPD, ENG, and FGM processing requests relevant to the
            pipeline query, found using the DownlinkManager
        """
        # TODO: Refactor so that downlink manager is outside of the request getter
        self.logger.info("🏀  Getting IDPU Requests")
        idpu_products = self.get_relevant_products(pipeline_query.data_products, SCIENCE_TYPES)
        if not idpu_products:
            return set()
        self.logger.info(f"Requested relevant products: {idpu_products}")

        if pipeline_query.times == TimeType.DOWNLINK:
            dl_list = self.downlink_manager.get_downlinks_by_downlink_time(pipeline_query)
        elif pipeline_query.times == TimeType.COLLECTION:
            dl_list = self.downlink_manager.get_downlinks_by_collection_time(pipeline_query)
        else:
            raise ValueError(f"Expected 'downlink' or 'collection', got {pipeline_query.times}")

        self.downlink_manager.print_downlinks(
            dl_list, prefix="➜\tFound downlinks that are relevant to the PipelineQuery"
        )
        general_processing_requests = {
            pr for pr in self.get_requests_from_downlinks(dl_list) if pr.data_product in pipeline_query.data_products
        }  # TODO: This is a hacky fix. Check why mapping 1 -> fgs and fgf, etc
        self.logger.info(f"🏀  Got {len(general_processing_requests)} requests from downlinks")
        return general_processing_requests

    def get_requests_from_downlinks(self, dl_list: List[Downlink]) -> Set[ProcessingRequest]:
        """Helper Function for get_general_processing_requests.

        Given a list of downlinks, get processing requests with the
        appropriate collection times

        Parameters
        ----------
        dl_list : List[Downlink]
            A list of Downlinks

        Returns
        -------
        Set[ProcessingRequest]
            A set of ProcessingRequests that cover all of the downlinks, such
            that all days on which the Downlinks occurred will have the
            files generated for the corresponding mission and data type
        """
        self.logger.debug(f"dl_list={dl_list}")
        delta = ONE_DAY_DELTA

        general_processing_requests = set()
        for dl in dl_list:
            for data_product in PACKET_MAP[dl.idpu_type]:
                start_date = dl.first_packet_info.collection_time.date()
                end_date = dl.last_packet_info.collection_time.date()
                while start_date <= end_date:
                    pr = ProcessingRequest(dl.mission_id, data_product, start_date)
                    general_processing_requests.add(pr)
                    start_date += delta

        return general_processing_requests
