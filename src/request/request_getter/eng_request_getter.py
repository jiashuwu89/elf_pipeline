from typing import Set, Type

from elfin.common import models
from sqlalchemy.sql import func

from data_type.pipeline_query import PipelineQuery
from data_type.processing_request import ProcessingRequest
from data_type.time_type import TimeType
from request.request_getter.request_getter import RequestGetter
from util import science_utils


# TODO: For all request getters, make sure that the data product is requested BEFORE getting requests
class EngRequestGetter(RequestGetter):
    def get(self, pipeline_query: Type[PipelineQuery]) -> Set[ProcessingRequest]:
        """Gets ENG ProcessingRequests based on categorical and bmon data.

        NOTE: The IdpuRequestGetter will obtain ENG requests for days
        on which relevant IDPU data was collected (IDPU types 14, 15, 16).
        This RequestGetter will get ENG requests for days on which relevant
        bmon or categoricals data was collected.

        Parameters
        ----------
        pipeline_query : Type[PipelineQuery]

        Returns
        -------
        Set[ProcessingRequest]
            A set of ENG processing requests relevant to the pipeline query
        """
        self.logger.info("⚽️  Getting ENG Requests")

        eng_processing_requests: Set[ProcessingRequest] = set()

        if "eng" not in pipeline_query.data_products:
            self.logger.info("⚽️  Got 0 ENG Requests")
            return eng_processing_requests
        self.logger.info("Requested relevant products: 'eng'")

        eng_processing_requests.update(self.get_categoricals_requests(pipeline_query))
        eng_processing_requests.update(self.get_bmon_requests(pipeline_query))

        eng_processing_requests = {pr for pr in eng_processing_requests if pr.date.year != 2000}  # TODO: Explain this

        self.logger.info(
            f"⚽️  Got {len(eng_processing_requests)} "
            + f"ENG processing request{science_utils.s_if_plural(eng_processing_requests)}"
        )
        return eng_processing_requests

    def get_categoricals_requests(self, pipeline_query: Type[PipelineQuery]) -> Set[ProcessingRequest]:
        """Gets processing requests, based on the categoricals table.

        If data in the categoricals table falls under the range and criteria
        specified by the pipeline query (times, mission id), and is related to
        a value that should be inserted to a CDF file, a processing request
        corresponding to that data will be created.

        Parameters
        ----------
        pipeline_query : Type[PipelineQuery]

        Returns
        -------
        Set[ProcessingRequest]
            A set of ENG processing requests relevant to the pipeline query
            that were calculated using information from the categoricals table
        """
        self.logger.info("➜  Getting ENG Categoricals requests")

        categoricals = [
            models.Categoricals.TMP_1,
            models.Categoricals.TMP_2,
            models.Categoricals.TMP_3,
            models.Categoricals.TMP_4,
            models.Categoricals.TMP_5,
            models.Categoricals.TMP_6,
        ]

        query = self.pipeline_config.session.query(
            models.Categorical.mission_id, func.date(models.Categorical.timestamp)
        ).distinct()
        if pipeline_query.times == TimeType.DOWNLINK:
            query = query.filter(
                models.Categorical.mission_id.in_(pipeline_query.mission_ids),
                models.Packet.timestamp >= pipeline_query.start_time,
                models.Packet.timestamp < pipeline_query.end_time,
                models.Categorical.name.in_(categoricals),
            ).join(models.Packet, models.Categorical.packet_id == models.Packet.id)
        elif pipeline_query.times == TimeType.COLLECTION:
            query = query.filter(
                models.Categorical.mission_id.in_(pipeline_query.mission_ids),
                models.Categorical.timestamp >= pipeline_query.start_time,
                models.Categorical.timestamp < pipeline_query.end_time,
                models.Categorical.name.in_(categoricals),
            )
        else:
            raise ValueError(f"Bad times: {pipeline_query.times}")

        categoricals_requests = {ProcessingRequest(mission_id, "eng", date) for mission_id, date in query}
        self.logger.info(
            f"➜  Got {len(categoricals_requests)} "
            + f"ENG Categoricals request{science_utils.s_if_plural(categoricals_requests)}"
        )
        self.logger.debug(
            f"🌥 🌥 🌥 🌥 🌥  Got {len(categoricals_requests)} "
            + f"categoricals processing request{science_utils.s_if_plural(categoricals_requests)}:\n\n\t"
            + "\n\t".join(str(br) for br in sorted(categoricals_requests))
            + "\n"
        )
        return categoricals_requests

    def get_bmon_requests(self, pipeline_query: Type[PipelineQuery]) -> Set[ProcessingRequest]:
        """Gets processing requests, based on the bmon table.

        If data in the bmon table falls under the range and criteria specified
        by the pipeline query (times, mission id), a processing request
        corresponding to that data will be created.

        Parameters
        ----------
        pipeline_query : Type[PipelineQuery]

        Returns
        -------
        Set[ProcessingRequest]
            A set of ENG processing requests relevant to the pipeline query
            that were calculated using information from the bmon table
        """
        self.logger.info("➜  Getting ENG Bmon requests")

        query = (
            self.pipeline_config.session.query(models.BmonData.mission_id, func.date(models.BmonData.timestamp))
            .distinct()
            .filter(models.BmonData.mission_id.in_(pipeline_query.mission_ids))
        )
        if pipeline_query.times == TimeType.DOWNLINK:
            query = query.filter(
                models.Packet.timestamp >= pipeline_query.start_time,
                models.Packet.timestamp < pipeline_query.end_time,
            ).join(models.Packet, models.BmonData.packet_id == models.Packet.id)
        elif pipeline_query.times == TimeType.COLLECTION:
            query = query.filter(
                models.BmonData.timestamp >= pipeline_query.start_time,
                models.BmonData.timestamp < pipeline_query.end_time,
            )
        else:
            raise ValueError(f"Bad times: {pipeline_query.times}")

        bmon_requests = {ProcessingRequest(mission_id, "eng", date) for mission_id, date in query}
        self.logger.info(
            f"➜  Got {len(bmon_requests)} " + f"ENG Bmon request{science_utils.s_if_plural(bmon_requests)}"
        )
        self.logger.debug(
            f"🌥 🌥 🌥 🌥 🌥  Got {len(bmon_requests)} "
            + f"bmon processing request{science_utils.s_if_plural(bmon_requests)}:\n\n\t"
            + "\n\t".join(str(br) for br in sorted(bmon_requests))
            + "\n"
        )
        return bmon_requests
