from elfin.common import models
from sqlalchemy.sql import func

from data_type.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter
from util import science_utils
from util.constants import MRM_TYPES


class MrmRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        self.logger.info("🏈  Getting MRM Requests")
        mrm_products = self.get_relevant_products(pipeline_query.data_products, MRM_TYPES)
        if not mrm_products:
            self.logger.info("🏈  Got 0 MRM processing requests")
            return set()
        self.logger.info(f"Requested relevant products: {mrm_products}")

        # TODO: This query does not seem right, or at least how it is being used
        sql_query = (
            self.pipeline_config.session.query(
                models.Packet.mission_id, models.MRM.mrm_type, func.date(models.MRM.timestamp)
            )
            .distinct()
            .filter(
                models.Packet.mission_id.in_(pipeline_query.mission_ids),
                models.Packet.timestamp >= pipeline_query.start_time,
                models.Packet.timestamp < pipeline_query.end_time,
                models.MRM.mrm_type.in_(mrm_products),
            )
            .join(models.Packet)
        )

        mrm_processing_requests = {
            ProcessingRequest(mission_id, mrm_type, date)
            for mission_id, mrm_type, date in sql_query
            if date is not None
        }

        self.logger.info(
            f"🏈  Got {len(mrm_processing_requests)} "
            + f"MRM processing request{science_utils.s_if_plural(mrm_processing_requests)}"
        )
        return mrm_processing_requests
