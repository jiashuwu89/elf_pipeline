import datetime as dt

import sqlalchemy
from elfin.common import models
from sqlalchemy.sql import func

from data_type.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter
from util import science_utils
from util.constants import MRM_PRODUCTS, MRM_TYPES


class MrmRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        self.logger.info("🏈\tGetting MRM Requests")
        mrm_products = self.get_mrm_products(pipeline_query.data_products)
        if not mrm_products:
            self.logger.info("Got 0 MRM processing requests")
            return set()

        sql_query = (
            self.pipeline_config.session.query(sqlalchemy.distinct(func.date(models.MRM.timestamp)))
            .filter(
                models.Packet.mission_id.in_(pipeline_query.mission_ids),
                models.Packet.timestamp >= pipeline_query.start_time,
                models.Packet.timestamp <= pipeline_query.end_time,
                models.MRM.mrm_type.in_(mrm_products),
            )
            .join(models.Packet)
        )

        mrm_processing_requests = {
            ProcessingRequest(res.mission_id, res.mrm_type, dt.date(res.date))
            for res in sql_query
            if res.date is not None
        }

        self.logger.info(
            f"🏈\tGot {len(mrm_processing_requests)} "
            + f"MRM processing request{science_utils.s_if_plural(mrm_processing_requests)}"
        )
        return mrm_processing_requests

    def get_mrm_products(self, data_products):
        mrm_products = set()
        for product in data_products:
            if product in MRM_PRODUCTS:
                mrm_products.update(MRM_TYPES[product])
        self.logger.debug(f"Found MRM Products: {mrm_products}")
        return mrm_products
