import datetime as dt

import sqlalchemy
from sqlalchemy.sql import func

from common import models
from types.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter
from util.constants import MRM_PRODUCTS, MRM_TYPES


class MrmRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        mrm_products = self.get_mrm_products(pipeline_query.data_products)
        if not mrm_products:
            return set()

        sql_query = (
            self.session.query(sqlalchemy.distinct(func.date(models.MRM.timestamp)))
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

        self.logger.debug(f"Got {len(mrm_processing_requests)} MRM processing requests")
        return mrm_processing_requests

    def get_mrm_products(self, data_products):
        mrm_products = set()
        for product in data_products:
            if product in MRM_PRODUCTS:
                mrm_products.update(MRM_TYPES[product])
        return mrm_products
