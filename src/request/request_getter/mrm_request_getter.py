import datetime as dt

import sqlalchemy
from sqlalchemy.sql import func

from common import models
from request.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter


class MrmRequestGetter(RequestGetter):
    def __init__(self, session, mission_ids, mrm_types):
        super().__init__(session)

        self.mission_ids = mission_ids
        self.mrm_types = mrm_types

    def get(self, start_time, end_time):
        query = (
            self.session.query(sqlalchemy.distinct(func.date(models.MRM.timestamp)))
            .filter(
                models.Packet.mission_id.in_(self.mission_ids),
                models.Packet.timestamp >= start_time,
                models.Packet.timestamp <= end_time,
                models.MRM.mrm_type.in_(self.mrm_types),
            )
            .join(models.Packet)
        )

        mrm_processing_requests = {
            ProcessingRequest(res.mission_id, res.mrm_type, dt.date(res.date)) for res in query if res.date is not None
        }

        self.logger.debug(f"Got {len(mrm_processing_requests)} MRM processing requests")
        return mrm_processing_requests
