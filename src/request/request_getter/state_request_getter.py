import datetime as dt

import sqlalchemy
from sqlalchemy.sql import func

from common import models
from request.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter


class StateRequestGetter(RequestGetter):
    def __init__(self, session, mission_ids):
        super().__init__(session)

        self.mission_ids = mission_ids

    def get(self, start_time, end_time):
        state_processing_requests = set()

        # Always process certain days
        for mission_id in self.mission_ids:
            cur_day = start_time
            while cur_day <= end_time:
                state_processing_requests.add(ProcessingRequest(mission_id, "state", cur_day))
                cur_day += dt.timedelta(days=1)

        # Query to see what dates we picked up
        query = self.session.query(sqlalchemy.distinct(func.date(models.CalculatedAttitude.time))).filter(
            models.CalculatedAttitude.mission_id.in_(self.mission_ids),
            models.CalculatedAttitude.insert_date >= start_time,
            models.CalculatedAttitude.insert_date < end_time,
        )

        for res in query:
            # Figure out which dates around each found attitude we must calculate
            t = res.timestamp.date()
            cur = t - dt.timedelta(days=5)
            end_limit = t + dt.timedelta(days=5)
            while cur <= end_limit:
                state_processing_requests.add(ProcessingRequest(mission_id, "state", cur))
                cur += dt.timedelta(days=1)

        self.logger.debug(f"Got {len(state_processing_requests)} State processing requests")
        return state_processing_requests
