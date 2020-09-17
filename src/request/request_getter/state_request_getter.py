import datetime as dt

from elfin.common import models
from sqlalchemy.sql import func

from data_type.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter
from util import science_utils


class StateRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        self.logger.info("⚾️  Getting State Requests")
        state_processing_requests = set()
        if "state" not in pipeline_query.data_products:
            self.logger.info("⚾️  Got 0 State Requests")
            return state_processing_requests
        self.logger.info("Requested relevant products: 'state'")

        state_processing_requests.update(self.get_direct_requests(pipeline_query))
        state_processing_requests.update(self.get_attitude_requests(pipeline_query))

        self.logger.info(f"⚾️  Got {len(state_processing_requests)} State processing requests")
        return state_processing_requests

    def get_direct_requests(self, pipeline_query):
        self.logger.info("➜  Getting State direct requests")

        # Always process certain days
        direct_requests = set()
        mission_id = None
        for mission_id in pipeline_query.mission_ids:
            cur_day = pipeline_query.start_time.date()
            last_day = pipeline_query.end_time.date()
            while cur_day <= last_day:
                direct_requests.add(ProcessingRequest(mission_id, "state", cur_day))
                cur_day += dt.timedelta(days=1)

        self.logger.info(
            f"➜  Got {len(direct_requests)} " + f"State direct request{science_utils.s_if_plural(direct_requests)}"
        )
        return direct_requests

    def get_attitude_requests(self, pipeline_query):
        self.logger.info("➜  Getting State attitude requests")

        # Query to see what dates we picked up
        attitude_requests = set()
        query = (
            self.pipeline_config.session.query(
                models.CalculatedAttitude.mission_id, func.date(models.CalculatedAttitude.time)
            )
            .distinct()
            .filter(
                models.CalculatedAttitude.mission_id.in_(pipeline_query.mission_ids),
                models.CalculatedAttitude.insert_date >= pipeline_query.start_time,
                models.CalculatedAttitude.insert_date < pipeline_query.end_time,
            )
        )
        for mission_id, date in query:
            # Figure out which dates around each found attitude we must calculate
            cur_date = date - dt.timedelta(days=5)
            end_limit = date + dt.timedelta(days=5)
            while cur_date <= end_limit:
                attitude_requests.add(ProcessingRequest(mission_id, "state", cur_date))
                cur_date += dt.timedelta(days=1)

        self.logger.info(
            f"➜  Got {len(attitude_requests)} "
            + f"State attitude request{science_utils.s_if_plural(attitude_requests)}"
        )
        return attitude_requests
