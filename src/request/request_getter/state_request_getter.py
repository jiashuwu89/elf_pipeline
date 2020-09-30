import datetime as dt
import os

from elfin.common import models
from sqlalchemy.sql import func

from data_type.processing_request import ProcessingRequest
from data_type.time_type import TimeType
from request.request_getter.request_getter import RequestGetter
from util import science_utils
from util.constants import MISSION_NAME_TO_ID_MAP, STATE_CALCULATE_RADIUS


class StateRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        self.logger.info("⚾️  Getting State Requests")
        state_processing_requests = set()
        if "state" not in pipeline_query.data_products:
            self.logger.info("⚾️  Got 0 State Requests")
            return state_processing_requests
        self.logger.info("Requested relevant products: 'state'")

        state_processing_requests.update(self.get_csv_requests(pipeline_query))
        state_processing_requests.update(self.get_attitude_requests(pipeline_query))

        self.logger.info(f"⚾️  Got {len(state_processing_requests)} State processing requests")
        return state_processing_requests

    def get_csv_requests(self, pipeline_query):
        """If necessary, create requests for all dates between dates in query.

        If we want to calculate requests given downlink times, it is not
        logical to create a request for every day between the given times, as
        we get state data from state csvs, which contains data (ex. position
        and velocity) organized by the time at which the data point occurs.
        This, to me, seems to behave as a 'collection time' than a 'downlink'
        time. The equivalent of 'downlink' time would be the time at which
        the files were created.

        Parameters
        ----------
        pipeline_query : PipelineQuery

        Returns
        -------
        Set[ProcessingRequest]
        """
        self.logger.info("➜  Getting State csv requests")

        csv_requests = set()
        if pipeline_query.times == TimeType.DOWNLINK:
            all_csv = os.listdir(self.pipeline_config.state_csv_dir)
            for csv in all_csv:  # TODO: Should this actually be mtime
                csv_datetime = dt.datetime.fromtimestamp(
                    os.stat(f"{self.pipeline_config.state_csv_dir}/{csv}").st_mtime
                )
                if pipeline_query.start_time <= csv_datetime < pipeline_query.end_time:
                    split_name = csv.split("_")
                    mission_id = MISSION_NAME_TO_ID_MAP[split_name[0]]
                    csv_date = dt.datetime.strptime(split_name[4], "%Y%m%d").date()
                    csv_requests.add(ProcessingRequest(mission_id, "state", csv_date))
        elif pipeline_query.times == TimeType.COLLECTION:  # Always process certain days
            mission_id = None
            for mission_id in pipeline_query.mission_ids:
                cur_day = pipeline_query.start_time.date()
                last_day = pipeline_query.end_time.date()
                while cur_day <= last_day:
                    csv_requests.add(ProcessingRequest(mission_id, "state", cur_day))
                    cur_day += dt.timedelta(days=1)
        else:
            raise ValueError(f"Bad times: {pipeline_query.times}")

        self.logger.info(f"➜  Got {len(csv_requests)} " + f"State csv request{science_utils.s_if_plural(csv_requests)}")
        return csv_requests

    def get_attitude_requests(self, pipeline_query):
        """Determines, based on attitude data times, what requests are needed.

        Parameters
        ----------
        pipeline_query : PipelineQuery

        Returns
        -------
        Set[ProcessingRequest]
        """
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
            )
        )
        if pipeline_query.times == TimeType.DOWNLINK:
            query = query.filter(
                models.CalculatedAttitude.insert_date >= pipeline_query.start_time,
                models.CalculatedAttitude.insert_date < pipeline_query.end_time,
            )
        elif pipeline_query.times == TimeType.COLLECTION:
            query = query.filter(
                models.CalculatedAttitude.time >= pipeline_query.start_time,
                models.CalculatedAttitude.time < pipeline_query.end_time,
            )
        else:
            raise ValueError(f"Bad times: {pipeline_query.times}")

        for mission_id, date in query:
            # Figure out which dates around each found attitude we must calculate
            cur_date = date - STATE_CALCULATE_RADIUS
            end_limit = date + STATE_CALCULATE_RADIUS
            while cur_date <= end_limit:
                attitude_requests.add(ProcessingRequest(mission_id, "state", cur_date))
                cur_date += dt.timedelta(days=1)

        self.logger.info(
            f"➜  Got {len(attitude_requests)} "
            + f"State attitude request{science_utils.s_if_plural(attitude_requests)}"
        )
        return attitude_requests
