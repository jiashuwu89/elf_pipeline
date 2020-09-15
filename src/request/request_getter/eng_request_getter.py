import sqlalchemy
from elfin.common import models
from sqlalchemy.sql import func

from data_type.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter
from util import science_utils


class EngRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        self.logger.info("Getting ENG Requests")
        categoricals_requests = self.get_categoricals_requests(pipeline_query.start_time, pipeline_query.end_time)
        bmon_requests = self.get_bmon_requests(pipeline_query.start_time, pipeline_query.end_time)

        eng_processing_requests = categoricals_requests.union(bmon_requests)

        # TODO: s if plural
        self.logger.info(f"Got {len(eng_processing_requests)}" + f"ENG processing request{science_utils.s_if_plural(eng_processing_requests)}")
        return eng_processing_requests

    def get_categoricals_requests(self, start_time, end_time):
        categoricals = [
            models.Categoricals.TMP_1,
            models.Categoricals.TMP_2,
            models.Categoricals.TMP_3,
            models.Categoricals.TMP_4,
            models.Categoricals.TMP_5,
            models.Categoricals.TMP_6,
        ]
        categoricals_query = (  # TODO: Fix this, missing mission
            self.pipeline_config.session.query(sqlalchemy.distinct(func.date(models.Categorical.timestamp)))
            .filter(
                models.Categorical.mission_id == 1,  # TODO: IS THIS A PROBLEM? replace with mission_id.in_(mission_ids)
                models.Packet.timestamp >= start_time,
                models.Packet.timestamp < end_time,
                models.Categorical.name.in_(categoricals),
            )
            .join(models.Packet)
        )
        return {  # TODO: replace res.mission_id and res.timestamp (res[0]) if necessary
            ProcessingRequest(res.mission_id, "eng", res.timestamp.date()) for res in categoricals_query
        }

    def get_bmon_requests(self, start_time, end_time):
        bmon_query = (
            self.pipeline_config.session.query(sqlalchemy.distinct(func.date(models.BmonData.timestamp)))
            .filter(models.Packet.timestamp >= start_time, models.Packet.timestamp < end_time)
            .join(models.Packet)
        )
        return {ProcessingRequest(res.mission_id, "eng", res.timestamp.date()) for res in bmon_query}
