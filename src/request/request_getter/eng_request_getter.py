import datetime as dt

import sqlalchemy
from elfin.common import models
from sqlalchemy.sql import func

from data_type.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter
from util import science_utils


# TODO: requests gotten by categoricals and bmon, but what about idpu?
class EngRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        self.logger.info("⚽️\tGetting ENG Requests")
        categoricals_requests = self.get_categoricals_requests(pipeline_query)
        bmon_requests = self.get_bmon_requests(pipeline_query)

        eng_processing_requests = categoricals_requests.union(bmon_requests)

        # TODO: s if plural
        self.logger.info(
            f"⚽️\tGot {len(eng_processing_requests)} "
            + f"ENG processing request{science_utils.s_if_plural(eng_processing_requests)}"
        )
        return eng_processing_requests

    def get_categoricals_requests(self, pipeline_query):
        self.logger.info("➜\tGetting ENG Categoricals requests")
        mission_ids = pipeline_query.mission_ids
        start_time = pipeline_query.start_time
        end_time = pipeline_query.end_time

        categoricals = [
            models.Categoricals.TMP_1,
            models.Categoricals.TMP_2,
            models.Categoricals.TMP_3,
            models.Categoricals.TMP_4,
            models.Categoricals.TMP_5,
            models.Categoricals.TMP_6,
        ]

        categoricals_requests = set()

        # TODO: make into single query
        for mission_id in mission_ids:
            query = (  # TODO: Fix this, missing mission
                self.pipeline_config.session.query(sqlalchemy.distinct(func.date(models.Categorical.timestamp)))
                .filter(
                    models.Categorical.mission_id == mission_id,
                    models.Packet.timestamp >= start_time,
                    models.Packet.timestamp < end_time,
                    models.Categorical.name.in_(categoricals),
                )
                .join(models.Packet)
            )

            current_requests = {
                ProcessingRequest(mission_id, "eng", dt.datetime.combine(res[0], dt.datetime.min.time()))
                for res in query
            }
            categoricals_requests.update(current_requests)

        self.logger.info(
            f"➜\tGot {len(categoricals_requests)} "
            + f"ENG Categoricals request{science_utils.s_if_plural(categoricals_requests)}"
        )
        return categoricals_requests

    def get_bmon_requests(self, pipeline_query):
        self.logger.info("➜\tGetting ENG Bmon requests")
        mission_ids = pipeline_query.mission_ids
        start_time = pipeline_query.start_time
        end_time = pipeline_query.end_time

        bmon_requests = set()

        for mission_id in mission_ids:
            query = (
                self.pipeline_config.session.query(sqlalchemy.distinct(func.date(models.BmonData.timestamp)))
                .filter(
                    models.BmonData.mission_id == mission_id,
                    models.Packet.timestamp >= start_time,
                    models.Packet.timestamp < end_time,
                )
                .join(models.Packet)
            )

            current_requests = {
                ProcessingRequest(mission_id, "eng", dt.datetime.combine(res[0], dt.datetime.min.time()))
                for res in query
            }
            bmon_requests.update(current_requests)

        self.logger.info(
            f"➜\tGot {len(bmon_requests)} " + f"ENG Bmon request{science_utils.s_if_plural(bmon_requests)}"
        )
        return bmon_requests
