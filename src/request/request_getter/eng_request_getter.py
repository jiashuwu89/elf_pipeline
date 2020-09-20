from elfin.common import models
from sqlalchemy.sql import func

from data_type.processing_request import ProcessingRequest
from request.request_getter.request_getter import RequestGetter
from util import science_utils
from util.constants import SCIENCE_TYPES


# TODO: For all request getters, make sure that the data product is requested BEFORE getting requests
# TODO: requests gotten by categoricals and bmon, but what about idpu?
#   - May need to extract Downlink Manager from IDPU request getter, since it isn't as closely coupled
#     as I previously thought
class EngRequestGetter(RequestGetter):
    def get(self, pipeline_query):
        self.logger.info("⚽️  Getting ENG Requests")

        eng_processing_requests = set()

        if "eng" not in pipeline_query.data_products:
            self.logger.info("⚽️  Got 0 ENG Requests")
            return eng_processing_requests
        self.logger.info("Requested relevant products: 'eng'")

        # eng_processing_requests.update(self.get_idpu_requests(pipeline_query))
        eng_processing_requests.update(self.get_categoricals_requests(pipeline_query))
        eng_processing_requests.update(self.get_bmon_requests(pipeline_query))

        eng_processing_requests = {pr for pr in eng_processing_requests if pr.date.year != 2000}

        self.logger.info(
            f"⚽️  Got {len(eng_processing_requests)} "
            + f"ENG processing request{science_utils.s_if_plural(eng_processing_requests)}"
        )
        return eng_processing_requests

    def get_idpu_requests(self, pipeline_query):
        # TODO: How to differentiate between downlink time and collection time
        # TODO: make sure all end times are <, not <=
        self.logger.info("➜  Getting ENG IDPU requests")
        query = self.pipeline_config.session.query(
            models.SciencePacket.mission_id, func.date(models.SciencePacket.timestamp)
        ).filter(
            models.SciencePacket.mission_id == pipeline_query.mission_id,
            models.SciencePacket.timestamp >= pipeline_query.start_time,
            models.SciencePacket.timestamp < pipeline_query.end_time,
            models.SciencePacket.idpu_type.in_(SCIENCE_TYPES["eng"]),
        )
        idpu_requests = {ProcessingRequest(mission_id, "eng", date) for mission_id, date in query}

        self.logger.info(
            f"➜  Got {len(idpu_requests)} " + f"ENG IDPU request{science_utils.s_if_plural(idpu_requests)}"
        )
        return idpu_requests

    def get_categoricals_requests(self, pipeline_query):
        self.logger.info("➜  Getting ENG Categoricals requests")

        categoricals = [
            models.Categoricals.TMP_1,
            models.Categoricals.TMP_2,
            models.Categoricals.TMP_3,
            models.Categoricals.TMP_4,
            models.Categoricals.TMP_5,
            models.Categoricals.TMP_6,
        ]

        query = (  # TODO: Fix this, missing mission
            self.pipeline_config.session.query(models.Categorical.mission_id, func.date(models.Categorical.timestamp))
            .distinct()
            .filter(
                models.Categorical.mission_id.in_(pipeline_query.mission_ids),
                models.Packet.timestamp >= pipeline_query.start_time,
                models.Packet.timestamp < pipeline_query.end_time,
                models.Categorical.name.in_(categoricals),
            )
            .join(models.Packet, models.Categorical.packet_id == models.Packet.id)
        )

        categoricals_requests = {ProcessingRequest(mission_id, "eng", date) for mission_id, date in query}
        self.logger.info(
            f"➜  Got {len(categoricals_requests)} "
            + f"ENG Categoricals request{science_utils.s_if_plural(categoricals_requests)}"
        )
        return categoricals_requests

    def get_bmon_requests(self, pipeline_query):
        self.logger.info("➜  Getting ENG Bmon requests")

        query = (
            self.pipeline_config.session.query(models.BmonData.mission_id, func.date(models.BmonData.timestamp))
            .distinct()
            .filter(
                models.BmonData.mission_id.in_(pipeline_query.mission_ids),
                models.Packet.timestamp >= pipeline_query.start_time,
                models.Packet.timestamp < pipeline_query.end_time,
            )
            .join(models.Packet, models.BmonData.packet_id == models.Packet.id)
        )

        bmon_requests = {ProcessingRequest(mission_id, "eng", date) for mission_id, date in query}
        self.logger.info(
            f"➜  Got {len(bmon_requests)} " + f"ENG Bmon request{science_utils.s_if_plural(bmon_requests)}"
        )
        return bmon_requests
