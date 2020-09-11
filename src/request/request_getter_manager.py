"""Definition of class to determine data to process, files to generate"""
import logging

from request.request_getter.eng_request_getter import EngRequestGetter
from request.request_getter.idpu_request_getter import IdpuRequestGetter
from request.request_getter.mrm_request_getter import MrmRequestGetter
from request.request_getter.state_request_getter import StateRequestGetter
from util.constants import IDPU_TYPES, MRM_PRODUCTS, MRM_TYPES, SCIENCE_TYPES


class RequestGetterManager:
    """An object to determine mission/time/product combinations to process"""

    def __init__(self, session, calculate, update_db):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.session = session
        self.calculate = calculate
        self.update_db = update_db

    def get_processing_requests(self, mission_ids, data_products, times, start_time, end_time):
        """Determines IDPU, MRM, ENG, and State products to be created"""
        # TODO: "times" is just for idpu products?

        request_getters = []

        selected_idpu_products = set()
        selected_mrm_products = set()
        for product in data_products:
            if product in IDPU_TYPES:
                selected_idpu_products.update(SCIENCE_TYPES[product])
            elif product in MRM_PRODUCTS:
                selected_mrm_products.update(MRM_TYPES[product])

        if selected_idpu_products:
            request_getters.append(
                IdpuRequestGetter(
                    self.session, mission_ids, selected_idpu_products, times, self.calculate, self.update_db
                )
            )
        if selected_mrm_products:
            request_getters.append(MrmRequestGetter(self.session, mission_ids, selected_mrm_products))
        if "eng" in data_products:
            request_getters.append(EngRequestGetter(self.session))  # TODO: Eng requests by Mission ID
        if "state" in data_products:
            request_getters.append(StateRequestGetter(self.session, mission_ids))

        processing_requests = set()
        for request_getter in request_getters:
            processing_requests.update(request_getter.get(start_time, end_time))

        return processing_requests
