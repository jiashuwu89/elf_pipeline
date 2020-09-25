from request.request_getter.request_getter import RequestGetter
from util.constants import MRM_TYPES, SCIENCE_TYPES


class TestRequestGetter:
    def test_get_relevant_products(self):
        assert sorted(RequestGetter.get_relevant_products(["eng", "fgf"], SCIENCE_TYPES)) == [14, 15, 16, 17, 18]
        assert sorted(RequestGetter.get_relevant_products(["epdef", "mrma"], MRM_TYPES)) == ["ACB"]
