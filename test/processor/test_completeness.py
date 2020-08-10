import datetime as dt

import pytest
from spacepy import pycdf

from src.processor import completeness


class Test_completeness_config:
    def test_one(self):
        with pytest.raises(ValueError):
            c = completeness.CompletenessConfig("BAD")

    def test_two(self):
        assert completeness.CompletenessConfig("mrm") != None

    def test_three(self):
        assert completeness.CompletenessConfig("fgm") != None

    def test_four(self):
        assert completeness.CompletenessConfig("epde") != None

    def test_five(self):
        assert completeness.CompletenessConfig("epdi") != None
