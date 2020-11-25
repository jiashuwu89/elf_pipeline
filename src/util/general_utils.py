"""General utility functions"""
import datetime as dt
import logging
from typing import Any, List

import numpy as np
from spacepy import pycdf

from util.constants import MAX_CDF_VALUE_DELTA


def convert_date_to_datetime(date: dt.date) -> dt.datetime:
    """Converts a date to the equivalent datetime at midnight."""
    return dt.datetime.combine(date, dt.datetime.min.time())


def equal_or_both_nan(a: Any, b: Any) -> bool:
    return (a == b) or (np.isnan(a) and np.isnan(b))


def compare_cdf(
    cdf_a: pycdf.CDF,
    cdf_b: pycdf.CDF,
    list_columns: List[str],
    single_columns: List[str],
    single_columns_allow_delta: List[str],
) -> None:
    """Compares the keys and specified columns of two CDFs.

    This will raise an AssertionError if something differs. This function was
    intended for use in test cases, to compare CDFs

    Parameters
    ----------
    cdf_a, cdf_b : pycdf.CDF
        The CDF objects to be compared
    list_columns : List[str]
        The keys for columns whose values are lists
    single_columns : List[str]
        The keys for columns whose values are single items
    single_columns_allow_delta : List[str]
        The keys for columns whose values are single items that must differ by
        less than MAX_CDF_VALUE_DELTA
    """
    logger = logging.getLogger(compare_cdf.__name__)

    logger.debug("Checking keys")
    assert cdf_a.keys() == cdf_b.keys()
    for key in cdf_a.keys():
        cdf_a_key = dict(cdf_a[key].attrs.items())
        cdf_b_key = dict(cdf_b[key].attrs.items())
        for a_item, b_item in zip(cdf_a_key.items(), cdf_b_key.items()):
            a_key, a_val = a_item
            b_key, b_val = b_item
            assert a_key == b_key
            if isinstance(a_val, np.ndarray):
                assert np.array_equal(a_val, b_val) is True
            else:
                assert equal_or_both_nan(a_val, b_val)

    logger.debug("Checking columns where each data point is a list/array of several values")
    for key in list_columns:
        logger.debug(f"Checking {key}")
        for new_row, expected_row in zip(cdf_a[key][...], cdf_b[key][...]):
            for new_val, expected_val in zip(new_row, expected_row):
                assert equal_or_both_nan(new_val, expected_val)

    logger.debug("Checking columns where each data point is a single value")
    for key in single_columns:
        logger.debug(f"Checking {key}")
        assert len(cdf_a[key][...]) == len(cdf_b[key][...])
        for new_row, expected_row in zip(cdf_a[key][...], cdf_b[key][...]):
            assert equal_or_both_nan(new_row, expected_row)

    logger.debug("Checking columns where each data point is a single value, and very small precision errors is allowed")
    for key in single_columns_allow_delta:
        logger.debug(f"Checking {key}")
        assert len(cdf_a[key][...]) == len(cdf_b[key][...])
        for new_row, expected_row in zip(cdf_a[key][...], cdf_b[key][...]):
            assert abs(new_row - expected_row) < MAX_CDF_VALUE_DELTA
