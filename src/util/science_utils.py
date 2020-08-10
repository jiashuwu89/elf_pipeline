"""
Utility functions relating to science
"""
import pandas as pd
from spacepy import pycdf


def dt_to_tt2000(dt):
    """ Wrapper for spacepy date conversion function, handling null dates """
    if pd.isnull(dt):
        return None
    return pycdf.lib.datetime_to_tt2000(dt)


def s_if_plural(x):
    """ Determine whether a collection should be referred to as plural """
    return "s" if len(x) > 1 else ""
