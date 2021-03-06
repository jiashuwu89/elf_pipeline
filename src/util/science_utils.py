"""Utility functions relating to science."""
from datetime import datetime, timedelta
from typing import List, Optional, Sized, Tuple

import numpy as np
import pandas as pd
from spacepy import pycdf

from util.constants import SCIENCE_TYPES

np.set_printoptions(precision=20)


def dt_to_tt2000(dt: datetime) -> Optional[int]:
    """Wrapper for spacepy date conversion function, handling null dates."""
    if pd.isnull(dt):
        return None
    return pycdf.lib.datetime_to_tt2000(dt)


def s_if_plural(x: Sized) -> str:
    """Determine whether a collection should be referred to as plural."""
    return "s" if len(x) != 1 else ""


# TODO: Check these types
def twos_comp(uint_val: int, bits: int = 24) -> int:
    """Performs a two's complement conversion"""
    mask = 2 ** (bits - 1)
    return -(uint_val & mask) + (uint_val & ~mask)


def hex_to_int(hex_data: str) -> int:
    """Given a string of hex data, convert the string to an integer"""
    if not isinstance(hex_data, str):
        raise ValueError(f"Expected str, not {type(hex_data)}")
    return twos_comp(65536 * int(hex_data[0:2], 16) + 256 * int(hex_data[2:4], 16) + int(hex_data[4:6], 16))


def get_angle_between(v1: pd.Series, v2: pd.Series) -> pd.Series:
    """Get the angle between 2 pd.Series of astropy.CartesianRepresentations.

    Formula: Inverse cosine of the dot product of the two vectors (which have
    been converted to unit vectors)

    Parameters
    ----------
    v1, v1 : pd.Series
        Series of CartesianRepresentations

    Returns
    -------
    pd.Series
        Series of values representing the angle between values in v1 and v2
    """
    v1 = pd.Series(v1.apply(lambda x: x / x.norm()))
    v2 = pd.Series(v2.apply(lambda x: x / x.norm()))
    final_series = pd.Series([np.arccos(v1[i].dot(v2[i]).to_value()) for i in range(v1.size)])
    return final_series.apply(np.degrees)


def interpolate_attitude(
    S_init: np.ndarray, t_init: datetime, S_fin: np.ndarray, t_fin: datetime
) -> Tuple[np.ndarray, np.ndarray]:
    """Wynne's Function to perform attitude interpolation.

    Parameters
    ----------
    t_init : dt.datetime
        First time
    t_fin : dt.datetime
        Last time
    S_init : np.array
        First numpy array
    S_fin : np.array
        Last numpy array

    Returns
    -------
    Tuple[np.ndarray, np.ndarray]
        An NP Array of DateTimes (One element per minute), and an NP Array of
        Attitudes (Interpolated)

    """
    # Find total angle difference
    phi_tot = np.arccos(np.dot(S_init, S_fin))
    # print('Angle between atts (deg):', phi_tot*180./np.pi)
    # print('Sinit, Sfin:', S_init, S_fin)

    # Find total time difference
    t_tot = (t_fin - t_init).total_seconds()

    # Define time array (1 soln per minute between t_init and t_fin), then
    # extract time portion in dt to find nearest minute
    t_arr_minres = []
    if t_init.second > 0 or t_init.microsecond > 0:
        current_time = datetime(t_init.year, t_init.month, t_init.day, t_init.hour, t_init.minute + 1, 0)
    else:
        current_time = t_init
    while current_time <= t_fin:
        t_arr_minres.append(current_time)
        current_time += timedelta(0, 60)
    delta_t_arr = np.array([t - t_init for t in t_arr_minres])

    # TESTING: add time delta from Sinit to Sfinal
    # delta_t_arr=np.append(delta_t_arr, t_fin-t_init)

    delta_t_arr = np.array([dt.total_seconds() for dt in delta_t_arr])

    # NOTE: Commented this out
    # t_arr_minres=np.array([t.strftime('%Y-%m-%d/%H:%M:%S') for t in t_arr_minres]) # change back to readable strings

    # TESTING
    # t_arr_minres=np.append(t_arr_minres, t_fin)

    # Define vectors
    X_axis = S_init
    Sinit_cross_Sfin = np.cross(S_init, S_fin)
    Z_axis_rot = Sinit_cross_Sfin / np.linalg.norm(Sinit_cross_Sfin)
    Y_axis = np.cross(Z_axis_rot, X_axis)

    # GEI to new system rotation
    matrix = np.array([X_axis, Y_axis, Z_axis_rot])
    # Transform S_init to new system
    S_init_new = np.dot(matrix, S_init)
    # New system back to GEI rotation
    matrix_inv = np.linalg.inv(matrix)

    # Use rotation matrix to rotate about Z_axis_rot by phi(t)
    interpolated_atts = []
    for delta_t in delta_t_arr:
        phi = phi_tot * (delta_t / t_tot)
        rot_matrix = np.array([[np.cos(phi), -np.sin(phi), 0], [np.sin(phi), np.cos(phi), 0], [0, 0, 1]])
        new_vec = np.dot(rot_matrix, S_init_new)
        S_gei = np.dot(matrix_inv, new_vec)
        interpolated_atts.append(S_gei / np.linalg.norm(S_gei))

    return np.array(t_arr_minres), np.array(interpolated_atts)


def convert_data_product_to_idpu_types(data_product: str) -> List[int]:
    return SCIENCE_TYPES.get(data_product, [])


def convert_data_products_to_idpu_types(data_products: List[str]) -> List[int]:
    idpu_types = set()
    for product in data_products:
        idpu_types.update(convert_data_product_to_idpu_types(product))
    return list(idpu_types)
