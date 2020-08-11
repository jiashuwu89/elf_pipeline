"""Utility functions relating to science"""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from spacepy import pycdf

np.set_printoptions(precision=20)


def dt_to_tt2000(dt):
    """Wrapper for spacepy date conversion function, handling null dates."""
    if pd.isnull(dt):
        return None
    return pycdf.lib.datetime_to_tt2000(dt)


def s_if_plural(x):
    """Determine whether a collection should be referred to as plural."""
    return "s" if len(x) > 1 else ""


def interpolate_attitude(S_init, t_init, S_fin, t_fin):
    """
    Wynne's Function to perform attitude interpolation

    Parameters:
        t_init, t_fin   First and Last times (Datetimes)
        S_init, S_fin   First and Last numpy arrays

    Return Value:
        Tuple of two numpy arrays:
        1. NP Array of DateTimes (One element per minute)
        2. NP Array of Attitudes (Interpolated)
    """
    # Find total angle difference
    phi_tot = np.arccos(np.dot(S_init, S_fin))
    # print('Angle between atts (deg):', phi_tot*180./np.pi)
    # print('Sinit, Sfin:', S_init, S_fin)

    # Find total time difference
    datetime_init = t_init
    datetime_fin = t_fin
    t_tot_obj = datetime_fin - datetime_init
    t_tot = t_tot_obj.total_seconds()

    # Define time array (1 soln per minute between t_init and t_fin), then extract time portion in dt to find nearest minute
    t_arr_minres = []
    if datetime_init.second > 0 or datetime_init.microsecond > 0:
        current_time = datetime(
            datetime_init.year, datetime_init.month, datetime_init.day, datetime_init.hour, datetime_init.minute + 1, 0
        )
    else:
        current_time = datetime_init
    while current_time <= datetime_fin:
        t_arr_minres.append(current_time)
        current_time += timedelta(0, 60)
    delta_t_arr = np.array([t - datetime_init for t in t_arr_minres])

    # TESTING: add time delta from Sinit to Sfinal
    # delta_t_arr=np.append(delta_t_arr, datetime_fin-datetime_init)

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


if __name__ == "__main__":
    # EXAMPLE FUNCTION CALL (using first two entries in ELA attitude file)
    Si = np.array([0.49483781545421507, 0.63977898197938476, 0.58806325392250303])
    Sf = np.array([0.55442686811141506, 0.55396341823980311, 0.62107598501974026])
    Ti = "2019-04-30/17:38:32"
    Tf = "2019-05-02/09:29:14"

    datetime_init = datetime.strptime(Ti, "%Y-%m-%d/%H:%M:%S")
    datetime_fin = datetime.strptime(Tf, "%Y-%m-%d/%H:%M:%S")

    interp_times, interp_atts = interpolate_attitude(Si, datetime_init, Sf, datetime_fin)

    print(interp_times, interp_atts)
