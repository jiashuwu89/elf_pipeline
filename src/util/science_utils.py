"""Utility functions relating to science.

Code written by science people/scientists belongs here
"""
from datetime import datetime, timedelta
from typing import List, Optional, Sized, Tuple

import numpy as np
import pandas as pd
from scipy.stats import chisquare
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


def handle_adjacent_sectors(df: pd.DataFrame) -> pd.DataFrame:
    """A function to address EPD adjacent sectors.

    Implemented by Jiashu Wu and integrated by James.

    Parameters
    ----------
    df : pd.DataFrame
        A Pandas DataFrame with columns "bin00" to "bin15", and column
        "idpu_type" (raw EPD bin counts and collection time).

    Returns
    -------
    pd.DataFrame
        A DataFrame whose bin counts are corrected. NOTE: no other columns are
        modified.
    """

    # --------------------------------------------------------------------------
    # search: 1. adjacent sector have the same value
    #         2. at least three channels are not zero
    #
    # correction:
    # problem: Yb and Yc have same value, Yd contains Yc' and Yd'
    #             log(Yb)   = m0 + m1*Xb + m2*Xb^2
    #             log(Yd/2) = m0 + m1*Xmid + m2*Xmid^2
    #             log(Ye)   = m0 + m1*Xe + m2*Xe^2
    #             initial Xmid=(Xc+Xd)/2
    #
    # step 1: parabola fit with three points
    #         find Xmid that gives delta y = Yc'+Yd'-Yd = 0
    #
    # step 2: if no delta y = 0 found after 5 iteration
    #         parabola fit with four points
    #         find Xmid that gives min chi square
    # --------------------------------------------------------------------------

    rawdata = np.transpose(
        np.array(
            [
                df["bin00"],
                df["bin01"],
                df["bin02"],
                df["bin03"],
                df["bin04"],
                df["bin05"],
                df["bin06"],
                df["bin07"],
                df["bin08"],
                df["bin09"],
                df["bin10"],
                df["bin11"],
                df["bin12"],
                df["bin13"],
                df["bin14"],
                df["bin15"],
            ]
        )
    )

    # --------------------------------------------------------------------------
    # search:
    # --------------------------------------------------------------------------
    sect_diff = np.absolute(np.diff(rawdata, axis=0))
    sect_diff_max = np.amax(sect_diff, axis=1)
    index1 = np.where(sect_diff_max == 0)

    # at least three channels are not zero
    sect_nozero = np.sum((rawdata > 0) * 1, axis=1)
    index2 = np.where(sect_nozero >= 3)

    index = np.intersect1d(index1[0], index2[0])

    # --------------------------------------------------------------------------
    # correction:
    # --------------------------------------------------------------------------

    maxiter = 4  # maximum iteration
    recurdata = np.copy(rawdata)
    datatime = pd.to_datetime(df["idpu_time"])

    for index_i in np.arange(index.shape[0]):
        for energy_i in np.arange(16):
            # exclude the cases with Yb Yd Ye have very similar values
            # a dip will show in the line plot if they are corrected
            std3point = np.std(
                [
                    rawdata[index[index_i], energy_i],
                    rawdata[index[index_i] + 2, energy_i],
                    rawdata[index[index_i] + 3, energy_i],
                ]
            )
            mean3point = np.mean(
                [
                    rawdata[index[index_i], energy_i],
                    rawdata[index[index_i] + 2, energy_i],
                    rawdata[index[index_i] + 3, energy_i],
                ]
            )
            if std3point < 0.1 * mean3point:
                continue

            # exclude the cases with Yb=Yc=Yd=0 or Yd=0
            y1 = np.log10(rawdata[index[index_i], energy_i]) if rawdata[index[index_i], energy_i] > 1 else 0
            y2 = (
                np.log10(0.5 * rawdata[index[index_i] + 2, energy_i])
                if 0.5 * rawdata[index[index_i] + 2, energy_i] > 1
                else 0
            )
            y3 = np.log10(rawdata[index[index_i] + 3, energy_i]) if rawdata[index[index_i] + 3, energy_i] > 1 else 0
            if (y1 + y2 + y3 == 0) or (y2 == 0):
                recurdata[index[index_i] + 1, energy_i] = 0  # Yc'=0
                recurdata[index[index_i] + 2, energy_i] = 0  # Yd'=0
                continue

            # exclude the cases with gap in time
            # datatime1 = pd.to_datetime(df["idpu_time"])
            x1 = (datatime[index[index_i]] - datatime[index[index_i]]).total_seconds()  # Xb
            x3 = (datatime[index[index_i] + 3] - datatime[index[index_i]]).total_seconds()  # Xe
            x4 = (datatime[index[index_i] + 1] - datatime[index[index_i]]).total_seconds()  # Xc
            x5 = (datatime[index[index_i] + 2] - datatime[index[index_i]]).total_seconds()  # Xd
            x2 = 0.5 * (x4 + x5)  # initial Xc+d/2=(Xc+Xd)/2

            # --------------------------------------------------------------------------
            # correction: step 1
            # use three points Yb Yd/2 Ye to solve the three parameters of parabola fit
            # first assume Xd/2=(Xc+Xd)/2
            # then iterate to find a Xd/2 that allow Yc'+Yd'=Yd
            # --------------------------------------------------------------------------

            nstep = 10  # time seperation in each interation
            tstart = x4  # initial=Xc
            tend = x5  # initial=Xd
            # iteration
            sumdiff = np.empty(nstep + 1)
            y4 = np.empty(nstep + 1)
            y5 = np.empty(nstep + 1)
            chisqmatrx = np.empty(nstep + 1)
            for iter_i in np.arange(maxiter + 1):
                tmatrix = np.linspace(tstart, tend, nstep + 1, endpoint=True)
                sumdiff[:] = np.nan
                y4[:] = np.nan
                y5[:] = np.nan
                for step_i in np.arange(nstep + 1):
                    x2 = tmatrix[step_i]
                    coeffs = np.polyfit([x1, x2, x3], [y1, y2, y3], 2)  # y=m0+m1*x+m2*x^2
                    model_poly = np.polyval(coeffs, [x4, x5])
                    y4[step_i] = np.power(10, model_poly[0])
                    y5[step_i] = np.power(10, model_poly[1])
                    sumdiff[step_i] = np.absolute(
                        y4[step_i] + y5[step_i] - rawdata[index[index_i] + 2, energy_i]
                    )  # delta y = Yc'+Yd'-Yd

                mindiff = np.min(sumdiff)
                minindex = np.where(sumdiff == mindiff)

                if mindiff > 1:
                    # if delta y > 1 then set tmatrix around minimum delta y and iterate
                    tstart = tmatrix[minindex[0][0] - 1] if minindex[0][0] > 0 else tmatrix[minindex[0][0]]
                    tend = tmatrix[minindex[0][0] + 1] if minindex[0][0] < nstep else tmatrix[minindex[0][0]]
                else:  # if  delta y = 0
                    recurdata[index[index_i] + 1, energy_i] = np.around(y4[minindex[0][0]])  # Yc'
                    recurdata[index[index_i] + 2, energy_i] = np.around(y5[minindex[0][0]])  # Yd'
                    break

            # --------------------------------------------------------------------------
            # correction: step 2
            # if no delta y = 0 found after 5 iteration use minimum chi square instead
            # --------------------------------------------------------------------------

            if (iter_i == maxiter) and (mindiff > 1):
                # add another point Ya or Yf, which one is larger
                if rawdata[index[index_i] - 1, energy_i] > rawdata[index[index_i] + 4, energy_i]:
                    y6 = (
                        np.log10(rawdata[index[index_i] - 1, energy_i])
                        if rawdata[index[index_i] - 1, energy_i] > 1
                        else 0
                    )  # Ya
                    x6 = (datatime[index[index_i] - 1] - datatime[index[index_i]]).total_seconds()  # Xa
                else:
                    y6 = (
                        np.log10(rawdata[index[index_i] + 4, energy_i])
                        if rawdata[index[index_i] + 4, energy_i] > 1
                        else 0
                    )  # Yf
                    x6 = (datatime[index[index_i] + 4] - datatime[index[index_i]]).total_seconds()  # Xa
                # start iteration
                tstart = x4
                tend = x5
                for iter_i in np.arange(maxiter + 1):
                    tmatrix = np.linspace(tstart, tend, nstep + 1, endpoint=True)
                    chisqmatrx[:] = np.nan
                    for step_i in np.arange(nstep + 1):
                        x2 = tmatrix[step_i]
                        coeffs = np.polyfit([x1, x2, x3, x6], [y1, y2, y3, y6], 2)
                        model_poly = np.polyval(coeffs, [x4, x5, x1, x2, x3, x6])
                        y4[step_i] = np.power(10, model_poly[0])
                        y5[step_i] = np.power(10, model_poly[1])
                        y_model = np.power(10, model_poly[2:6])
                        y_obsv = np.power(10, [y1, y2, y3, y6])
                        cresult = chisquare(y_model, f_exp=y_obsv)
                        chisqmatrx[step_i] = cresult.statistic

                    if iter_i == 0:  # first iteration
                        chisqmin_curr = np.min(chisqmatrx)
                        minindex = np.where(chisqmatrx == chisqmin_curr)
                        tstart = tmatrix[minindex[0][0] - 1] if minindex[0][0] > 0 else tmatrix[minindex[0][0]]
                        tend = tmatrix[minindex[0][0] + 1] if minindex[0][0] < nstep else tmatrix[minindex[0][0]]
                    else:  # after first iteration
                        chisqmin_prev = chisqmin_curr
                        chisqmin_curr = np.min(chisqmatrx)
                        minindex = np.where(chisqmatrx == chisqmin_curr)
                        if (
                            chisqmin_prev - chisqmin_curr
                        ) < 10 ** -4:  # if previous and current chi square are too close
                            recurdata[index[index_i] + 1, energy_i] = np.around(y4[minindex[0][0]])  # Yc'
                            recurdata[index[index_i] + 2, energy_i] = np.around(y5[minindex[0][0]])  # Yd'
                            break
                        else:
                            tstart = tmatrix[minindex[0][0] - 1] if minindex[0][0] > 0 else tmatrix[minindex[0][0]]
                            tend = tmatrix[minindex[0][0] + 1] if minindex[0][0] < nstep else tmatrix[minindex[0][0]]

        ## for develop only
        # fig = plt.figure()
        # plotindx = np.arange(31)-15
        # plotdata1 = rawdata[index[index_i]+plotindx,:].astype(np.float32)
        # plotdata1[plotdata1 == 0] = np.nan
        # ax1 = plt.subplot(2, 1, 1)
        # plt.plot(plotdata1)
        # ax1.set_yscale('log')
        # ax1lim = ax1.get_ylim()
        # ax1.set_ylim([1,ax1lim[1]])

        # plotdata2 = recurdata[index[index_i]+plotindx,:].astype(np.float32)
        # plotdata2[plotdata2 == 0] = np.nan
        # ax2 = plt.subplot(2, 1, 2)
        # plt.plot(plotdata2)
        # ax2.set_yscale('log')
        # ax2lim = ax2.get_ylim()
        # ax2.set_ylim([1,ax2lim[1]])

        # #plt.show()
        # plt.savefig(str(index_i)+'.png')
        # plt.close(fig)

    df["bin00"] = recurdata[:, 0]
    df["bin01"] = recurdata[:, 1]
    df["bin02"] = recurdata[:, 2]
    df["bin03"] = recurdata[:, 3]
    df["bin04"] = recurdata[:, 4]
    df["bin05"] = recurdata[:, 5]
    df["bin06"] = recurdata[:, 6]
    df["bin07"] = recurdata[:, 7]
    df["bin08"] = recurdata[:, 8]
    df["bin09"] = recurdata[:, 9]
    df["bin10"] = recurdata[:, 10]
    df["bin11"] = recurdata[:, 11]
    df["bin12"] = recurdata[:, 12]
    df["bin13"] = recurdata[:, 13]
    df["bin14"] = recurdata[:, 14]
    df["bin15"] = recurdata[:, 15]

    return df
