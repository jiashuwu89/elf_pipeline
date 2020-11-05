"""Processor for State data, which includes things like attitude"""
import datetime as dt
from typing import List, Type

import numpy as np
import pandas as pd
from astropy import coordinates as a_c
from astropy.time import Time
from elfin.common import models
from spacepy import pycdf
from sqlalchemy import desc

from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from processor.science_processor import ScienceProcessor
from util import general_utils
from util.constants import ATTITUDE_SOLUTION_RADIUS, IDL_SCRIPT_VERSION, MINS_IN_DAY, ONE_DAY_DELTA
from util.science_utils import get_angle_between, interpolate_attitude

# TODO: Make sure that dfs input and output are consistent in terms of column types


class StateProcessor(ScienceProcessor):
    """The processor of State data.

    NOTE: Many of this processor's methods operate on the CDF object in place.
    Thus, many methods take in the CDF as a parameter and have no return
    value.

    Parameters
    ----------
    pipeline_config : Type[PipelineConfig]
    """

    def __init__(self, pipeline_config: Type[PipelineConfig]):
        super().__init__(pipeline_config)

        self.state_type = "defn"
        self.state_csv_dir = pipeline_config.state_csv_dir
        self.nan_df = pd.DataFrame()  # Holds nan_df if it needs to be reused (useful mostly for dumps)

    # TODO: Should this, as well as other processors, return a Set?
    def generate_files(self, processing_request: ProcessingRequest) -> List[str]:
        """Generates a single level 1 STATE CDF related to the request.

        There are no level 0 State products to generate.

        Parameters
        ----------
        processing_request
            A ProcessingRequest specifying that a specific ENG file be created

        Returns
        -------
        List[str]
            A list containing a single filename, the name of the generated
            level 1 State CDF
        """
        probe = processing_request.probe

        cdf_fname = self.make_filename(processing_request, 1)
        cdf = self.create_empty_cdf(cdf_fname)

        csv_df = self.combine_state_csvs(processing_request)
        self.update_cdf_with_csv_df(probe, csv_df, cdf)  # time, position, and velocity

        self.update_cdf_with_sun(processing_request, cdf)  # Sun/Shadow Variable

        # Inserting Attitude and Sun Calculations, if possible
        att_df = self.get_attitude(processing_request)
        if not att_df.empty:
            self.update_cdf_with_att_df(probe, att_df, cdf)
            self.update_cdf_with_sun_calculations(probe, csv_df, att_df, cdf)
        else:
            # Fill appropriate columns with NaN - Vassilis's request
            self.logger.warning(f"No attitude data found for {processing_request.date} (searched 30 days before/after)")
            self.update_cdf_with_nans(probe, cdf)

        cdf.close()
        return [cdf_fname]

    def make_filename(self, processing_request: ProcessingRequest, level: int, size=None) -> str:
        """Constructs the appropriate filename for a L1 file, and returns the full path

        Overrides default implementation of `make_filename`.

        Parameters
        ----------
        processing_request
        level : int
        size : Optional[int]

        Returns
        -------
        str
            The name of the file containing data corresponding to the
            given ProcessingRequest
        """
        fname = self.get_fname(processing_request.probe, level, self.state_type, processing_request.date)
        return f"{self.output_dir}/{fname}"

    @staticmethod
    def get_fname(probe: str, level: int, state_type: str, file_date: dt.datetime) -> str:
        return f"{probe}_l{level}_state_{state_type}_{file_date.strftime('%Y%m%d')}_v01.cdf"

    def create_empty_cdf(self, fname: str) -> pycdf.CDF:
        """Creates a CDF with the desired fname, using the correct mastercdf.

        If a corresponding file already exists, it will be removed.

        This overrides the default fill_cdf method in order to insert data
        about generation date and "MODS". TODO: See fill_cdf in MrmProcessor,
        it seems to do the same thing. This seems to be an example of code
        duplication that should be resolved cleanly.

        Parameters
        ----------
        fname : str
            The target path and filename of the file to be created

        Returns
        -------
        pycdf.CDF
            A CDF object associated with the given filename
        """
        datestr_run = dt.datetime.utcnow().strftime("%04Y-%02m-%02d")

        cdf = super().create_empty_cdf(fname)
        cdf.attrs["Generation_date"] = datestr_run
        cdf.attrs["MODS"] = "Rev- " + datestr_run

        return cdf

    def combine_state_csvs(self, processing_request: ProcessingRequest) -> pd.DataFrame:
        """Reads CSVs and returns values from 00:00:00 to 23:59:59.

        Parameters
        ----------
        processing_request: ProcessingRequest

        Returns
        -------
        pd.DataFrame
            A DataFrame of position and velocity data (both described in three
            columns representing X, Y, and Z)
        """

        df = None
        for d in [processing_request.date - ONE_DAY_DELTA, processing_request.date]:
            cdf_fname = self.get_fname(processing_request.probe, 1, self.state_type, d)
            csv_fname = f"{self.state_csv_dir}/{cdf_fname.split('/')[-1].rstrip('.cdf')}.csv"
            try:
                csv_df = self.read_state_csv(csv_fname)
                if df is not None:
                    df = df.append(csv_df)
                else:
                    df = csv_df
            except FileNotFoundError:
                self.logger.warning(f"File not found: {csv_fname}")

        if df is None:
            raise RuntimeError(
                f"Couldn't create csv_df, check /home/elfin-esn/state_data for csv for {processing_request.date}"
            )

        date_lower_bound = pd.Timestamp(processing_request.date)
        date_upper_bound = pd.Timestamp(processing_request.date + ONE_DAY_DELTA)
        return df.loc[(df.index >= date_lower_bound) & (df.index < date_upper_bound)]

    def read_state_csv(self, csv_fname: str) -> pd.DataFrame:
        """Read the state vectors CSV produced by STK.

        Parameters
        ----------
        csv_fname : str
            The name of a CSV file to read position and velocity data from

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the position and velocity data found in the
            given CSV
        """

        self.logger.debug(f"Reading {csv_fname}")
        csv_state = pd.read_csv(csv_fname)

        df = pd.DataFrame(columns=["time", "pos_gei", "vel_gei"])
        df["time"] = csv_state["Time (UTCG)"].apply(lambda x: dt.datetime.strptime(x, "%d %b %Y %H:%M:%S.%f"))

        pos_gei = np.asarray([csv_state["x (km)"], csv_state["y (km)"], csv_state["z (km)"]], dtype=np.float32)
        vel_gei = np.asarray(
            [csv_state["vx (km/sec)"], csv_state["vy (km/sec)"], csv_state["vz (km/sec)"]], dtype=np.float32
        )

        df["pos_gei"] = pos_gei.T.tolist()
        df["vel_gei"] = vel_gei.T.tolist()
        df = df.set_index("time")

        return df

    @staticmethod
    def update_cdf_with_csv_df(probe: str, csv_df: pd.DataFrame, cdf: pycdf.CDF) -> None:
        """Inserts position and velocity data into the provided CDF.

        Parameters
        ----------
        probe : str
            The probe name (ex. "ela" or "elb")
        csv_df : pd.DataFrame
            A DataFrame of position and velocity data, as obtained from a
            call to combine_state_csvs. TODO: refactor to have this method
            just call combine_state_csvs by itself, so that the parameter
            can just be the CDF and a ProcessingRequest
        cdf : pycdf.CDF
            The CDF object into which the position and velocity data should be
            inserted.
        """
        cdf_df = pd.DataFrame()
        cdf_df[f"{probe}_state_time"] = csv_df.index.values
        cdf_df[f"{probe}_state_time"] = cdf_df[f"{probe}_state_time"].apply(pycdf.lib.datetime_to_tt2000)
        cdf_df[f"{probe}_pos_gei"] = csv_df["pos_gei"].values
        cdf_df[f"{probe}_vel_gei"] = csv_df["vel_gei"].values
        for k in cdf_df.keys():
            cdf[k] = cdf_df[k].values.tolist()

    def update_cdf_with_sun(self, processing_request: ProcessingRequest, cdf: pycdf.CDF) -> None:
        """Inserts sun data into the given CDF.

        As a non-technical explanation, sun data refers to if the relevant
        satellite is in presence of the sun, or if it is in shadow.

        Values in the CDF can be either 0 or 1. Each 0 refers to umbra or
        penumbra. Each 1 represents the satellite being 'in sun'.

        Parameters
        ----------
        processing_request : ProcessingRequest
        cdf : pycdf.CDF
        """
        base_datetime = general_utils.convert_date_to_datetime(processing_request.date)
        end_time = base_datetime + dt.timedelta(seconds=86399)

        # Query for sun events in the Events table
        query = (
            self.session.query(models.Event)
            .filter(
                models.Event.type_id == 3,
                models.Event.start_time < end_time,  # TODO: check if it should be <=, timedelta of 1 day
                models.Event.stop_time > base_datetime,  # TODO: make sure date is right time
                models.Event.mission_id == processing_request.mission_id,
            )
            .order_by(desc(models.Event.id))
        )  # Larger ID -> More Recent -> More Accurate
        if query.count() == 0:
            self.logger.warning(f"No Sun Data between {processing_request.date} and {end_time}")
            cdf[f"{processing_request.probe}_sun"] = pd.Series()
            return

        # Initialize DataFrame to store times and corresponding 'sun value' which defaults to 0
        final_df = pd.DataFrame(columns=["time", "_sun"])
        base_datetime = general_utils.convert_date_to_datetime(processing_request.date)
        final_df["time"] = [base_datetime + dt.timedelta(seconds=i) for i in range(86400)]
        final_df["_sun"] = 0

        # Update Ranges in the DataFrame that are 'in sun'
        for row in query:
            if final_df.loc[(final_df.time >= row.start_time) & (final_df.time <= row.stop_time), "_sun"].any():
                self.logger.debug(f"Oh No! Overlapping Sun! {row.start_time} - {row.stop_time} is probably no good!")
                continue
            final_df.loc[(final_df.time >= row.start_time) & (final_df.time <= row.stop_time), "_sun"] = 1  # Check this

        cdf[f"{processing_request.probe}_sun"] = final_df["_sun"]

    def get_attitude(self, processing_request: ProcessingRequest) -> pd.DataFrame:
        """Get a DataFrame of Attitude data.

        For each minute in the day beginning on [start_time], find the
        attitude solution or the closest solution (up to 30 days difference).

        Approach:
            * Create DataFrame
            * Find potential attitude solutions with a time difference of at
              most 30 days. Find the first solutions above/below the range
              (start_time, end_time), and hold onto anything between them
              (including the found solutions)
            * Fill in DataFrame
            * Return DataFrame

        Parameters
        ----------
        processing_request : ProcessingRequest
            An object containing information about the desired request

        Returns
        -------
        pd.DataFrame
            A DataFrame of closest attitude solutions
        """

        base_datetime = general_utils.convert_date_to_datetime(processing_request.date)
        end_time = base_datetime + dt.timedelta(seconds=86399)

        # Query for solutions, find the solutions that we want to use (One extra one on either 'side')
        query = (
            self.session.query(models.CalculatedAttitude)
            .filter(
                models.CalculatedAttitude.time >= base_datetime - ATTITUDE_SOLUTION_RADIUS,
                models.CalculatedAttitude.time <= end_time + ATTITUDE_SOLUTION_RADIUS,
                models.CalculatedAttitude.mission_id == processing_request.mission_id,
                models.CalculatedAttitude.idl_script_version == IDL_SCRIPT_VERSION,
            )
            .order_by(models.CalculatedAttitude.time)
        )
        if query.count() == 0:
            return pd.DataFrame()
        query_list = self.select_usable_attitude_queries(query, base_datetime, end_time)
        query_list = self.drop_duplicate_attitude_queries(query_list)

        # Convert the queries into a dictionary format for easier access
        self.logger.debug(f"Potential DateTimes: {[q.time for q in query_list]}")
        q_dict_list = [self.get_q_dict(i) for i in query_list]

        # Preparing the final DF that is ultimately returned (Solution date is tt2000)
        final_df = pd.DataFrame(columns=["time", "time_dt", "solution_date", "X", "Y", "Z", "uncertainty"])
        final_df["time_dt"] = [base_datetime + dt.timedelta(minutes=i) for i in range(MINS_IN_DAY)]
        final_df["time"] = final_df["time_dt"].apply(pycdf.lib.datetime_to_tt2000)

        if len(q_dict_list) == 1:  # Edge Case: If there is only one result, just fill everything in with it
            final_df.loc[:, "X"] = q_dict_list[0]["X"]
            final_df.loc[:, "Y"] = q_dict_list[0]["Y"]
            final_df.loc[:, "Z"] = q_dict_list[0]["Z"]
            final_df.loc[:, "uncertainty"] = q_dict_list[0]["uncertainty"]
            final_df.loc[:, "solution_date"] = q_dict_list[0]["solution_date_tt2000"]
        else:
            final_df = self.insert_interpolated_attitude_data(final_df, q_dict_list)

        return final_df

    # TODO: Eliminate q_dict because I was stupid
    @staticmethod
    def get_q_dict(q):
        """ Turns some yucky SQL query result thing into a useful dict """
        q_dict = {
            "X": q.X,
            "Y": q.Y,
            "Z": q.Z,
            "uncertainty": q.uncertainty,
            "solution_date_dt": q.time,
            "solution_date_tt2000": pycdf.lib.datetime_to_tt2000(q.time),
        }
        return q_dict

    # TODO: Finish typing here
    @staticmethod
    def select_usable_attitude_queries(query, base_datetime: dt.datetime, end_time: dt.datetime):
        """Find the solutions that we want to use (One extra one on either 'side')

        TODO: Get types for query and return value

        Parameters
        ----------
        query
        base_datetime : dt.datetime
        end_time : dt.datetime

        Returns
        -------
        List[]
            A list of queries that are valid solutions.
        """
        query_list = []
        found_first = False
        for q in query:
            if not found_first and q.time <= end_time:
                if q.time >= base_datetime:
                    found_first = True
                    query_list.append(q)
                else:
                    query_list = [q]
                    if q.time > end_time:
                        break
            else:
                query_list.append(q)
                if q.time >= end_time:
                    break
        return query_list

    # TODO: Types
    def drop_duplicate_attitude_queries(self, query_list):
        """Remove duplicate attitudes from query_list.

        This is a helper function for get_attitude.

        Because attitude solutions are no longer replaced, we need to drop
        duplicates, favoring solutions with lower uncertainty.

        Parameters
        ----------
        query_list

        Returns
        -------
        ?
            A list of queries without duplicate values
        """
        query_list = sorted(query_list, key=lambda q: q.uncertainty if q.uncertainty else 0)
        seen = set()
        temp_query_list = []
        for q in query_list:
            if q.time not in seen:
                seen.add(q.time)
                temp_query_list.append(q)
            else:
                self.logger.debug(f"Attitude for {q.time} obtained on {q.insert_date}, but more recent solution exists")
        return sorted(temp_query_list, key=lambda q: q.time)

    def insert_interpolated_attitude_data(self, final_df: pd.DataFrame, q_dict_list) -> pd.DataFrame:
        """Adds relevant attitude information into the attitude DataFrame.

        This is a helper function for get_attitude

        Parameters
        ----------
        final_df : pd.DataFrame
        q_dict_list

        Returns
        -------
        pd.DataFrame
            A DataFrame with information from the queries provided in
            q_dict_list.
        """
        # Filling in X, Y, Z of times after the last solution, since can't do interpolation on them
        # Filling in solution_date and uncertainty for all items
        final_df.loc[:, "solution_date"] = q_dict_list[-1]["solution_date_tt2000"]
        final_df.loc[:, "uncertainty"] = q_dict_list[-1]["uncertainty"]

        # Interpolation (Using Wynne's code) and filling in before/after first and last solutions
        xyz_list = [[q["X"], q["Y"], q["Z"]] for q in q_dict_list]
        time_list_dt = [q["solution_date_dt"] for q in q_dict_list]
        for i in range(len(time_list_dt) - 1):
            self.logger.debug(
                f"Iteration {i}: From {xyz_list[i]}, {time_list_dt[i]} To {xyz_list[i + 1]}, {time_list_dt[i + 1]}"
            )
            times_dt, atts = interpolate_attitude(xyz_list[i], time_list_dt[i], xyz_list[i + 1], time_list_dt[i + 1])
            for t, a in zip(times_dt, atts):
                t = pycdf.lib.datetime_to_tt2000(t)
                if any(final_df["time"] == t):
                    final_df.loc[final_df["time"] == t, ["X", "Y", "Z"]] = a
        final_df.loc[final_df["time"] < q_dict_list[0]["solution_date_tt2000"], ["X", "Y", "Z"]] = xyz_list[0]
        final_df.loc[final_df["time"] > q_dict_list[-1]["solution_date_tt2000"], ["X", "Y", "Z"]] = xyz_list[-1]

        for i, q in enumerate(q_dict_list[:-1]):
            # - Only applicable if not the last query
            # - want to update anything not updated yet, below half way point
            unfilled = final_df["solution_date"] == q_dict_list[-1]["solution_date_tt2000"]
            below_halfway = (
                final_df["time"] < (q["solution_date_tt2000"] + q_dict_list[i + 1]["solution_date_tt2000"]) / 2
            )

            final_df.loc[unfilled & below_halfway, "solution_date"] = q["solution_date_tt2000"]
            final_df.loc[unfilled & below_halfway, "uncertainty"] = q["uncertainty"]

        return final_df

    @staticmethod
    def update_cdf_with_att_df(probe: str, att_df: pd.DataFrame, cdf: pycdf.CDF) -> None:
        """Adds attitude information to a given CDF

        Parameters
        ----------
        probe : str
        att_df : pd.DataFrame
            A DataFrame of Attitude data, as obtained from a call to
            get_attitude
        cdf : pycdf.CDF
        """
        cdf[probe + "_att_time"] = att_df["time"].values
        cdf[probe + "_att_solution_date"] = att_df["solution_date"].values
        cdf[probe + "_att_gei"] = att_df[["X", "Y", "Z"]].values
        cdf[probe + "_att_uncertainty"] = att_df["uncertainty"].values

    def update_cdf_with_sun_calculations(
        self, probe: str, vel_pos_df: pd.DataFrame, att_df: pd.DataFrame, cdf: pycdf.CDF
    ) -> None:
        """Function to calculate sun angle and orbnorm angle (in degrees)

        1. Prepare DataFrames
            - vel_pos_df: keep only rows that occur exactly on the minute
            - Check that vel_pos_df and att_df have 60 * 24 = 1440 rows
            - final_df: prepare columns
        2. Sun Angle Calculations:
            - Get sun position vector and attitude vector (stored in pd.Series)
            - Get angle between these vectors
        3. Orbnorm Angle Calculations:
            - Get the cross product of position and velocity vectors (stored in pd.Series)
            - Get angle between the resulting vector and attitude vector

        Parameters
        ----------
        probe : str
        vel_pos_df : pd.DataFrame
        att_df : pd.DataFrame
        cdf : pycdf.CDF
        """

        # Clean up DataFrames:
        # Delete duplicates, only keep times that are exactly on the minute, check length
        att_df = att_df.drop_duplicates(subset=["time"]).reset_index(drop=True)
        vel_pos_df = vel_pos_df.loc[~vel_pos_df.index.duplicated(keep="first")]
        vel_pos_df = vel_pos_df.loc[(vel_pos_df.index.second == 0) & (vel_pos_df.index.microsecond == 0)]
        if att_df.shape[0] != MINS_IN_DAY:
            raise ValueError(f"attitude DataFrame is the wrong shape (expected 1440): {att_df.shape[0]}")
        if vel_pos_df.shape[0] != MINS_IN_DAY:
            self.logger.warning(
                f"Bad velocity and position df, reducing attitude df from 1440 min/day to: {vel_pos_df.shape[0]}"
            )

            # TODO: Email Warning

            att_df = att_df[[i in vel_pos_df.index for i in att_df["time_dt"]]]
            att_df = att_df.reset_index(drop=True)

        # Creating DataFrame to be returned later
        final_df = pd.DataFrame(columns=["time", "_spin_sun_angle", "_spin_orbnorm_angle"])
        final_df["time"] = att_df["time"]

        # Sun Angle Calculations
        sun_v_series = pd.Series([a_c.get_sun(Time(att_df["time_dt"][i])).cartesian for i in range(len(att_df))])
        att_v_series = pd.Series([a_c.CartesianRepresentation(*i) for i in att_df[["X", "Y", "Z"]].values])
        final_df["_spin_sun_angle"] = get_angle_between(sun_v_series, att_v_series)

        # Orbit Normal Angle Calculations
        cross_pos_vel_series = pd.Series([np.cross(i, j) for i, j in zip(vel_pos_df["pos_gei"], vel_pos_df["vel_gei"])])
        cross_pos_vel_series = cross_pos_vel_series.apply(lambda x: a_c.CartesianRepresentation(*x))
        final_df["_spin_orbnorm_angle"] = get_angle_between(cross_pos_vel_series, att_v_series)

        # Fill in missing data with empty columns if necessary
        if att_df.shape[0] != MINS_IN_DAY:
            first_time = att_df["time_dt"][0].date()
            all_minutes = [
                dt.datetime.combine(first_time, dt.datetime.min.time()) + dt.timedelta(minutes=i)
                for i in range(MINS_IN_DAY)
            ]
            empty_minutes = [
                pycdf.lib.datetime_to_tt2000(i) for i in all_minutes if i not in att_df["time_dt"].to_list()
            ]
            empty_column = [None] * len(empty_minutes)
            empty_df = pd.DataFrame(
                {"time": empty_minutes, "_spin_sun_angle": empty_column, "_spin_orbnorm_angle": empty_column}
            )
            final_df = final_df.append(empty_df)
            final_df = final_df.sort_values(by="time").reset_index(drop=True)

        for column in ["_spin_sun_angle", "_spin_orbnorm_angle"]:
            cdf[probe + column] = final_df[column].values

    def update_cdf_with_nans(self, probe: str, cdf: pycdf.CDF) -> None:
        """Fills the given CDF with nan values.

        This should be used in the case that no attitude data is available.

        Parameters
        ----------
        probe : str
        cdf : pycdf.CDF
        """
        nan_numeric_cols = ["_att_solution_date", "_att_uncertainty", "_spin_sun_angle", "_spin_orbnorm_angle"]
        nan_df_cols = ["_att_time", "X", "Y", "Z"] + nan_numeric_cols

        # If nan_df needs to be created, then create it
        if self.nan_df.empty:
            self.nan_df = pd.DataFrame(columns=nan_df_cols)
            self.nan_df["_att_time"] = [dt.datetime(1, 1, 1) for _ in range(MINS_IN_DAY)]  # Lowest year is 1 in CDFs
            self.nan_df["_att_time"] = self.nan_df["_att_time"].apply(pycdf.lib.datetime_to_tt2000)
            for col in nan_numeric_cols:
                self.nan_df[col] = pd.to_numeric(self.nan_df[col], errors="coerce")

        cdf[probe + "_att_time"] = self.nan_df["_att_time"]
        cdf[probe + "_att_gei"] = self.nan_df[["X", "Y", "Z"]].values
        for col in nan_numeric_cols:
            cdf[probe + col] = self.nan_df[col]
