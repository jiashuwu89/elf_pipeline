"""Processor for State data, which includes things like attitude"""
import datetime as dt

import numpy as np
import pandas as pd
from astropy import coordinates as a_c
from astropy.time import Time
from spacepy import pycdf
from sqlalchemy import desc

from common import models
from processor.science_processor import ScienceProcessor
from util.constants import IDL_SCRIPT_VERSION, STATE_CSV_DIR
from util.science_utils import interpolate_attitude


class StateProcessor(ScienceProcessor):
    """The processor of State data"""

    def __init__(self, pipeline_config):
        super().__init__(pipeline_config)

        self.state_type = "defn"
        self.nan_df = pd.DataFrame()  # Holds nan_df if it needs to be reused (useful mostly for dumps)

    def generate_files(self, processing_request):
        """Generate L1 state file for processing_request"""
        probe = processing_request.probe

        cdf_fname = self.make_filename(processing_request, 1)
        cdf = self.create_cdf(cdf_fname)

        csv_df = self.combine_state_csvs(processing_request.date)
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

    def make_filename(self, processing_request, level, size=None):
        """Constructs the appropriate filename for a L1 file, and returns the full path

        Overrides default implementation
        """
        probe = processing_request.probe
        file_date = processing_request.date.strftime("%Y%m%d")
        fname = f"{probe}_l{level}_state_{self.state_type}_{file_date}_v01.cdf"
        return f"{self.output_dir}/{fname}"

    def create_cdf(self, fname):
        datestr_run = dt.datetime.utcnow().strftime("%04Y-%02m-%02d")

        cdf = super().create_cdf(fname)
        cdf.attrs["Generation_date"] = datestr_run
        cdf.attrs["MODS"] = "Rev- " + datestr_run

        return cdf

    def combine_state_csvs(self, date):
        """Reads CSVs and returns values from 00:00:00 to 23:59:59."""

        df = None
        for d in [date - dt.timedelta(days=1), date]:
            cdf_fname = self.make_filename(level=1, collection_date=d)  # TODO: FIx this
            csv_fname = STATE_CSV_DIR + cdf_fname.split("/")[-1].rstrip(".cdf") + ".csv"

            try:
                csv_df = self.read_state_csv(csv_fname)
                if df:
                    df = df.append(csv_df)
                else:
                    df = csv_df
            except FileNotFoundError:
                self.logger.warning(f"File not found: {csv_fname}")

        if not df:
            raise RuntimeError(
                f"csv_df could not be created! Check /home/elfin-esn/state_data to see if csv exists for {date}"
            )

        return df.loc[(df.index >= date) & (df.index < date + dt.timedelta(days=1))]

    def read_state_csv(self, csv_fname):
        """Read the state vectors CSV produced by STK."""

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

    def update_cdf_with_csv_df(self, probe, csv_df, cdf):
        self.logger.debug("Updating State CDF with position and velocity data")
        cdf_df = pd.DataFrame()
        cdf_df[f"{probe}_state_time"] = csv_df.index.values
        cdf_df[f"{probe}_state_time"] = cdf_df[f"{probe}_state_time"].apply(pycdf.lib.datetime_to_tt2000)
        cdf_df[f"{probe}_pos_gei"] = csv_df["pos_gei"].values
        cdf_df[f"{probe}_vel_gei"] = csv_df["vel_gei"].values
        for k in cdf_df.keys():
            cdf[k] = cdf_df[k].values.tolist()

    def update_cdf_with_sun(self, processing_request, cdf):
        """
        Each 1 represents the satellite being 'in sun'
        Each 0 refers to umbra or penumbra
        """
        end_time = processing_request.date + dt.timedelta(seconds=86399)

        # Query for sun events in the Events table
        query = (
            self.session.query(models.Event)
            .filter(
                models.Event.type_id == 3,
                models.Event.start_time < end_time,
                models.Event.stop_time > processing_request.date,  # TODO: make sure date is right time
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
        final_df["time"] = [processing_request.date + dt.timedelta(seconds=i) for i in range(86400)]
        final_df["_sun"] = 0

        # Update Ranges in the DataFrame that are 'in sun'
        for row in query:
            selection = final_df.loc[(final_df.time >= row.start_time) & (final_df.time <= row.stop_time), "_sun"]
            if selection.any():
                self.logger.debug(f"Oh No! Overlapping Sun! {row.start_time} - {row.stop_time} is probably no good!")
                continue
            selection = 1  # Check this

        cdf[f"{processing_request.probe}_sun"] = final_df["_sun"]

    def get_attitude(self, processing_request):
        """
        For each minute in the day beginning on [start_time], find the attitude solution
        or the closest solution (up to 30 days difference).

        Approach:
        1. Create DataFrame
        2. Find potential attitude solutions with a time difference of at most 30 days
            - Find the first solutions above/below the range (start_time, end_time), and
              hold onto anything between them (including the found solutions)
        3. Fill in DataFrame
            - TODO: Interpolation is performed, but seems to disagree with XiaoJia's code
        4. Return DataFrame
        """

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

        # Preparing the final DF that is ultimately returned (Solution date is tt2000)
        final_df = pd.DataFrame(columns=["time", "time_dt", "solution_date", "X", "Y", "Z", "uncertainty"])
        final_df["time_dt"] = [processing_request.date + dt.timedelta(minutes=i) for i in range(60 * 24)]
        final_df["time"] = final_df["time_dt"].apply(pycdf.lib.datetime_to_tt2000)

        # Query for solutions
        end_time = processing_request.date + dt.timedelta(seconds=86399)
        first_possible_time = processing_request.date - dt.timedelta(days=30)
        last_possible_time = end_time + dt.timedelta(days=30)
        query = (
            self.session.query(models.CalculatedAttitude)
            .filter(
                models.CalculatedAttitude.time >= first_possible_time,
                models.CalculatedAttitude.time <= last_possible_time,
                models.CalculatedAttitude.mission_id == processing_request.mission_id,
                models.CalculatedAttitude.idl_script_version == IDL_SCRIPT_VERSION,
            )
            .order_by(models.CalculatedAttitude.time)
        )
        if query.count() == 0:
            return pd.DataFrame()

        # Find the solutions that we want to use (One extra one on either 'side')
        query_list = []
        found_first = False
        for q in query:
            if not found_first and q.time <= end_time:
                if q.time >= processing_request.date:
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

        # Because attitude solutions are no longer replaced, we need to drop
        # duplicates, favoring solutions with lower uncertainty
        query_list = sorted(query_list, key=lambda q: q.uncertainty if q.uncertainty else 0)
        seen = set()
        temp_query_list = []
        for q in query_list:
            if q.time not in seen:
                seen.add(q.time)
                temp_query_list.append(q)
            else:
                self.logger.debug(f"Attitude for {q.time} obtained on {q.insert_date}, but more recent solution exists")
        query_list = sorted(temp_query_list, key=lambda q: q.time)

        # Convert the queries into a dictionary format for easier access
        self.logger.debug(f"Potential DateTimes: {[q.time for q in query_list]}")
        q_dict_list = [get_q_dict(i) for i in query_list]

        # Edge Case: If there is only one result, just fill everything in with it
        if len(q_dict_list) == 1:
            q = q_dict_list[0]
            final_df.loc[:, "X"] = q["X"]
            final_df.loc[:, "Y"] = q["Y"]
            final_df.loc[:, "Z"] = q["Z"]
            final_df.loc[:, "uncertainty"] = q["uncertainty"]
            final_df.loc[:, "solution_date"] = q["solution_date_tt2000"]
            return final_df

        # Filling in X, Y, Z of times after the last solution, since can't do interpolation on them
        # Filling in solution_date and uncertainty for all items
        final_q = q_dict_list[-1]
        final_df.loc[:, "solution_date"] = final_q["solution_date_tt2000"]
        final_df.loc[:, "uncertainty"] = final_q["uncertainty"]

        # Interpolation (Using Wynne's code) and filling in before/after first and last solutions
        xyz_list = [[q["X"], q["Y"], q["Z"]] for q in q_dict_list]
        time_list_dt = [q["solution_date_dt"] for q in q_dict_list]
        for i in range(len(time_list_dt) - 1):
            self.logger.debug(
                f"Iteration {i}: From {xyz_list[i]}, {time_list_dt[i]} To {xyz_list[i + 1]}, {time_list_dt[i + 1]}"
            )
            times_dt, atts = interpolate_attitude(xyz_list[i], time_list_dt[i], xyz_list[i + 1], time_list_dt[i + 1])
            final_df_time = final_df["time"]
            for t, a in zip(times_dt, atts):
                t = pycdf.lib.datetime_to_tt2000(t)
                if any(final_df_time == t):
                    final_df.loc[final_df["time"] == t, ["X", "Y", "Z"]] = a
        final_df.loc[final_df["time"] < q_dict_list[0]["solution_date_tt2000"], ["X", "Y", "Z"]] = xyz_list[0]
        final_df.loc[final_df["time"] > q_dict_list[-1]["solution_date_tt2000"], ["X", "Y", "Z"]] = xyz_list[-1]

        for idx, q in enumerate(q_dict_list[:-1]):
            # needs_closest_sol is used for closest attitude solution
            # - Only applicable if not the last query
            # - want to update anything not updated yet, below half way point
            unfilled = final_df["solution_date"] == final_q["solution_date_tt2000"]
            halfway_limit = (q["solution_date_tt2000"] + q_dict_list[idx + 1]["solution_date_tt2000"]) / 2
            below_halfway = final_df["time"] < halfway_limit
            needs_closest_sol = unfilled & below_halfway

            final_df.loc[needs_closest_sol, "solution_date"] = q["solution_date_tt2000"]
            final_df.loc[needs_closest_sol, "uncertainty"] = q["uncertainty"]

        return final_df

    def update_cdf_with_att_df(self, probe, att_df, cdf):
        self.logger.debug("Updating CDF with attitude dataframe")
        cdf[probe + "_att_time"] = att_df["time"]
        cdf[probe + "_att_solution_date"] = att_df["solution_date"]
        cdf[probe + "_att_gei"] = att_df[["X", "Y", "Z"]].values
        cdf[probe + "_att_uncertainty"] = att_df["uncertainty"].values

    def update_cdf_with_sun_calculations(self, probe, vel_pos_df, att_df, cdf):
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
        """

        def get_angle_between(v1: pd.Series(), v2: pd.Series()) -> pd.Series():
            """
            Get the angle between two pd.Series of astropy.CartesianRepresentations
            Formula: Inverse cosine of the dot product of the two vectors (which have been converted to unit vectors)
            """
            v1 = pd.Series(v1.apply(lambda x: x / x.norm()))
            v2 = pd.Series(v2.apply(lambda x: x / x.norm()))
            final_series = pd.Series([np.arccos(v1[i].dot(v2[i]).to_value()) for i in range(v1.size)])
            return final_series.apply(np.degrees)

        # Clean up DataFrames:
        # Delete duplicates, only keep times that are exactly on the minute, check length
        att_df = att_df.drop_duplicates(subset=["time"]).reset_index(drop=True)
        vel_pos_df = vel_pos_df.loc[~vel_pos_df.index.duplicated(keep="first")]
        vel_pos_df = vel_pos_df.loc[(vel_pos_df.index.second == 0) & (vel_pos_df.index.microsecond == 0)]
        if att_df.shape[0] != 60 * 24:
            raise ValueError(f"attitude DataFrame is the wrong shape (expected 1440): {att_df.shape[0]}")
        if vel_pos_df.shape[0] != 60 * 24:
            msg = f"Bad velocity and position df, reducing attitude df from 1440 min/day to: {vel_pos_df.shape[0]}"
            self.logger.warning(msg)

            # TODO: Email Warning

            att_df = att_df[[i in vel_pos_df.index for i in att_df["time_dt"]]]
            att_df = att_df.reset_index(drop=True)

        # Creating DataFrame to be returned later
        final_df = pd.DataFrame(columns=["time", "_spin_sun_angle", "_spin_orbnorm_angle"])
        final_df["time"] = att_df["time"]

        # Sun Angle Calculations
        sun_v_list = [a_c.get_sun(Time(att_df["time_dt"][i])).cartesian for i in range(len(att_df))]
        att_v_list = [a_c.CartesianRepresentation(*i) for i in att_df[["X", "Y", "Z"]].values]
        sun_v_series = pd.Series(sun_v_list)
        att_v_series = pd.Series(att_v_list)
        final_df["_spin_sun_angle"] = get_angle_between(sun_v_series, att_v_series)

        # Orbit Normal Angle Calculations
        cross_pos_vel_list = [np.cross(i, j) for i, j in zip(vel_pos_df["pos_gei"], vel_pos_df["vel_gei"])]
        cross_pos_vel_series = pd.Series(cross_pos_vel_list)
        cross_pos_vel_series = cross_pos_vel_series.apply(lambda x: a_c.CartesianRepresentation(*x))
        final_df["_spin_orbnorm_angle"] = get_angle_between(cross_pos_vel_series, att_v_series)

        # Fill in missing data with empty columns if necessary
        if att_df.shape[0] != 1440:
            first_time = att_df["time_dt"][0].date()
            all_minutes = [
                dt.datetime.combine(first_time, dt.datetime.min.time()) + dt.timedelta(minutes=i)
                for i in range(60 * 24)
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
            cdf[probe + column] = final_df[column]

    def update_cdf_with_nans(self, probe, cdf):
        nan_numeric_cols = ["_att_solution_date", "_att_uncertainty", "_spin_sun_angle", "_spin_orbnorm_angle"]
        nan_df_cols = ["_att_time", "X", "Y", "Z"] + nan_numeric_cols

        # If nan_df needs to be created, then create it
        if self.nan_df.empty:
            self.nan_df = pd.DataFrame(columns=nan_df_cols)
            self.nan_df["_att_time"] = [dt.datetime(1, 1, 1) for _ in range(60 * 24)]  # Lowest year is 1 in CDFs
            self.nan_df["_att_time"] = self.nan_df["_att_time"].apply(pycdf.lib.datetime_to_tt2000)
            for col in nan_numeric_cols:
                self.nan_df[col] = pd.to_numeric(self.nan_df[col], errors="coerce")

        cdf[probe + "_att_time"] = self.nan_df["_att_time"]
        cdf[probe + "_att_gei"] = self.nan_df[["X", "Y", "Z"]].values
        for col in nan_numeric_cols:
            cdf[probe + col] = self.nan_df[col]
