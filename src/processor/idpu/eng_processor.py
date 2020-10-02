import statistics

import numpy as np
import pandas as pd
from elfin.common import models

from processor.idpu.idpu_processor import IdpuProcessor
from util.constants import ONE_DAY_DELTA
from util.science_utils import dt_to_tt2000


class EngProcessor(IdpuProcessor):
    """A processor that generates ENG files"""

    # Get IDPU Types from processing request
    def generate_files(self, processing_request):
        """Generates a single level 1 ENG CDF related to the request.

        Parameters
        ----------
        processing_request
            A ProcessingRequest specifying that a specific ENG file be created

        Returns
        -------
        List[str]
            A list containing a single filename, the name of the generated
            level 1 ENG CDF
        """
        l1_file_name, _ = self.generate_l1_products(processing_request)  # Default param to None -> will generate l0 df

        return [l1_file_name]

    def generate_l0_df(self, processing_request):
        """Creates a level 0 DataFrame of ENG IDPU data.

        Even if there is no ENG IDPU data, there may be BMON or Categorical
        data that needs to be obtained later for the level 1 file. Therefore,
        an empty DataFrame is returned so that this later step is possible.

        Parameters
        ----------
        processing_request

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame of ENG IDPU data relating to the processing
            request; the DataFrame may be empty
        """
        try:
            l0_df = super().generate_l0_df(processing_request)
        except RuntimeError as e:
            self.logger.info(f"Level 0 DataFrame empty, initializing empty DataFrame: {e}")
            l0_df = pd.DataFrame()
        return l0_df

    def process_rejoined_data(self, processing_request, df):
        """Performs some transformations on data that has been rejoined.

        No major processing necessary for ENG level 0, apart from converting
        data into bytes

        Parameters
        ----------
        processing_request
        df : pd.DataFrame
            A DataFrame of rejoined data

        Returns
        -------
        pd.DataFrame
            A DataFrame in which the 'data' column has been converted to bytes
        """
        df["data"] = df["data"].apply(lambda x: bytes.fromhex(x) if x else None)
        return df

    def generate_l1_df(self, processing_request, l0_df):
        """Need to override default implementation to avoid cutting out data."""
        self.logger.info(f"🔵  Generating Level 1 DataFrame for {str(processing_request)}")
        if l0_df is None:
            self.logger.info("Still need a Level 0 DataFrame, generating now")
            l0_df = self.generate_l0_df(processing_request)

        # Allow derived class to transform data
        l1_df = self.transform_l0_df(processing_request, l0_df)

        lower_time_bound = np.datetime64(processing_request.date)
        upper_time_bound = np.datetime64(processing_request.date + ONE_DAY_DELTA)
        separated_dfs = []
        for time_col in ["idpu_time", "sips_time", "epd_time", "fgm_time", "fc_time"]:
            if time_col in l1_df.columns:
                cur_df = l1_df[(l1_df[time_col] >= lower_time_bound) & (l1_df[time_col] < upper_time_bound)]
                separated_dfs.append(cur_df)
            else:
                self.logger.debug(f"Couldn't find the column {time_col}, but it's probably OK")
        l1_df = pd.concat(separated_dfs, axis=0, ignore_index=True, sort=True)

        # Timestamp conversion
        for time_col in ["idpu_time", "sips_time", "epd_time", "fgm_time", "fc_time"]:
            if time_col in l1_df.columns:
                l1_df[time_col] = l1_df[time_col].apply(dt_to_tt2000)

        if l1_df.empty:
            raise RuntimeError(f"Final Dataframe is empty: {str(processing_request)}")

        return l1_df

    def transform_l0_df(self, processing_request, l0_df):
        """Creates an initial level 1 ENG DataFrame from a level 0 DataFrame.

        The resulting DataFrame will contain IDPU data from the level 0
        DataFrame, as well as FC and Battery Monitor Data from the
        corresponding tables.

        Parameters
        ----------
        processing_request
        l0_df : pd.DataFrame
            A DataFrame of level 0 ENG IDPU data
        """

        final_df = pd.DataFrame()

        # TODO: Rewrite this
        for _, row in l0_df.iterrows():
            to_add = {"idpu_time": row["idpu_time"]}
            to_add.update(self.extract_data(row["idpu_type"], row["data"], row["idpu_time"]))
            final_df = final_df.append(to_add, ignore_index=True, sort=False)

        fc_df = self.get_fc_df(processing_request)
        if not fc_df.empty:
            final_df = pd.concat([final_df, fc_df], axis=0, ignore_index=True, sort=True)
        else:
            self.logger.info("No FC Data")

        bmon_df = self.get_bmon_df(processing_request)
        if not bmon_df.empty:
            final_df = pd.concat([final_df, bmon_df], axis=0, ignore_index=True, sort=True)
        else:
            self.logger.info("No Battery Monitor Data")

        if final_df.empty:
            raise RuntimeError("Empty df")
        return final_df

    @staticmethod
    def extract_data(data_type, data, idpu_time):
        """From IDPU data, obtain a representative dictionary.

        Parameters
        ----------
        data_type : int
            Should be a value in {14, 15, 16}
        data : bytes
            Bytes of IDPU data to be reformatted
        idpu_time

        Returns
        -------
        dict
            A Dictionary that can be converted into a row of a level 1 ENG
            DataFrame
        """
        if data_type == 14:  # SIPS
            return {
                "sips_time": idpu_time,
                "sips_5v0_current": int.from_bytes(data[6:8], "big"),
                "sips_5v0_voltage": int.from_bytes(data[8:10], "big"),
                "sips_input_current": int.from_bytes(data[10:12], "big"),
                "sips_input_temp": int.from_bytes(data[12:14], "big"),
                "sips_input_voltage": int.from_bytes(data[14:16], "big"),
            }
        if data_type == 15:  # EPD
            return {
                "epd_time": idpu_time,
                "epd_biasl": int.from_bytes(data[0:2], "big"),
                "epd_biash": int.from_bytes(data[2:4], "big"),
                "epd_efe_temp": int.from_bytes(data[4:6], "big"),
            }
        if data_type == 16:  # FGM
            return {
                "fgm_time": idpu_time,
                "fgm_8_volt": int.from_bytes(data[0:2], "big"),  # status byte count 0
                "fgm_sh_temp": int.from_bytes(data[6:8], "big"),  # status byte count 3
                "fgm_3_3_volt": int.from_bytes(data[2:4], "big"),  # status byte count 1
                "fgm_analog_ground": int.from_bytes(data[4:6], "big"),  # status byte count 2
                "fgm_eu_temp": int.from_bytes(data[8:10], "big"),  # status byte count 4
            }
        raise ValueError(f"⚠️ \tWanted data type 14, 15, 16; instead got {data_type}")

    def get_fc_df(self, processing_request):
        """For a given processing request, gets relevant categorical data.

        Refer to name_converter dictionary for additional information

        Parameters
        ----------
        processing_request

        Returns
        -------
        pd.DataFrame
            A DataFrame of FC Data
        """
        name_converter = {
            models.Categoricals.TMP_1: "fc_idpu_temp",
            models.Categoricals.TMP_2: "fc_batt_temp_1",
            models.Categoricals.TMP_3: "fc_batt_temp_2",
            models.Categoricals.TMP_4: "fc_batt_temp_3",
            models.Categoricals.TMP_5: "fc_batt_temp_4",
            models.Categoricals.TMP_6: "fc_chassis_temp"
            # models.Categoricals.TMP_7: SHOULD BE HELIUM RADIO BUT NOT USED
        }

        query = self.session.query(models.Categorical).filter(
            models.Categorical.mission_id == processing_request.mission_id,
            models.Categorical.timestamp >= processing_request.date,
            models.Categorical.timestamp < processing_request.date + ONE_DAY_DELTA,
            models.Categorical.name.in_(name_converter.keys()),
        )

        fc_df = pd.DataFrame([{"fc_time": row.timestamp, name_converter[row.name]: row.value} for row in query])
        return fc_df

    def get_bmon_df(self, processing_request):
        """For a given processing request, gets relevant battery monitor data.

        NOTE: To calculate the values, need to average the two values provided for each time

        Parameters
        ----------
        processing_request

        Returns
        -------
        pd.DataFrame
            A DataFrame of Battery Monitor Data
        """

        query = self.session.query(models.BmonData).filter(
            models.BmonData.mission_id == processing_request.mission_id,
            models.BmonData.timestamp >= processing_request.date,
            models.BmonData.timestamp < processing_request.date + ONE_DAY_DELTA,
        )

        # TODO: Check if this works
        fc_temp = {1: {}, 2: {}}
        for row in query:
            row.timestamp = row.timestamp
            if row.timestamp not in fc_temp[row.power_board_id]:
                fc_temp[row.power_board_id][row.timestamp] = set()
            fc_temp[row.power_board_id][row.timestamp].add(row.temperature_register)

        fc_avionics_temp_1 = pd.DataFrame.from_dict(
            {
                "fc_time": list(fc_temp[1].keys()),
                "fc_avionics_temp_1": [statistics.mean(temps) for temps in fc_temp[1].values()],
            }
        )
        fc_avionics_temp_2 = pd.DataFrame.from_dict(
            {
                "fc_time": list(fc_temp[2].keys()),
                "fc_avionics_temp_2": [statistics.mean(temps) for temps in fc_temp[2].values()],
            }
        )

        bmon_df = pd.concat([fc_avionics_temp_1, fc_avionics_temp_2], axis=0, ignore_index=True, sort=True)
        return bmon_df

    def get_completeness_updater(self, processing_request):
        """No completeness calculations necessary for ENG data.

        Parameters
        ----------
        processing_request

        Returns
        -------
        None
        """
        return None

    def get_cdf_fields(self, processing_request):
        """Provides a mapping of CDF fields to DataFrame fields for ENG data.

        Parameters
        ----------
        processing_request

        Returns
        -------
        Dict[str, str]
            A mapping of CDF fields to ENG level 1 DataFrame fields
        """
        self.logger.debug(f"Getting CDF fields for processing request: {processing_request}")
        eng_fields = [
            "fc_time",
            "idpu_time",
            "fgm_time",
            "epd_time",
            "sips_time",
            "fc_avionics_temp_1",
            "fc_avionics_temp_2",
            "fc_batt_temp_1",
            "fc_batt_temp_2",
            "fc_batt_temp_3",
            "fc_batt_temp_4",
            "fc_chassis_temp",
            "fc_idpu_temp",
            "sips_5v0_current",
            "sips_5v0_voltage",
            "sips_input_current",
            "sips_input_temp",
            "sips_input_voltage",
            "epd_biash",
            "epd_biasl",
            "epd_efe_temp",
            "fgm_3_3_volt",
            "fgm_8_volt",
            "fgm_analog_ground",
            "fgm_eu_temp",
            "fgm_sh_temp",
        ]

        return {f"{processing_request.probe}_{field}": field for field in eng_fields}
