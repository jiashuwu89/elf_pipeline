"""Class to generate ENG files

Classes:

    EngProcessor
"""
import datetime as dt
import statistics

import pandas as pd
from elfin.common import models
from spacepy import pycdf

from processor.idpu.idpu_processor import IdpuProcessor
from util.science_utils import dt_to_tt2000


class EngProcessor(IdpuProcessor):
    """Class to generate ENG files"""

    # Get IDPU Types from processing request
    def generate_files(self, processing_request):
        l1_file_name, _ = self.generate_l1_products(processing_request)  # Default param to None -> will generate l0 df

        return [l1_file_name]

    def generate_l0_df(self, processing_request):
        try:
            l0_df = self.generate_l0_df(processing_request)
        except RuntimeError as e:
            self.logger.info(f"Level 0 DataFrame empty, initializing empty DataFrame: {e}")
            l0_df = pd.DataFrame()
        return l0_df

    def process_rejoined_data(self, processing_request, df):
        """No major processing necessary for ENG level 0"""
        data_bytes = []
        for _, row in df.iterrows():
            if row["data"] is not None:
                data_bytes.append(bytes.fromhex(row["data"]))
            else:
                data_bytes.append(None)
        df["data"] = data_bytes
        return df

    def transform_l0_df(self, processing_request, l0_df):
        """
        Creates Dataframe using Inputed Data, as well as FC and Battery Monitor
        Data found using EngDownlinkManager
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

    def extract_data(self, data_type, data, idpu_time):
        """ Helper Function for Transform Level 0 """
        self.logger.debug(f"Data type {data_type} is good? {data_type in (14, 15, 16)}")
        if data_type == 14:  # SIPS
            return {
                "sips_time": dt_to_tt2000(idpu_time),
                "sips_5v0_current": int.from_bytes(data[6:8], "big"),
                "sips_5v0_voltage": int.from_bytes(data[8:10], "big"),
                "sips_input_current": int.from_bytes(data[10:12], "big"),
                "sips_input_temp": int.from_bytes(data[12:14], "big"),
                "sips_input_voltage": int.from_bytes(data[14:16], "big"),
            }
        if data_type == 15:  # EPD
            return {
                "epd_time": dt_to_tt2000(idpu_time),
                "epd_biasl": int.from_bytes(data[0:2], "big"),
                "epd_biash": int.from_bytes(data[2:4], "big"),
                "epd_efe_temp": int.from_bytes(data[4:6], "big"),
            }
        if data_type == 16:  # FGM
            return {
                "fgm_time": dt_to_tt2000(idpu_time),
                "fgm_8_volt": int.from_bytes(data[0:2], "big"),  # status byte count 0
                "fgm_sh_temp": int.from_bytes(data[6:8], "big"),  # status byte count 3
                "fgm_3_3_volt": int.from_bytes(data[2:4], "big"),  # status byte count 1
                "fgm_analog_ground": int.from_bytes(data[4:6], "big"),  # status byte count 2
                "fgm_eu_temp": int.from_bytes(data[8:10], "big"),  # status byte count 4
            }
        raise ValueError(f"⚠️ \tWanted data type 14, 15, 16; instead got {data_type}")

    def get_fc_df(self, processing_request):
        """
        Returns a DataFrame of FC Data
        Refer to name_converter dictionary for additional information
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
            models.Categorical.timestamp < processing_request.date + dt.timedelta(days=1),
            models.Categorical.name.in_(list(name_converter.keys())),
        )

        fc_df = pd.DataFrame(
            [
                {"fc_time": pycdf.lib.datetime_to_tt2000(row.timestamp), name_converter[row.name]: row.value}
                for row in query
            ]
        )
        return fc_df

    def get_bmon_df(self, processing_request):
        """
        Returns a Dataframe containing Battery Monitor Data
        NOTE: To calculate the values, need to average the two values provided for each time
        """

        query = self.session.query(models.BmonData).filter(
            models.BmonData.mission_id == processing_request.mission_id,
            models.BmonData.timestamp >= processing_request.date,
            models.BmonData.timestamp < processing_request.date + dt.timedelta(days=1),
        )

        # TODO: Check if this works
        fc_temp = {1: {}, 2: {}}
        for row in query:
            row.timestamp = pycdf.lib.datetime_to_tt2000(row.timestamp)
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
        return None

    def get_cdf_fields(self, processing_request):
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

        return {field: field for field in eng_fields}
