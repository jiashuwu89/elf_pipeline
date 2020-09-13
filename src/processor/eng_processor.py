"""Class to generate ENG files

Classes:

    EngProcessor
"""
import datetime as dt
import statistics

import pandas as pd
from spacepy import pycdf

from common import models
from processor.science_processor import ScienceProcessor
from util.constants import SCIENCE_TYPES
from util.science_utils import dt_to_tt2000
from utils.db.downlinks import Downlinks


class EngProcessor(ScienceProcessor):
    """Class to generate ENG files"""

    def __init__(self, pipeline_config):
        super().__init__(self, pipeline_config)

        # TODO: Fix this (see fgm or epd processors)
        self.idpu_types = SCIENCE_TYPES[data_product]

        self.eng_fields = [
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

        self.cdf_fields_l1 = {}
        for field in self.eng_fields:
            self.cdf_fields_l1[field] = field

    def gen_level_0(self, collection_time):
        """ Try to generate level 0, but return empty df if it fails (to keep processing) """
        try:
            _, lv0_orig_df = super().gen_level_0(collection_time)
            return None, lv0_orig_df
        except RuntimeError as e:
            self.logger.warning(e)
            return None, pd.DataFrame()

    def process_level_0(self, df):
        """No processing necessary for ENG level 0"""
        data_bytes = []
        for _, row in df.iterrows():
            if row["data"] is not None:
                data_bytes.append(bytes.fromhex(row["data"]))
            else:
                data_bytes.append(None)
        df["data"] = data_bytes
        return df

    def transform_level_0(self, df, collection_date):
        """
        Creates Dataframe using Inputed Data, as well as FC and Battery Monitor
        Data found using EngDownlinkManager
        """

        orig_df = pd.DataFrame()

        for _, row in df.iterrows():
            to_add = {"idpu_time": row["idpu_time"]}
            to_add.update(self.extract_data(row["idpu_type"], row["data"], row["idpu_time"]))

            orig_df = orig_df.append(to_add, ignore_index=True, sort=False)

        start = collection_date + dt.timedelta(microseconds=0)
        end = collection_date + dt.timedelta(days=1) - dt.timedelta(microseconds=1)
        eng_downlinks_manager = EngDownlinkManager(self.mission_id, start, end)

        final_df = orig_df

        fc_df = eng_downlinks_manager.get_fc()
        if not fc_df.empty:
            final_df = pd.concat([final_df, fc_df], axis=0, ignore_index=True, sort=True)
        else:
            self.logger.debug("No FC Data")

        bmon_df = eng_downlinks_manager.get_bmon_data()
        if not bmon_df.empty:
            final_df = pd.concat([final_df, bmon_df], axis=0, ignore_index=True, sort=True)
        else:
            self.logger.debug("No Battery Monitor Data")

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

    def process_level_1(self, df):
        pass

    def process_level_2(self, df):
        pass


class EngDownlinkManager(Downlinks):
    def __init__(self, mission_id, start, end, session=None):
        super().__init__(session)
        self.mission_id = mission_id
        self.start = start
        self.end = end

    def get_fc(self):
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
            models.Categorical.mission_id == self.mission_id,
            models.Categorical.timestamp >= self.start,
            models.Categorical.timestamp < self.end,
            models.Categorical.name.in_(list(name_converter.keys())),
        )

        final_df = pd.DataFrame(
            [
                {"fc_time": pycdf.lib.datetime_to_tt2000(row.timestamp), name_converter[row.name]: row.value}
                for row in query
            ]
        )
        return final_df

    def get_bmon_data(self):
        """
        Returns a Dataframe containing Battery Monitor Data
        NOTE: To calculate the values, need to average the two values provided for each time
        """

        query = self.session.query(models.BmonData).filter(
            models.BmonData.mission_id == self.mission_id,
            models.BmonData.timestamp >= self.start,
            models.BmonData.timestamp < self.end,
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

        final_df = pd.concat([fc_avionics_temp_1, fc_avionics_temp_2], axis=0, ignore_index=True, sort=True)
        return final_df
