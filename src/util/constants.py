"""Commonly used constants throughout science processing.

This file holds some information about the mission, specifically information
that helps with science processing (ex. data products). These were originally
spread throughout the various files but it seems convenient to store them in
a common location.

The contents include data structures storing information concerning:
    * missions
    * data products
    * directory paths
    * other constants
"""
import datetime as dt
import os

from elfin.common import models

# TODO: Add date and datetime format strings!!

# Missions
MISSION_DICT = {1: "ela", 2: "elb", 3: "em3"}
MISSION_NAME_TO_ID_MAP = {"ela": 1, "elb": 2, "em3": 3}
ALL_MISSIONS = [1, 2]

# Data Products
ENG_PRODUCTS = ["eng"]
EPD_PRODUCTS = ["epdef", "epdif", "epdes", "epdis"]
FGM_PRODUCTS = ["fgf", "fgs"]
MRM_PRODUCTS = ["mrma", "mrmi"]
STATE_PRODUCTS = ["state-defn", "state-pred"]
IDPU_PRODUCTS = EPD_PRODUCTS + FGM_PRODUCTS + ENG_PRODUCTS
ALL_PRODUCTS = EPD_PRODUCTS + FGM_PRODUCTS + ENG_PRODUCTS + MRM_PRODUCTS + STATE_PRODUCTS

# A Dictionary mapping IDPU type to products
PACKET_MAP = {
    1: ["fgs", "fgf"],  # TODO: Document this based on conversation with Ethan
    2: ["fgs", "fgf"],
    17: ["fgf", "fgf"],
    18: ["fgf", "fgf"],
    3: ["epdef"],
    4: ["epdef"],
    22: ["epdef"],
    7: ["epdef"],  # uncompressed 32-sector EPDEF
    8: ["epdef"],  # compressed 32-sector EPDEF
    5: ["epdif"],
    6: ["epdif"],
    23: ["epdif"],
    19: ["epdis"],
    20: ["epdes"],
    14: ["eng"],
    15: ["eng"],
    16: ["eng"],
    24: ["epdef", "epdif"],
}

# A Dictionary mapping products to IDPU type
# TODO: Figure this out (why are 1/2 fgs and fgf packets, but so are 17 and 18)
SCIENCE_TYPES = {
    "eng": [14, 15, 16],  # 14: SIPS, 15: EPD, 16: FGM
    "fgf": [1, 2, 17, 18],
    "fgs": [1, 2, 17, 18],
    "epdef": [3, 4, 7, 8, 22, 24],
    "epdes": [20],
    "epdif": [5, 6, 23, 24],
    "epdis": [19],
}

# A Dictionary of MRM Types
MRM_TYPES = {"mrma": "ACB", "mrmi": "IDPU"}
MRM_ENUM_MAP = {
    models.MRM_Type.ACB: "mrma",
    models.MRM_Type.IDPU: "mrmi",
}

# Dictionaries relating to completeness
FGM_COMPLETENESS_TABLE_PRODUCT_MAP = {1: "FGM", 2: "FGM", 17: "FGM", 18: "FGM"}

EPDE_COMPLETENESS_TABLE_PRODUCT_MAP = {
    3: "EPDE",
    4: "EPDE",
    7: "EPDE_32",
    8: "EPDE_32",
    20: "EPDE",  # TODO: Separate label for survey mode?
    22: "IEPDE",
    24: "IEPDE",
}

EPDI_COMPLETENESS_TABLE_PRODUCT_MAP = {
    5: "EPDI",
    6: "EPDI",
    19: "EPDI",  # TODO: Separate label for survey mode?
    23: "IEPDI",
    24: "IEPDI",
}

MRM_COMPLETENESS_TABLE_PRODUCT_MAP = {-1: "MRM"}

COMPLETENESS_TABLE_PRODUCT_MAP = {
    "fgs": FGM_COMPLETENESS_TABLE_PRODUCT_MAP,
    "fgf": FGM_COMPLETENESS_TABLE_PRODUCT_MAP,
    "epdef": EPDE_COMPLETENESS_TABLE_PRODUCT_MAP,
    "epdes": EPDE_COMPLETENESS_TABLE_PRODUCT_MAP,
    "epdif": EPDI_COMPLETENESS_TABLE_PRODUCT_MAP,
    "epdis": EPDI_COMPLETENESS_TABLE_PRODUCT_MAP,
    "mrma": MRM_COMPLETENESS_TABLE_PRODUCT_MAP,
    "mrmi": MRM_COMPLETENESS_TABLE_PRODUCT_MAP,
}

GAP_CATEGORIZATION_DATA_TYPES = ["EPDE", "EPDE_32", "EPDI", "IEPDE", "IEPDI"]

# Science zone completeness gap boundaries and science zone sections
SMALL_LARGE_GAP_MULTIPLIERS = (1.5, 8.0)
SCIENCE_ZONE_SECTIONS = [1 / 3, 2 / 3]

# IDPU types of compressed data, survey data, IBO data
COMPRESSED_TYPES = [2, 4, 6, 8, 18, 19, 20, 24]
SURVEY_TYPES = [19, 20]
IBO_TYPES = [22, 23, 24]

# IDL
IDL_SCRIPT_VERSION = 8

# Directory Paths
STATE_DEFN_CSV_DIR = "/home/elfin/state_data"
STATE_PRED_CSV_DIR = "/home/elfin/state_data"

SERVER_BASE_DIR = "/nfs/elfin-data"
CREDENTIALS_FILE = "src/util/credentials.py"
if os.path.basename(os.getcwd()) in ["pipeline-refactor", "refactor"]:  # TODO: fix this
    MASTERCDF_DIR = "mastercdf"
    EPD_CALIBRATION_DIR = "calibration"
    TEST_DATA_DIR = "tst/test_data"
elif os.path.basename(os.getcwd()) in ["src", "doc"]:
    MASTERCDF_DIR = "../mastercdf"
    EPD_CALIBRATION_DIR = "../calibration"
    TEST_DATA_DIR = "../tst/test_data"
else:
    raise RuntimeError(f"Cannot run from this directory: {os.getcwd()}")

# Paths for CDFs being transferred to Themis server
DATA_PRODUCT_PATHS = {
    "eng": ["eng", "eng"],
    "epde": ["epd", "epd"],
    "epdi": ["epd", "epd"],
    "epdef": ["epd", "epd/fast/electron"],
    "epdes": ["epd", "epd/survey/electron"],
    "epdif": ["epd", "epd/fast/ion"],
    "epdis": ["epd", "epd/survey/ion"],
    "fgs": ["fgm", "fgm/survey"],
    "fgf": ["fgm", "fgm/fast"],
    "fgm": ["fgm", "fgm"],
    "mrma": ["mrma", "mrma"],
    "mrmi": ["mrmi", "mrmi"],
    "state_defn": ["state/defn", "state/defn"],
    "state_pred": ["state/pred", "state/pred"],
}

# NUMBERS
SECONDS_IN_MINUTE = 60
MINS_IN_DAY = 60 * 24
BITS_IN_BYTE = 8

# Misc
LOOK_BEHIND_DELTA = dt.timedelta(hours=5)  # Begin search this far behind for packets
ONE_DAY_DELTA = dt.timedelta(days=1)
MAX_CDF_VALUE_DELTA = 1e-13  # Maximum allowable difference between values in CDFs
DAILY_EMAIL_LIST = ["jcking1034@gmail.com", "derekclee232@gmail.com", "elfin-notifications@epss.ucla.edu"]

# When an attitude is found, reprocess days that are STATE_CALCULATE_RADIUS before or after the attitude data point
STATE_CALCULATE_RADIUS = dt.timedelta(days=5)
ATTITUDE_SOLUTION_RADIUS = dt.timedelta(days=30)

# EPD
BIN_COUNT = 16
VALID_NUM_SECTORS = [4, 16, 32]
IBO_DATA_BYTE = 10

# Instruments run at 80 Hz and report data in terms of 1/80 second.
INSTRUMENT_CLK_FACTOR = 80

# ENG
CATEGORICALS_TO_CDF_NAME_MAP = {
    models.Categoricals.TMP_1: "fc_idpu_temp",
    models.Categoricals.TMP_2: "fc_batt_temp_1",
    models.Categoricals.TMP_3: "fc_batt_temp_2",
    models.Categoricals.TMP_4: "fc_batt_temp_3",
    models.Categoricals.TMP_5: "fc_batt_temp_4",
    models.Categoricals.TMP_6: "fc_chassis_temp",
    # models.Categoricals.TMP_7: SHOULD BE HELIUM RADIO BUT NOT USED
    models.Categoricals.SP_TMP_1: "acb_solarpanel_temp_1",
    models.Categoricals.SP_TMP_2: "acb_solarpanel_temp_2",
    models.Categoricals.SP_TMP_3: "acb_solarpanel_temp_3",
    models.Categoricals.SP_TMP_4: "acb_solarpanel_temp_4",
}

# Date Ranges
BOGUS_EPD_DATERANGE = (dt.date(2021, 2, 26), dt.date(2021, 5, 9))
MISSION_START_DATE = dt.date(2018, 9, 15)
