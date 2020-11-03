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
STATE_PRODUCTS = ["state"]
IDPU_PRODUCTS = EPD_PRODUCTS + FGM_PRODUCTS + ENG_PRODUCTS
ALL_PRODUCTS = EPD_PRODUCTS + FGM_PRODUCTS + ENG_PRODUCTS + MRM_PRODUCTS + STATE_PRODUCTS

# A Dictionary mapping IDPU type to products
PACKET_MAP = {
    1: ["fgs"],  # TODO: Document this based on conversation with Ethan
    2: ["fgs"],
    17: ["fgf"],
    18: ["fgf"],
    3: ["epdef"],
    4: ["epdef"],
    5: ["epdif"],
    6: ["epdif"],
    19: ["epdis"],
    20: ["epdes"],
    14: ["eng"],
    15: ["eng"],
    16: ["eng"],
}

# A Dictionary mapping products to IDPU type
# TODO: Figure this out (why are 1/2 fgs and fgf packets, but so are 17 and 18)
SCIENCE_TYPES = {
    "eng": [14, 15, 16],  # 14: SIPS, 15: EPD, 16: FGM
    "fgf": [17, 18],
    "fgs": [1, 2],
    "epdef": [3, 4],
    "epdes": [20],
    "epdif": [5, 6],
    "epdis": [19],
}

# A Dictionary of MRM Types
MRM_TYPES = {"mrma": "ACB", "mrmi": "IDPU"}
MRM_ENUM_MAP = {
    models.MRM_Type.ACB: "mrma",
    models.MRM_Type.IDPU: "mrmi",
}

# In the science_zone_completeness table, the data product column expects "FGM", "EPD", or "MRM"
COMPLETENESS_TABLE_PRODUCT_MAP = {
    "fgs": "FGM",
    "fgf": "FGM",
    "epdef": "EPDE",
    "epdif": "EPDI",
    "epdes": "EPDE",
    "epdis": "EPDI",
    "mrma": "MRM",
    "mrmi": "MRM",
}

# IDPU types of compressed data, survey data
COMPRESSED_TYPES = [2, 4, 6, 18, 19, 20]
SURVEY_TYPES = [19, 20]

# IDL
IDL_SCRIPT_VERSION = 8

# Directory Paths
STATE_CSV_DIR = "/home/elfin-esn/state_data"
SERVER_BASE_DIR = "/themis/data/elfin"
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

# NUMBERS
MINS_IN_DAY = 60 * 24
BITS_IN_BYTE = 8

# Misc
LOOK_BEHIND_DELTA = dt.timedelta(hours=5)  # Begin search this far behind for packets
ONE_DAY_DELTA = dt.timedelta(days=1)
DAILY_EMAIL_LIST = ["jcking1034@gmail.com"]


# When an attitude is found, reprocess days that are STATE_CALCULATE_RAIDUS before or after the attitude data point
STATE_CALCULATE_RADIUS = dt.timedelta(days=5)
ATTITUDE_SOLUTION_RADIUS = dt.timedelta(days=30)

# EPD
BIN_COUNT = 16
VALID_NUM_SECTORS = [4, 16]
