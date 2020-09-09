"""constants.py: Commonly used constants throughout science processing.

This file holds some information about the mission, specifically information
that helps with science processing (ex. data products). These were originally
spread throughout the various files but it seems convenient to store them in
a common location.

Contents:
- Items related to Data Products

"""
import datetime as dt

MISSION_DICT = {1: "ela", 2: "elb", 3: "em3"}
ALL_MISSIONS = [1, 2]

MASTERCDF_DIR = "/home/elfin-esn/OPS/science/trunk/science_processing/mastercdf"

# Lists of different groups of data products
EPD_PRODUCTS = ["epdef", "epdif", "epdes", "epdis"]
FGM_PRODUCTS = ["fgf", "fgs"]
MRM_PRODUCTS = ["mrma", "mrmi"]
ENG_PRODUCTS = ["eng"]
STATE_PRODUCTS = ["state"]

# A Dictionary of MRM Types
MRM_TYPES = {"mrma": "ACB", "mrmi": "IDPU"}

ALL_PRODUCTS = EPD_PRODUCTS + FGM_PRODUCTS + ENG_PRODUCTS + MRM_PRODUCTS + STATE_PRODUCTS


# A Dictionary holding Data Products by idpu_type
# 4 and 6 (EPD) as well as 2 and 18 (FGM) are compressed
PACKET_MAP = {
    # FGM
    1: ["fgs", "fgf"],
    2: ["fgs", "fgf"],
    17: ["fgs", "fgf"],
    18: ["fgs", "fgf"],
    # EPD
    3: ["epdef"],
    4: ["epdef"],
    5: ["epdif"],
    6: ["epdif"],
    19: ["epdis"],
    20: ["epdes"],
    # ENG
    14: ["eng"],
    15: ["eng"],
    16: ["eng"],
}

# A Dictionary holding idpu_type by Data Product
SCIENCE_TYPES = {
    # FGM
    "fgs": [1, 2],
    "fgf": [17, 18],
    # EPD
    "epdef": [3, 4],  # 4 and 6 are compressed
    "epdif": [5, 6],
    "epdes": [20],  # Survey Mode is always compressed
    "epdis": [19],  # IDPU types are flipped to make things more confusing
    # ENG
    "eng": [14, 15, 16],  # 14: SIPS, 15: EPD, 16: FGM
}

# IDPU types of compressed data
COMPRESSED_TYPES = [2, 4, 6, 18, 19, 20]


IDL_SCRIPT_VERSION = 8

LOOK_BEHIND_DELTA = dt.timedelta(hours=5)  # Begin search this far behind for packets

DAILY_EMAIL_LIST = ["jcking1034@gmail.com"]

STATE_CSV_DIR = "/home/elfin-esn/state_data/"
