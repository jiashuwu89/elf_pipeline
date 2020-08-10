import datetime as dt
import logging
from abc import ABC

import pandas as pd
from spacepy import pycdf

from util.completeness import CompletenessConfig
from util.constants import MASTERCDF_DIR, MISSION_DICT
from util.science_utils import dt_to_tt2000, s_if_plural


class ScienceProcessor(ABC):
    """
    Base class used for all data product processing from the database.
    Implements some basic functionalities common to all data products.
    """

    def __init__(self, session, output_dir, processor_name):

        self.session = session
        self.output_dir = output_dir

        self.downlink_manager = DownlinkManager(session)
        self.logger = logging.getLogger(f"science.processor.{processor_name}")

    def generate_files(self, processing_request):
        pass


# TODO: make_filename and create_CDF functions
