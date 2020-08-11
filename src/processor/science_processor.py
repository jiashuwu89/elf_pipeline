import datetime as dt
import logging
import os
from abc import ABC, abstractmethod

import pandas as pd
from spacepy import pycdf

from db.downlink import DownlinkManager
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

    @abstractmethod
    def generate_files(self, processing_request):
        pass

    def make_filename(self, processing_request, level, size=None):
        """Constructs the appropriate filename for a L0/L1/L2 file, and returns the full path (level is int)"""

        fname = f"{processing_request.get_probe()}_l{level}_{processing_request.data_product}_{processing_request.datestrftime('%Y%m%d')}"
        if level == 0:
            if size is None:
                raise ValueError("No size given for level 0 naming")
            fname += f"_{str(size)}.pkt"
        elif level == 1:
            fname += "_v01.cdf"
        else:
            raise ValueError("INVALID LEVEL")
        return f"{self.output_dir}/{fname}"

    def create_cdf(self, fname):
        """
        Gets or creates a CDF with the desired fname. If existing path is specified, it would check to see if the correct CDF exists.
        If it does not exist, a new cdf will be created with the master cdf.
            fname - a string that includes the  target file path along with the target file name of the desired file.
                The file name is of the data product format used.
        """
        fname_parts = fname.split("/")[-1].split("_")
        probe = fname_parts[0]
        level_str = fname_parts[1]
        idpu_type = fname_parts[2]

        if os.path.isfile(fname):
            os.remove(fname)

        master_cdf = f"/home/elfin-esn/OPS/science/trunk/science_processing/mastercdf/{probe}_{level_str}_{idpu_type}_00000000_v01.cdf"

        return pycdf.CDF(fname, master_cdf)
