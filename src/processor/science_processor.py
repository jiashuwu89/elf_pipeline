import logging
import os
from abc import ABC, abstractmethod

import numpy as np
from spacepy import pycdf

from request.downlink_manager import DownlinkManager  # TODO: Split Downlink Manager
from util.constants import MASTERCDF_DIR


class ScienceProcessor(ABC):
    """
    Base class used for all data product processing from the database.
    Implements some basic functionalities common to all data products.
    """

    def __init__(self, pipeline_config):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.session = pipeline_config.session
        self.output_dir = pipeline_config.output_dir

        self.downlink_manager = DownlinkManager(pipeline_config)

    @abstractmethod
    def generate_files(self, processing_request):
        raise NotImplementedError

    def make_filename(self, processing_request, level, size=None):
        """Constructs the appropriate filename for a L0/L1/L2 file, and returns the full path (level is int)"""
        formatted_date = processing_request.date.strftime("%Y%m%d")
        fname = f"{processing_request.probe}_l{level}_{processing_request.data_product}_{formatted_date}"
        if level == 0:
            if size is None:
                raise ValueError("No size given for level 0 naming")
            fname += f"_{size}.pkt"
        elif level == 1:
            fname += "_v01.cdf"
        else:
            raise ValueError(f"Invalid Level: {level}")
        return f"{self.output_dir}/{fname}"

    # TODO: Rename to indicate that an EMPTY cdf will be created to be filled in
    def create_cdf(self, fname):
        """
        Gets or creates a CDF with the desired fname. If existing path is
        specified, it would check to see if the correct CDF exists.
        If it does not exist, a new cdf will be created with the master cdf.
            fname - a string that includes the  target file path along with
            the target file name of the desired file.
                The file name is of the data product format used.
        """
        fname_parts = fname.split("/")[-1].split("_")
        probe = fname_parts[0]
        level_str = fname_parts[1]
        idpu_type = fname_parts[2]

        if os.path.isfile(fname):
            os.remove(fname)

        master_cdf = f"{MASTERCDF_DIR}/{probe}_{level_str}_{idpu_type}_00000000_v01.cdf"
        self.logger.debug(f"Creating cdf using mastercdf {master_cdf}")

        return pycdf.CDF(fname, master_cdf)

    def fill_cdf(self, processing_request, df, cdf):
        """Inserts data from df into a CDF file

        Parameters
        ==========
        df
        cdf
        """
        cdf_fields = self.get_cdf_fields(processing_request)
        for key in cdf_fields:
            df_field_name = cdf_fields[key]
            cdf_field_name = f"{processing_request.probe}_{key}"

            if cdf_field_name in cdf.keys() and df_field_name in df.columns:
                data = df[df_field_name].values
                # numpy array with lists need to be converted to a multi-dimensional numpy array of numbers
                if isinstance(data[0], list):
                    data = np.stack(data)

                cdf[cdf_field_name] = data

    def get_cdf_fields(self, processing_request):
        """Get CDF Fields to help populate the CDF, override if necessary"""
        self.logger.debug(f"No CDF fields for {str(processing_request)}")
        return {}
