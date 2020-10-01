import logging
import os
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
from spacepy import pycdf

from util.constants import MASTERCDF_DIR

# TODO: level 0 files on server are MESSY because of COUNTS in fname, so files not OVERWRITTEN


class ScienceProcessor(ABC):
    """Base class used for all data product processing from the database.

    Implements some basic functionalities common to all data products.
    """

    def __init__(self, pipeline_config):
        self.logger = logging.getLogger(self.__class__.__name__)

        # TODO: Just store pipeline_config?
        self.session = pipeline_config.session
        self.output_dir = pipeline_config.output_dir
        self.update_db = pipeline_config.update_db

    @abstractmethod
    def generate_files(self, processing_request):
        """Given a ProcessingRequest, creates all relevant files.

        To be overridden by derived classes.

        Parameters
        ----------
        processing_request : ProcessingRequest

        Returns
        -------
        list
            A list of file names of the generated files
        """
        raise NotImplementedError

    def make_filename(self, processing_request, level: int, size: int = None) -> str:
        """Constructs the appropriate filename for a L0/L1/L2 file.

        Parameters
        ----------
        processing_request : ProcessingRequest
        level : int
            The level of the file, currently either 0 or 1
        size : int, optional
            The size (number of rows) of the associated DataFrame, required
            if level is 0

        Returns
        -------
        str
            The full path and filename associated with the ProcessingRequest
        """
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

    def create_empty_cdf(self, fname: str) -> pycdf.CDF:
        """Creates a CDF with the desired fname, using the correct mastercdf.

        If a corresponding file already exists, it will be removed.

        Parameters
        ----------
        fname : str
            The target path and filename of the file to be created

        Returns
        -------
        pycdf.CDF
            A CDF object associated with the given filename
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

    def fill_cdf(self, processing_request, df: pd.DataFrame, cdf: pycdf.CDF) -> None:
        """Inserts data from df into a CDF file.

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame
            A DataFrame of science data to be used in the CDF
        cdf : pycdf.CDF
            The CDF object to receive science data

        Returns
        -------
        None
            The CDF is modified in-place
        """
        cdf_fields = self.get_cdf_fields(processing_request)
        for cdf_field_name, df_field_name in cdf_fields.items():
            if cdf_field_name in cdf.keys() and df_field_name in df.columns:
                data = df[df_field_name].values
                # numpy array with lists need to be converted to a multi-dimensional numpy array of numbers
                if isinstance(data[0], list):
                    data = np.stack(data)

                cdf[cdf_field_name] = data

    def get_cdf_fields(self, processing_request):
        """Get CDF Fields to help populate the CDF.

        To be overridden if necessary. By default, the method specifies that
        no CDF fields are necessary.

        Parameters
        ----------
        processing_request : ProcessingRequest

        Returns
        -------
        dict
            A dictionary mapping CDF fields to DataFrame column names
        """
        self.logger.debug(f"No CDF fields for {str(processing_request)}")
        return {}
