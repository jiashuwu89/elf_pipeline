"""Class to generate MRM files (both ACB or IDPU)."""
import datetime as dt
from typing import Dict, List, Type

import pandas as pd
from elfin.common import models
from spacepy import pycdf

from data_type.completeness_config import MrmCompletenessConfig
from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from output.metric.completeness import CompletenessUpdater
from processor.science_processor import ScienceProcessor
from util.constants import MRM_TYPES, ONE_DAY_DELTA

# TODO: Processors return sets?


class MrmProcessor(ScienceProcessor):
    """A type of ScienceProcessor which generates files for MRM data.

    Parameters
    ----------
    pipeline_config : Type[PipelineConfig]
    """

    def __init__(self, pipeline_config: Type[PipelineConfig]):
        super().__init__(pipeline_config)

        self.completeness_updater = CompletenessUpdater(pipeline_config.session, MrmCompletenessConfig)

    def generate_files(self, processing_request: ProcessingRequest) -> List[str]:
        """Generates a level 1 MRM file, given a processing request.

        Parameters
        ----------
        processing_request : ProcessingRequest

        Returns
        -------
        List[str]
            A list which is empty if no files can be generated, or containing
            a single string representing a level 1 mrm file
        """
        if processing_request.mission_id == 2 and processing_request.data_product == "mrmi":
            self.logger.warning("Skipping IDPU MRM data product generation for ELFIN-B")
            return []

        mrm_df = self.get_mrm_df(processing_request)
        if mrm_df.empty:
            self.logger.info("No matching data found")
            return []

        mrm_df_copy = mrm_df.copy()
        mrm_df_copy["times"] = mrm_df_copy["timestamp"]
        mrm_df_copy["idpu_type"] = -1
        self.completeness_updater.update_completeness_table(processing_request, mrm_df_copy, self.update_db)

        cdf_fname = self.make_filename(processing_request, level=1)

        cdf = self.create_empty_cdf(cdf_fname)
        self.fill_cdf(processing_request, mrm_df, cdf)
        cdf.close()

        return [cdf_fname]

    def get_mrm_df(self, processing_request: ProcessingRequest) -> pd.DataFrame:
        """Creates a DataFrame of relevant MRM data.

        Parameters
        ----------
        processing_request : ProcessingRequest

        Returns
        -------
        mrm_df : pd.DataFrame
            A DataFrame with the columns 'timestamp' and 'timestamp_tt2000' to
            represent times, and 'mrm' to contain lists of x, y, and z values
            for mrm data
        """
        query = (
            self.session.query(models.MRM)
            .filter(
                models.MRM.timestamp >= processing_request.date,
                models.MRM.timestamp < processing_request.date + ONE_DAY_DELTA,
                models.MRM.mrm_type == MRM_TYPES[processing_request.data_product],
                models.MRM.mission_id == processing_request.mission_id,
            )
            .order_by(models.MRM.timestamp)
        )

        mrm_df = pd.read_sql_query(query.statement, query.session.bind)
        mrm_df = mrm_df.drop_duplicates(subset=["timestamp", "mrm_x", "mrm_y", "mrm_z", "mrm_type", "mission_id"])
        mrm_df["timestamp_tt2000"] = mrm_df["timestamp"].apply(pycdf.lib.datetime_to_tt2000)
        mrm_df["mrm"] = mrm_df[["mrm_x", "mrm_y", "mrm_z"]].values.tolist()
        mrm_df = mrm_df[["timestamp", "timestamp_tt2000", "mrm"]]

        return mrm_df

    def fill_cdf(self, processing_request: ProcessingRequest, df: pd.DataFrame, cdf: pycdf.CDF) -> None:
        """Fills a given CDF with relevant MRM data.

        This overrides the default fill_cdf method in order to insert data
        about generation date and "MODS"

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame
            A Pandas DataFrame with MRM data, in the format of the DataFrames
            obtained from the get_mrm_df method.
        cdf : pycdf.CDF
            A CDF object to which MRM data will be inserted.
        """
        super().fill_cdf(processing_request, df, cdf)

        datestr_run = dt.datetime.utcnow().strftime("%04Y-%02m-%02d")
        cdf.attrs["Generation_date"] = datestr_run
        cdf.attrs["MODS"] = f"Rev- {datestr_run}"

    def get_cdf_fields(self, processing_request: ProcessingRequest) -> Dict[str, str]:
        """Provides a mapping of CDF fields to MRM DataFrame fields.

        Parameters
        ----------
        processing_request : ProcessingRequest

        Returns
        -------
        Dict[str, str]
            A dictionary mapping CDF field names to MRM DataFrame names
        """
        probe = processing_request.probe
        data_product = processing_request.data_product
        return {f"{probe}_{data_product}_time": "timestamp_tt2000", f"{probe}_{data_product}": "mrm"}
