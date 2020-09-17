"""Class to generate MRM files (both ACB or IDPU)."""
import datetime as dt

import pandas as pd
from elfin.common import models
from spacepy import pycdf

from data_type.completeness_config import MrmCompletenessConfig
from metric.completeness import CompletenessUpdater
from processor.science_processor import ScienceProcessor
from util.constants import MRM_TYPES

# TODO: Processors return sets?


class MrmProcessor(ScienceProcessor):
    """A type of ScienceProcessor which generates files for MRM data."""

    def __init__(self, pipeline_config):
        super().__init__(pipeline_config)

        self.completeness_updater = CompletenessUpdater(pipeline_config.session, MrmCompletenessConfig)

    def generate_files(self, processing_request):
        """Generates a level 1 MRM file, given a processing request."""
        if processing_request.mission_id == 2 and processing_request.data_product == "mrmi":
            self.logger.warning("Skipping IDPU MRM data product generation for ELFIN-B")
            return []

        mrm_df = self.get_mrm_df(processing_request)
        if mrm_df.empty:
            self.logger.info("No matching data found")
            return []

        self.completeness_updater.update_completeness_table(processing_request, mrm_df["timestamp"])

        # Create CDF
        cdf_fname = self.make_filename(processing_request, level=1)
        cdf = self.create_cdf(cdf_fname)

        # Fill CDF
        self.fill_cdf(processing_request, mrm_df, cdf)

        cdf.close()
        return [cdf_fname]

    def get_mrm_df(self, processing_request):
        query = (
            self.session.query(models.MRM)
            .filter(
                models.MRM.timestamp >= processing_request.date,
                models.MRM.timestamp <= processing_request.date + dt.timedelta(days=1),
                models.MRM.mrm_type == MRM_TYPES[processing_request.data_product],
                models.MRM.mission_id == processing_request.mission_id,
            )
            .order_by(models.MRM.timestamp)
        )

        mrm_df = pd.read_sql_query(query.statement, query.session.bind)
        mrm_df = mrm_df.drop_duplicates(subset=["timestamp", "mrm_x", "mrm_y", "mrm_z", "mrm_type", "mission_id"])

        return mrm_df

    def fill_cdf(self, processing_request, df, cdf):
        self.logger.debug("Filling MRM CDF")
        probe = processing_request.probe

        datestr_run = dt.datetime.utcnow().strftime("%04Y-%02m-%02d")
        cdf.attrs["Generation_date"] = datestr_run
        cdf.attrs["MODS"] = f"Rev- {datestr_run}"  # TODO: Check this string

        cdf[f"{probe}_{processing_request.data_product}_time"] = df["timestamp"].apply(pycdf.lib.datetime_to_tt2000)
        cdf[f"{probe}_{processing_request.data_product}"] = df[["mrm_x", "mrm_y", "mrm_z"]].values
