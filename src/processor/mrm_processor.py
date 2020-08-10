"""Class to generate MRM files (both ACB or IDPU)."""
import datetime as dt

import pandas as pd
from spacepy import pycdf

from common import models
from processor.science_processor import ScienceProcessor


class MrmProcessor(ScienceProcessor):
    """A type of ScienceProcessor which generates files for MRM data."""
    def __init__(self, session, output_dir):
        super().__init__(session, output_dir, "mrm")

    def generate_files(self, processing_request):
        """Generates a level 1 MRM file, given a processing request."""
        if processing_request.mission_id == 2 and processing_request.data_product == "mrmi":
            self.log.warning("Skipping IDPU MRM data product generation for ELFIN-B")
            return []

        if processing_request.data_product == "mrma":
            mrm_type = "ACB"
        elif processing_request.data_product == "mrmi":
            mrm_type = "IDPU"
        else:
            ValueError(f"Bad data product: {processing_request.data_product}")

        cdf_fields_l1 = {processing_request.data_product: "data", processing_request.data_product + "_time": "time"}

        query = (
            self.session.query(models.MRM)
            .filter(
                models.MRM.timestamp >= processing_request.date,
                models.MRM.timestamp <= processing_request.date + dt.timedelta(days=1),
                models.MRM.mrm_type == mrm_type,
                models.MRM.mission_id == processing_request.mission_id,
            )
            .order_by(models.MRM.timestamp)
        )

        mrm_df = pd.read_sql_query(query.statement, query.session.bind)
        mrm_df = mrm_df.drop_duplicates(subset=["timestamp", "mrm_x", "mrm_y", "mrm_z", "mrm_type", "mission_id"])

        if mrm_df.shape[0] == 0:
            self.log.info("No matching data found")
            return []

        # TODO: Completeness

        # Create CDF
        cdf_fname = self.make_filename(level=1, collection_date=processing_request.date)
        cdf = self.create_CDF(cdf_fname)

        # Fill CDF
        datestr_run = dt.datetime.utcnow().strftime("%04Y-%02m-%02d")
        cdf.attrs["Generation_date"] = datestr_run
        cdf.attrs["MODS"] = "Rev- " + datestr_run
        cdf[self.probe_name + "_" + processing_request.data_product + "_time"] = mrm_df["timestamp"].apply(
            pycdf.lib.datetime_to_tt2000
        )
        cdf[self.probe_name + "_" + processing_request.data_product] = mrm_df[["mrm_x", "mrm_y", "mrm_z"]].values
        cdf.close()

        return [cdf_fname]
