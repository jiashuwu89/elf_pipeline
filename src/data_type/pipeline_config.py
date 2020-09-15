from abc import ABC


class PipelineConfig(ABC):
    """A class to hold configurations of the pipeline.

    Attributes
    ----------
    session
        From elfin.common.db
    calculate: bool
        Specifies if downlinks should be calculated
    update_db: bool
        Specifies if the science_downlinks table should be updated
    generate_files: bool
        Specifies if files should be generated
    output_dir: str
        The directory where generated files should be stored
    upload: bool
        Specifies if files should be uploaded to the server
    email: bool
        Specifies if warnings should be emailed, if problems occur
    """
