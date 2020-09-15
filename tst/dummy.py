"""Dummy classes to help with testing"""
import datetime as dt
import tempfile

from elfin.common import db

from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from processor.science_processor import ScienceProcessor


class DummyPipelineConfig(PipelineConfig):
    def __init__(self):
        if db.SESSIONMAKER is None:
            db.connect("production")
        self.session = db.SESSIONMAKER()
        self.calculate = False
        self.update_db = False
        self.generate_files = False
        self.output_dir = tempfile.mkdtemp()
        self.upload = False
        self.email = False


class DummyProcessingRequest(ProcessingRequest):
    def __init__(self):
        super().__init__(1, "epdef", dt.date(2020, 8, 5))


class DummyScienceProcessor(ScienceProcessor):
    def generate_files(self, processing_request):
        raise NotImplementedError
