"""Dummy classes to help with testing"""
import datetime as dt
import tempfile

from elfin.common import db

from data_type.pipeline_config import PipelineConfig
from data_type.processing_request import ProcessingRequest
from output.downlink.downlink_manager import DownlinkManager
from processor.science_processor import ScienceProcessor
from util.constants import TEST_DATA_DIR


class DummyPipelineConfig(PipelineConfig):
    def __init__(self):
        if db.SESSIONMAKER is None:
            db.connect("production")
        self._session = db.SESSIONMAKER()
        self._output_dir = tempfile.mkdtemp()

    @property
    def session(self):
        return self._session

    @property
    def update_db(self):
        return False

    @property
    def generate_files(self):
        return False

    @property
    def output_dir(self):
        return self._output_dir

    @property
    def state_defn_csv_dir(self):
        return f"{TEST_DATA_DIR}/csv"

    @property
    def state_pred_csv_dir(self):
        return f"{TEST_DATA_DIR}/csv"

    @property
    def upload(self):
        return False

    @property
    def email(self):
        return False


class DummyProcessingRequest(ProcessingRequest):
    def __init__(self):
        super().__init__(1, "epdef", dt.date(2020, 8, 5))


class DummyScienceProcessor(ScienceProcessor):
    def generate_files(self, processing_request):
        raise NotImplementedError


class SafeTestPipelineConfig(PipelineConfig):
    def __init__(self):
        if db.SESSIONMAKER is None:
            db.connect("production")
        self._session = db.SESSIONMAKER()
        self._output_dir = tempfile.mkdtemp()

    @property
    def session(self):
        return self._session

    @property
    def update_db(self):
        return False

    @property
    def generate_files(self):
        return True

    @property
    def output_dir(self):
        return self._output_dir

    @property
    def state_defn_csv_dir(self):
        return f"{TEST_DATA_DIR}/csv"

    @property
    def state_pred_csv_dir(self):
        return f"{TEST_DATA_DIR}/csv"

    @property
    def upload(self):
        return False

    @property
    def email(self):
        return False


DUMMY_DOWNLINK_MANAGER = DownlinkManager(DummyPipelineConfig())
