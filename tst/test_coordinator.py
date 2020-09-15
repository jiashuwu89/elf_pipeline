import tempfile

from elfin.common import db

from coordinator import Coordinator
from data_type.pipeline_config import PipelineConfig


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


class TestArgparsePipelineConfig:
    def test_transfer_files(self):
        pipeline_config = DummyPipelineConfig()
        coordinator = Coordinator(pipeline_config)
        assert coordinator.transfer_files(["BLAH", "BLAH", "BLAH"]) == 0
