import os

import pytest

from data_type.pipeline_config import ArgparsePipelineConfig
from run import CLIHandler


class TestArgparsePipelineConfig:
    ARGS = ["-v", "-w", "-q", "-o", ".", "daily"]
    ARGPARSE_PIPELINE_CONFIG = ArgparsePipelineConfig(CLIHandler.get_argparser().parse_args(ARGS))

    def test_session(self):
        assert self.ARGPARSE_PIPELINE_CONFIG.session is not None

    def test_update_db(self):
        assert self.ARGPARSE_PIPELINE_CONFIG.update_db is True
        assert isinstance(self.ARGPARSE_PIPELINE_CONFIG.update_db, bool)

    def test_generate_files(self):
        assert self.ARGPARSE_PIPELINE_CONFIG.update_db is True
        assert isinstance(self.ARGPARSE_PIPELINE_CONFIG.generate_files, bool)

    def test_output_dir(self):
        assert os.path.isdir(self.ARGPARSE_PIPELINE_CONFIG.output_dir)

    def test_state_csv_dir(self):
        assert os.path.isdir(self.ARGPARSE_PIPELINE_CONFIG.output_dir)

    def test_upload(self):
        assert self.ARGPARSE_PIPELINE_CONFIG.update_db is True
        assert isinstance(self.ARGPARSE_PIPELINE_CONFIG.upload, bool)

    def test_email(self):
        assert self.ARGPARSE_PIPELINE_CONFIG.email is False
        assert isinstance(self.ARGPARSE_PIPELINE_CONFIG.email, bool)

    def test_db_update_necessary(self):
        assert ArgparsePipelineConfig.db_update_necessary(True) is False
        assert ArgparsePipelineConfig.db_update_necessary(False) is True

    def test_file_generation_necessary(self):
        assert ArgparsePipelineConfig.file_generation_necessary("daily") is True
        assert ArgparsePipelineConfig.file_generation_necessary("dump") is True
        assert ArgparsePipelineConfig.file_generation_necessary("downlinks") is False

    def test_validate_output_dir(self):
        assert ArgparsePipelineConfig.validate_output_dir(os.getcwd()) == os.getcwd()

        with pytest.raises(ValueError):
            ArgparsePipelineConfig.validate_output_dir("/BAD_DIRECTORY")

    def test_upload_necessary(self):
        assert ArgparsePipelineConfig.upload_necessary(True, True) is False
        assert ArgparsePipelineConfig.upload_necessary(True, False) is False
        assert ArgparsePipelineConfig.upload_necessary(False, True) is True
        assert ArgparsePipelineConfig.upload_necessary(False, False) is False

    def test_email_necessary(self):
        assert ArgparsePipelineConfig.email_necessary(True) is False
        assert ArgparsePipelineConfig.email_necessary(False) is True
