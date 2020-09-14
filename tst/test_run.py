import pytest
from src.run import ArgparsePipelineConfig, ArgparsePipelineQuery


class TestArgparsePipelineConfig:
    def test_get_times(self):
        assert ArgparsePipelineConfig.get_times("run_daily", ["BLAH", "BLAH"], None) == "downlink"
        assert ArgparsePipelineConfig.get_times("run_daily", None, ["BLAH", "BLAH"]) == "downlink"
        assert ArgparsePipelineConfig.get_times("run_dump", ["BLAH", "BLAH"], None) == "downlink"
        assert ArgparsePipelineConfig.get_times("run_dump", None, ["BLAH", "BLAH"]) == "collection"
        assert ArgparsePipelineConfig.get_times("run_downlinks", ["BLAH", "BLAH"], None) == "downlink"
        assert ArgparsePipelineConfig.get_times("run_downlinks", None, ["BLAH", "BLAH"]) == "collection"

    def test_downlink_calculation_necessary(self):
        assert ArgparsePipelineConfig.downlink_calculation_necessary("downlink", "yes") is True
        assert ArgparsePipelineConfig.downlink_calculation_necessary("downlink", "no") is True
        assert ArgparsePipelineConfig.downlink_calculation_necessary("downlink", "nodb") is True
        assert ArgparsePipelineConfig.downlink_calculation_necessary("collection", "yes") is True
        assert ArgparsePipelineConfig.downlink_calculation_necessary("collection", "no") is False
        assert ArgparsePipelineConfig.downlink_calculation_necessary("collection", "nodb") is True

    def test_downlink_upload_necessary(self):
        assert ArgparsePipelineConfig.downlink_upload_necessary("run_daily", "yes") is True
        assert ArgparsePipelineConfig.downlink_upload_necessary("run_daily", "no") is True
        assert ArgparsePipelineConfig.downlink_upload_necessary("run_daily", "nodb") is True
        assert ArgparsePipelineConfig.downlink_upload_necessary("run_dump", "yes") is True
        assert ArgparsePipelineConfig.downlink_upload_necessary("run_dump", "no") is False
        assert ArgparsePipelineConfig.downlink_upload_necessary("run_dump", "nodb") is False
        assert ArgparsePipelineConfig.downlink_upload_necessary("run_downlinks", "yes") is True
        assert ArgparsePipelineConfig.downlink_upload_necessary("run_downlinks", "no") is False
        assert ArgparsePipelineConfig.downlink_upload_necessary("run_downlinks", "nodb") is False

    def test_file_generation_necessary(self):
        assert ArgparsePipelineConfig.file_generation_necessary("run_daily") is True
        assert ArgparsePipelineConfig.file_generation_necessary("run_dump") is True
        assert ArgparsePipelineConfig.file_generation_necessary("run_downlinks") is False

    def test_get_output_dir(self):
        assert ArgparsePipelineConfig.get_output_dir("/") == "/"
        assert isinstance(ArgparsePipelineConfig.get_output_dir(None), str)

        with pytest.raises(ValueError):
            ArgparsePipelineConfig.get_output_dir("/BAD_DIRECTORY")

    def test_upload_necessary(self):
        assert ArgparsePipelineConfig.upload_necessary(True, True) is False
        assert ArgparsePipelineConfig.upload_necessary(True, False) is False
        assert ArgparsePipelineConfig.upload_necessary(False, True) is True
        assert ArgparsePipelineConfig.upload_necessary(False, False) is False

    def test_email_necessary(self):
        assert ArgparsePipelineConfig.email_necessary(True) is False
        assert ArgparsePipelineConfig.email_necessary(True) is True


class TestArgparsePipelineQuery:
    def test_get_mission_ids(self):
        assert ArgparsePipelineQuery.get_mission_ids(True, True, True) == [1, 2, 3]
        assert ArgparsePipelineQuery.get_mission_ids(True, True, False) == [1, 2]
        assert ArgparsePipelineQuery.get_mission_ids(True, False, True) == [1, 3]
        assert ArgparsePipelineQuery.get_mission_ids(True, False, False) == [1]
        assert ArgparsePipelineQuery.get_mission_ids(False, True, True) == [2, 3]
        assert ArgparsePipelineQuery.get_mission_ids(False, True, False) == [2]
        assert ArgparsePipelineQuery.get_mission_ids(False, False, True) == [3]
        assert ArgparsePipelineQuery.get_mission_ids(False, False, False) == [1, 2]

    def test_get_data_products(self):
        assert ArgparsePipelineQuery.get_data_products(["fgf", "fgs"]) == ["fgf", "fgs"]

        with pytest.raises(ValueError):
            ArgparsePipelineQuery.get_data_products([])

    def test_get_times(self):
        pass
        # assert ArgparsePipelineQuery.get_times("run_daily", ["BLAH", "BLAH"], None) == ("downlink", "BLAH"
        # assert ArgparsePipelineQuery.get_times("run_daily", None, ["BLAH", "BLAH"]) == "downlink"
        # assert ArgparsePipelineQuery.get_times("run_dump", ["BLAH", "BLAH"], None) == "downlink"
        # assert ArgparsePipelineQuery.get_times("run_dump", None, ["BLAH", "BLAH"]) == "collection"
        # assert ArgparsePipelineQuery.get_times("run_downlinks", ["BLAH", "BLAH"], None) == "downlink"
        # assert ArgparsePipelineQuery.get_times("run_downlinks", None, ["BLAH", "BLAH"]) == "collection"
