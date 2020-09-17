import datetime as dt
import os

import dateutil
import pytest

from run import ArgparsePipelineConfig, ArgparsePipelineQuery, CLIHandler


class TestArgparsePipelineConfig:
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
        assert ArgparsePipelineQuery.get_times(True) == "collection"
        assert ArgparsePipelineQuery.get_times(False) == "downlink"

    def test_validate_time(self):
        start_time_str = "2020-09-08"
        end_time_str = "2020-09-12"
        start_time = dt.datetime(2020, 9, 8)
        end_time = dt.datetime(2020, 9, 12)

        assert ArgparsePipelineQuery.validate_time(start_time_str, end_time_str) == (start_time, end_time)

        with pytest.raises(RuntimeError):
            ArgparsePipelineQuery.validate_time(end_time_str, start_time_str)

        with pytest.raises(dateutil.parser._parser.ParserError):
            ArgparsePipelineQuery.validate_time("BLAH", 5)


# TODO: Rename CLIHandler for consistency
class TestCLIHandler:
    def test_init(self):
        assert CLIHandler() is not None

    # TODO: Fix this
    def test_get_argparser(self):
        argparser = CLIHandler.get_argparser()
        args_1 = argparser.parse_args(["-v", "-w", "-q", "-o", ".", "daily"])
        DICT_1 = {
            "verbose": True,
            "withhold_files": True,
            "quiet": True,
            "output_dir": ".",
            "subcommand": "daily",
            "ela": True,
            "elb": True,
            "em3": False,
            "abandon_calculated_products": False,
            "products": ["epdef", "epdif", "epdes", "epdis", "fgf", "fgs", "eng", "mrma", "mrmi", "state"],
        }

        # TODO: Test times differently, bc daily option causes it to vary
        for key, value in DICT_1.items():
            assert args_1.__dict__[key] == value

        args_2 = argparser.parse_args(["-o", ".", "dump", "--ela", "2020-09-09", "2020-10-10", "--elb"])
        assert args_2.__dict__ == {
            "verbose": False,
            "withhold_files": False,
            "quiet": False,
            "output_dir": ".",
            "subcommand": "dump",
            "ela": True,
            "elb": True,
            "em3": False,
            "abandon_calculated_products": False,
            "select_downlinks_by_collection_time": False,
            "start_time": "2020-09-09",
            "end_time": "2020-10-10",
            "products": ["epdef", "epdif", "epdes", "epdis", "fgf", "fgs", "eng", "mrma", "mrmi", "state"],
        }

        args_3 = argparser.parse_args(
            ["--verbose", "--withhold-files", "--quiet", "-o", ".", "downlinks", "-a", "-c", "2019-1-1", "2019-2-2"]
        )
        # raise ValueError(args_3.__dict__)

        assert args_3.__dict__ == {
            "verbose": True,
            "withhold_files": True,
            "quiet": True,
            "output_dir": ".",
            "subcommand": "downlinks",
            "ela": False,
            "elb": False,
            "em3": False,
            "abandon_calculated_products": True,
            "select_downlinks_by_collection_time": True,
            "start-time": "2019-1-1",
            "end-time": "2019-2-2",
            "products": ["epdef", "epdif", "epdes", "epdis", "fgf", "fgs", "eng", "mrma", "mrmi", "state"],
        }

        args_4 = argparser.parse_args(["-o", "..", "dump", "2020-12-01", "2020-12-2", "-p", "epdef", "epdif"])
        assert args_4.__dict__ == {
            "verbose": False,
            "withhold_files": False,
            "quiet": False,
            "output_dir": "..",
            "subcommand": "dump",
            "ela": False,
            "elb": False,
            "em3": False,
            "abandon_calculated_products": False,
            "select_downlinks_by_collection_time": False,
            "start-time": "2020-12-01",
            "end-time": "2020-12-2",
            "products": ["epdef", "epdif"],
        }
