from run import CLIHandler


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
            "products": [
                "epdef",
                "epdif",
                "epdes",
                "epdis",
                "fgf",
                "fgs",
                "eng",
                "mrma",
                "mrmi",
                "state-defn",
                "state-pred",
            ],
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
            "products": [
                "epdef",
                "epdif",
                "epdes",
                "epdis",
                "fgf",
                "fgs",
                "eng",
                "mrma",
                "mrmi",
                "state-defn",
                "state-pred",
            ],
        }

        args_3 = argparser.parse_args(
            ["--verbose", "--withhold-files", "--quiet", "-a", "-o", ".", "downlinks", "-c", "2019-1-1", "2019-2-2"]
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
            "start_time": "2019-1-1",
            "end_time": "2019-2-2",
            "products": [
                "epdef",
                "epdif",
                "epdes",
                "epdis",
                "fgf",
                "fgs",
                "eng",
                "mrma",
                "mrmi",
                "state-defn",
                "state-pred",
            ],
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
            "start_time": "2020-12-01",
            "end_time": "2020-12-2",
            "products": ["epdef", "epdif"],
        }
