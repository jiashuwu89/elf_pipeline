"""CLI for running the Science Processing Pipeline

For additional help, run: python run.py --help
"""

import argparse
import datetime as dt
import logging
import os
import tempfile

from dateutil.parser import parse as dateparser
from elfin.common import db

from coordinator import Coordinator
from data_type.pipeline_config import PipelineConfig
from data_type.pipeline_query import PipelineQuery
from util.constants import ALL_MISSIONS, ALL_PRODUCTS, LOOK_BEHIND_DELTA

# Logging Init
LOG_FORMAT: str = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
DATE_FORMAT: str = "%H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)


# TODO: Fix tests (have restructured the CLI)
class ArgparsePipelineConfig(PipelineConfig):
    def __init__(self, args):
        # Initialize DB connection
        if db.SESSIONMAKER is None:
            db.connect("production")
        self.session = db.SESSIONMAKER()

        # Initialize parameters/options from command line
        self.update_db = self.db_update_necessary(args.abandon_calculated_downlinks)
        self.generate_files = self.file_generation_necessary(args.subcommand)
        self.output_dir = self.validate_output_dir(args.output_dir)
        self.upload = self.upload_necessary(args.withhold_files, self.generate_files)
        self.email = self.email_necessary(args.quiet)

    @staticmethod
    def db_update_necessary(abandon_calculated_downlinks):
        """Determines if it is necessary to calculate downlinks."""
        return not abandon_calculated_downlinks

    @staticmethod
    def file_generation_necessary(subcommand):
        return subcommand in ["daily", "dump"]

    @staticmethod
    def validate_output_dir(output_dir):
        if output_dir:
            if not os.path.isdir(output_dir):
                raise ValueError(f"Bad Output Directory: {output_dir}")
        return output_dir

    @staticmethod
    def upload_necessary(withhold_files, generate_files):
        return not withhold_files and generate_files

    @staticmethod
    def email_necessary(quiet):
        return not quiet


class ArgparsePipelineQuery(PipelineQuery):
    def __init__(self, args):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.mission_ids = self.get_mission_ids(args.ela, args.elb, args.em3)
        self.data_products = self.get_data_products(args.products)
        self.times, self.start_time, self.end_time = self.get_times(
            args.downlink_time,
            args.collection_time,
        )

    @staticmethod
    def get_mission_ids(ela, elb, em3):
        """Determine which missions to process, defaulting to ELA and ELB only"""
        mission_ids = []

        if ela:
            mission_ids.append(1)
        if elb:
            mission_ids.append(2)
        if em3:
            mission_ids.append(3)
        if len(mission_ids) == 0:
            mission_ids = ALL_MISSIONS.copy()

        return mission_ids

    @staticmethod
    def get_data_products(products):
        if not products:
            raise ValueError("Products should not be null!")
        return products

    @staticmethod
    def get_times(d, c):
        if d and c:
            raise RuntimeError("Cannot have both downlink time and collection time range")
        elif d:
            times = "downlink"
            start_time = dateparser(d[0], tzinfos=0)
            end_time = dateparser(d[1], tzinfos=0)
        elif c:
            times = "collection"
            start_time = dateparser(c[0], tzinfos=0)
            end_time = dateparser(c[1], tzinfos=0)
        else:
            raise RuntimeError("Need either downlink time or collection time range")
        return (times, start_time, end_time)


class CLIHandler:
    """A class to parse arguments and use them to run the pipeline"""

    def __init__(self):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)  # TODO: Log to file
        self.argparser: argparse.ArgumentParser = self.get_argparser()

    # TODO: ELA, ELB, EM3 Options should be options for subcommand, not top level
    @staticmethod
    def get_argparser():
        """ Get argparser for parsing arguments """
        argparser = argparse.ArgumentParser(description="Process Science Data from ELFIN")

        # General Options
        argparser.add_argument("-v", "--verbose", help="Use DEBUG log level", action="store_true")
        argparser.add_argument(
            "-w", "--withhold-files", help="Avoid uploading L0/L1 files to the server", action="store_true"
        )
        argparser.add_argument("-q", "--quiet", help="If problems occur, don't notify via email", action="store_true")
        argparser.add_argument(
            "-o",
            "--output-dir",
            help="Directory to output generated files (Default: temporary directory)",
            action="store",
            default=tempfile.mkdtemp(),
            metavar="DIR",
        )

        # Require subcommands
        subcommands = argparser.add_subparsers(required=True, dest="subcommand")

        # daily subcommand
        sub_daily = subcommands.add_parser(
            "daily",
            help="Run daily processing",
            description="Process all science packets received over the last day. \
                Functionally equivalent to a 'dump' of all missions and all \
                data products over the last 5 hours, uploading all calculated \
                downlinks",
        )
        end_time = dt.datetime(*dt.datetime.utcnow().timetuple()[:4])
        start_time = end_time - LOOK_BEHIND_DELTA
        sub_daily.set_defaults(
            subcommand="daily",
            ela=True,
            elb=True,
            em3=False,
            abandon_calculated_downlinks=False,
            downlink_time=[start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S")],
            products=ALL_PRODUCTS,
        )

        # dump subcommand
        sub_dump = subcommands.add_parser(
            "dump",
            help="Selectively process science data",
            description="Reprocess and generate a dump of specific science data \
                products.",
        )
        sub_dump.set_defaults(
            subcommand="dump",
            collection_time=None,
            downlink_time=None,
        )
        sub_dump = CLIHandler.add_common_options(sub_dump)
        sub_dump.add_argument(
            "-p",
            "--products",
            help="Process data belonging to PRODUCTS",
            action="store",
            nargs="+",
            choices=ALL_PRODUCTS,
            default=ALL_PRODUCTS,
            metavar="PRODUCTS",
        )

        # downlinks subcommand
        sub_downlinks = subcommands.add_parser(
            "downlinks",
            help="Calculate/List downlink entries",
            description="Scan downlinked science packets and group them into downlink entries",
        )
        sub_downlinks.set_defaults(
            subcommand="downlinks", collection_time=None, downlink_time=None, products=ALL_PRODUCTS
        )
        sub_downlinks = CLIHandler.add_common_options(sub_downlinks)

        return argparser

    @staticmethod
    def add_common_options(sub_parser):
        # Missions
        sub_parser.add_argument("-1", "--ela", help="Process ELA data", action="store_true")
        sub_parser.add_argument("-2", "--elb", help="Process ELB data", action="store_true")
        sub_parser.add_argument("-3", "--em3", help="Process EM3 data", action="store_true")

        sub_parser.add_argument(
            "-a",
            "--abandon-calculated-downlinks",
            help="Avoid updating the science_downlink table with downlinks that were calculated during execution",
            action="store_true",
        )
        sub_parser_time_group = sub_parser.add_mutually_exclusive_group(required=True)
        sub_parser_time_group.add_argument(
            "-c",
            "--collection-time",
            help="generate downlink entries in the range [START, END) using collection time",
            action="store",
            nargs=2,
            metavar=("START", "END"),
        )
        sub_parser_time_group.add_argument(
            "-d",
            "--downlink-time",
            help="generate downlink entries in the range [START, END)",
            action="store",
            nargs=2,
            metavar=("START", "END"),
        )

        return sub_parser

    def run(self):
        """ Get arguments and perform processing, noting the duration """
        start_time: dt.datetime = dt.datetime.utcnow()
        self.logger.info(f"🤠  Beginning at {start_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\t🤠")

        args: argparse.ArgumentParser = self.argparser.parse_args()

        if args.verbose:
            # https://stackoverflow.com/questions/12158048/changing-loggings-basicconfig-which-is-already-set

            # Remove all handlers associated with the root logger object.
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)

            # Reconfigure logging again, this time with a file.
            logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)
            self.logger.debug("Logging level set to 'DEBUG'")

        pipeline_config = ArgparsePipelineConfig(args)
        coordinator: Coordinator = Coordinator(pipeline_config)
        pipeline_query = ArgparsePipelineQuery(args)
        coordinator.execute_pipeline(pipeline_query)

        elapsed_time: dt.timedelta = dt.datetime.utcnow() - start_time
        self.logger.info(f"🤠\tTotal run time: {str(elapsed_time)}\t🤠")


if __name__ == "__main__":
    cli_handler = CLIHandler()
    cli_handler.run()
