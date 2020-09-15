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
DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)


class ArgparsePipelineConfig(PipelineConfig):
    def __init__(self, args):
        # Initialize DB connection
        if db.SESSIONMAKER is None:
            db.connect("production")
        self.session = db.SESSIONMAKER()

        # Initialize parameters/options from command line
        times = self.get_times(args.func, args.d, args.c)
        self.calculate = self.downlink_calculation_necessary(times, args.calculate)
        self.update_db = self.downlink_upload_necessary(args.func, args.calculate)
        self.generate_files = self.file_generation_necessary(args.func)
        self.output_dir = self.get_output_dir(args.output_dir)
        self.upload = self.upload_necessary(args.no_upload, args.generate_files)
        self.email = self.email_necessary(args.no_email)

    @staticmethod
    def get_times(func, d, c):
        if func == "run_daily" or d:
            return "downlink"
        if c:
            return "collection"
        raise ValueError("Couldn't determine value for times")

    @staticmethod
    def downlink_calculation_necessary(times, calculate):
        return times == "downlink"  # TODO: This probably isn't needed: or calculate in ["yes", "nodb"]

    @staticmethod
    def downlink_upload_necessary(func, calculate):
        return func == "run_daily" or calculate == "yes"

    @staticmethod
    def file_generation_necessary(func):
        return func in ["run_daily", "run_dump"]

    @staticmethod
    def get_output_dir(output_dir):
        if output_dir:
            if not os.path.isdir(output_dir):
                raise ValueError(f"Bad Output Directory: {output_dir}")
            return output_dir
        return tempfile.mkdtemp()

    @staticmethod
    def upload_necessary(no_upload, generate_files):
        return not no_upload and generate_files

    @staticmethod
    def email_necessary(no_email):
        return not no_email


class ArgparsePipelineQuery(PipelineQuery):
    def __init__(self, args):
        self.logger = logging.getLogger(self.__class__.__name__)

        self.mission_ids = self.get_mission_ids(args.ela, args.elb, args.em3)
        self.data_products = self.get_data_products(args.products)
        self.times, self.start_time, self.end_time = self.get_times(args.func, args.d, args.c)

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
            raise ValueError("No products specified")
        return products

    @staticmethod
    def get_times(func, d, c):
        if func == "run_daily":
            times = "downlink"
            end_time = dt.datetime(*dt.datetime.utcnow().timetuple()[:4])
            start_time = end_time - LOOK_BEHIND_DELTA
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

    def get_argparser(self):
        """ Get argparser for parsing arguments """
        self.logger.debug("Creating argparser")

        argparser = argparse.ArgumentParser(description="Process Science Data from ELFIN")
        subcommands = argparser.add_subparsers(required=True, dest="subcommand")

        # daily subcommand
        sub_daily = subcommands.add_parser(
            "daily", help="run daily processing", description="Process all science packets received over the last day"
        )
        sub_daily.set_defaults(func="run_daily")

        # dump subcommand
        sub_dump = subcommands.add_parser(
            "dump",
            help="selectively process science data",
            description="Reprocess and generate a dump of specific science data \
                products. If collection time is specified, the downlinks table \
                will be ignored. Otherwise, the table will be updated unless \
                --no-db is used.",
        )
        sub_dump.set_defaults(func="run_dump")
        sub_dump_time_group = sub_dump.add_mutually_exclusive_group(required=True)
        sub_dump_time_group.add_argument(
            "-c",
            "--collection-time",
            help="process data collected in the range [START, END)",
            action="store",
            nargs=2,
            metavar=("START", "END"),
        )
        sub_dump_time_group.add_argument(
            "-d",
            "--downlink-time",
            help="process data downlinked in the range [START, END)",
            action="store",
            nargs=2,
            metavar=("START", "END"),
        )
        sub_dump.add_argument(
            "-p",
            "--products",
            help="process data belonging to PRODUCTS",
            action="store",
            nargs="+",
            choices=ALL_PRODUCTS,
            default=ALL_PRODUCTS,
            metavar="PRODUCTS",
        )

        # downlinks subcommand
        sub_downlinks = subcommands.add_parser(
            "downlinks",
            help="calculate/list downlink entries",
            description="Scan downlinked science packets and group them into downlink entries",
        )
        sub_downlinks.set_defaults(func="run_downlinks")
        sub_downlinks.add_argument(
            "-c",
            "--collection-time",
            help="generate downlink entries in the range [START, END) using collection time",
            action="store",
            nargs=2,
            metavar=("START", "END"),
        )
        sub_downlinks.add_argument(
            "-d",
            "--downlink-time",
            help="generate downlink entries in the range [START, END)",
            action="store",
            nargs=2,
            metavar=("START", "END"),
        )

        # Options
        argparser.add_argument("--ela", help="Process ELA data", action="store_true")
        argparser.add_argument("--elb", help="Process ELB data", action="store_true")
        argparser.add_argument("--em3", help="Process EM3 data", action="store_true")
        argparser.add_argument(
            "--calculate",
            help="mode for calculating downlink entries (from science_packets \
                table). If set to nodb, will be calculated but not uploaded. \
                Choices: (yes/no/nodb), Default: yes",
            action="store",
            choices=["yes", "no", "nodb"],
            default="yes",
        )

        argparser.add_argument("-v", "--verbose", help="use DEBUG log level", action="store_true")
        argparser.add_argument("-n", "--no-upload", help="don't upload L0/L1 files to the server", action="store_true")
        argparser.add_argument(
            "-ne", "--no-email", help="don't send warning emails to people in the email list", action="store_true"
        )
        argparser.add_argument(
            "-o", "--output-dir", help="directory in which to output generated files", action="store", metavar="DIR"
        )

        return argparser

    def run(self):
        """ Get arguments and perform processing, noting the duration """
        start_time: dt.datetime = dt.datetime.utcnow()
        self.logger.info(f"🤠\tBegining at {start_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\t🤠")

        args: argparse.ArgumentParser = self.argparser.parse_args()

        if args.verbose:
            self.logger.setLevel(logging.DEBUG)

        pipeline_config = ArgparsePipelineConfig(args)
        coordinator: Coordinator = Coordinator(pipeline_config)
        pipeline_query = ArgparsePipelineQuery(args)
        coordinator.execute_pipeline(pipeline_query)

        elapsed_time: dt.timedelta = dt.datetime.utcnow() - start_time
        self.logger.info(f"🤠\tTotal run time: {str(elapsed_time)}\t🤠")


if __name__ == "__main__":
    cli_handler = CLIHandler()
    cli_handler.run()
