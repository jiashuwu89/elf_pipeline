""" CLI for running the Science Processing Pipeline

python run.py --help
"""

import argparse
import datetime as dt
import logging

import util.constants
from coordinator import Coordinator

# TODO: logging.getLogger(__name__)
# https://docs.python.org/3/library/logging.html#logging.basicConfig


# Logging Init
LOG_FORMAT = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("science")


class CLIHandler:
    def __init__(self):
        self.logger = logging.getLogger("CLI")
        self.argparser = self.get_argparser()

    def get_argparser(self):
        """ Get argparser for parsing arguments """

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
            choices=util.constants.ALL_PRODUCTS,
            default=util.constants.ALL_PRODUCTS,
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
        start_time = dt.datetime.utcnow()
        self.logger.info(f"🤠\tBegining at {start_time.strftime('%Y-%m-%d %H:%M:%S')} (UTC)\t🤠")

        args = self.argparser.parse_args()

        if args.verbose:
            self.logger.setLevel(logging.DEBUG)

        coordinator = Coordinator()
        coordinator.handle_args(args)
        coordinator.run_func()

        elapsed_time = dt.datetime.utcnow() - start_time
        logger.info(f"🤠\tTotal run time: {str(elapsed_time)}\t🤠")


if __name__ == "__main__":
    cliHandler = CLIHandler()
    cliHandler.run()
