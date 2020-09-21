"""CLI for running the Science Processing Pipeline

For additional help, run: python run.py --help
"""

import argparse
import datetime as dt
import logging
import tempfile

from coordinator import Coordinator
from data_type.pipeline_config import ArgparsePipelineConfig
from data_type.pipeline_query import ArgparsePipelineQuery
from util.constants import ALL_PRODUCTS, LOOK_BEHIND_DELTA

# Logging Init
LOG_FORMAT: str = "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
DATE_FORMAT: str = "%H:%M:%S"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)


class CLIHandler:
    """A class to parse arguments and use them to run the pipeline"""

    def __init__(self):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)  # TODO: Log to file
        self.argparser: argparse.ArgumentParser = self.get_argparser()

    @staticmethod
    def get_argparser():
        """ Get argparser for parsing arguments """
        argparser = argparse.ArgumentParser(description="Process Science Data from ELFIN")

        # General Options
        argparser.add_argument("-v", "--verbose", help="use DEBUG log level", action="store_true")
        argparser.add_argument(
            "-w", "--withhold-files", help="avoid uploading L0/L1 files to the server", action="store_true"
        )
        argparser.add_argument("-q", "--quiet", help="if problems occur, don't notify via email", action="store_true")
        argparser.add_argument(
            "-o",
            "--output-dir",
            help="specify directory to output generated files (Default: temporary directory)",
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
            abandon_calculated_products=False,
            downlink_time=[start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S")],
            products=ALL_PRODUCTS,
        )

        # dump subcommand
        sub_dump = subcommands.add_parser(
            "dump",
            help="Selectively process science data",
            description="Reprocess and generate a dump of specific science data products.",
        )
        sub_dump.set_defaults(subcommand="dump")
        sub_dump = CLIHandler.add_common_options(sub_dump)

        # downlinks subcommand
        sub_downlinks = subcommands.add_parser(
            "downlinks",
            help="Calculate/List downlink entries",
            description="scan downlinked science packets and group them into downlink entries",
        )
        sub_downlinks.set_defaults(subcommand="downlinks")
        sub_downlinks = CLIHandler.add_common_options(sub_downlinks)

        return argparser

    @staticmethod
    def add_common_options(sub_parser):
        # Missions
        sub_parser.add_argument("-1", "--ela", help="process ELA data", action="store_true")
        sub_parser.add_argument("-2", "--elb", help="process ELB data", action="store_true")
        sub_parser.add_argument("-3", "--em3", help="process EM3 data", action="store_true")

        sub_parser.add_argument(
            "-a",
            "--abandon-calculated-products",
            help="avoid updating the science_downlink table with products that were calculated during execution "
            + "(for example, downlinks or completeness)",
            action="store_true",
        )
        sub_parser.add_argument(
            "-c",
            "--select-downlinks-by-collection-time",
            help="for IDPU data types, utilize collection times in the science_downlinks table to determine days to"
            + "process (as opposed to gathering IDPU data through calculating downlinks",
            action="store_true",
        )
        sub_parser.add_argument(  # TODO: Make this positional instead?
            "-p",
            "--products",
            help="process data belonging to PRODUCTS",
            action="store",
            nargs="+",
            choices=ALL_PRODUCTS,
            default=ALL_PRODUCTS,
            metavar="PRODUCTS",
        )

        sub_parser.add_argument("start_time", help="process data beginning on this day", action="store")
        sub_parser.add_argument("end_time", help="process data before this day", action="store")

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
