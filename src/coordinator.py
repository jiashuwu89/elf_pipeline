"""
The Coordinator class coordinates the main tasks of the pipeline:
    - Determining what new data was obtained and what files should be created
    - Creation of new files
    - Uploading of new files, and reporting errors
"""


import datetime as dt
import logging
import os
import tempfile

from dateutil.parser import parse as dateparser

from common import db, models
from db.request_manager import RequestManager
from output.exception_collector import ExceptionCollector
from output.server_manager import ServerManager
from processor.processor_manager import ProcessorManager


class Coordinator:
    """
    mission_ids
        list containing subset of 1, 2, 3 for ELA, ELB, EM3 (respectively)
    times
        "downlink" or "collection", for downlink time and collection time,
        respectively
    start_time/end_time
        time range to search for data
    products
        list containing subset of ALL_PRODUCTS, specifying products for which
        to search for data
    calculate
        Search for new data and use to calculate new downlinks, as opposed to
        using downlinks already found in science downlinks table
    upload_to_db
        Relevant ONLY IF calculate is True. Upload calculated downlinks to the
        science downlinks table
    output_dir
        Directory in which to put files when generated
    upload
        Upload generated files to server
    email
        Email notifications if exceptions occurred during processing
    """

    def __init__(self):

        if db.SESSIONMAKER is None:
            db.connect("production")
        self.session = db.SESSIONMAKER()
        self.logger = logging.getLogger("Coordinator")

        self.mission_ids = None
        self.times = None
        self.start_time = None
        self.end_time = None
        self.products = None
        self.calculate = None
        self.upload_to_db = None
        self.output_dir = None
        self.upload = None
        self.email = None

        self.request_manager = RequestManager(self.session)
        self.processor_manager = ProcessorManager(self.session)
        self.server_manager = ServerManager()
        self.exception_collector = ExceptionCollector(DAILY_EMAIL_LIST)

    def handle_args(self, args):

        if args.ela:
            self.mission_ids.append(1)
        if args.elb:
            self.mission_ids.append(2)
        if args.em3:
            self.mission_ids.append(3)
        if not self.mission_ids:
            self.log.info("No missions specified, defaulting to ELA and ELB")
            self.mission_ids = ALL_MISSIONS

        if args.func == "run_daily":
            self.times = "downlink"
            self.end_time = dt.datetime(*dt.datetime.utcnow().timetuple()[:4])
            self.start_time = self.end_time - dt.timedelta(hours=5)
        elif args.d:
            self.times = "downlink"
            self.start_time = dateparser(args.d[0], tzinfos=0)
            self.end_time = dateparser(args.d[1], tzinfos=0)
        elif args.c:
            self.times = "collection"
            self.start_time = dateparser(args.c[0], tzinfos=0)
            self.end_time = dateparser(args.c[1], tzinfos=0)
        else:
            raise Exception("Need either downlink time or collection time range")

        if not args.products:
            raise Exception("No products specified")
        self.products = args.products

        self.calculate = args.calculate in ["yes", "nodb"]

        self.upload_to_db = args.calculate not in ["no", "nodb"]

        self.generate_files = args.func in ["run_daily", "run_dump"]

        if args.output_dir:
            if os.path.isdir(args.output_dir):
                self.output_dir = args.output_dir
            else:
                raise ValueError(f"Bad Output Directory: {args.output_dir}")
        else:
            self.output_dir = tempfile.mkdtemp()

        self.upload = not args.no_upload and self.generate_files

        self.email = not args.no_email

    def run_func(self):
        try:
            processing_requests = self.request_manager.get_processing_requests(
                self.mission_ids,
                self.data_products,  # TODO: Sort out product name vs idpu_type, not 1 to 1
                self.times,
                self.start_time,
                self.end_time,
                self.calculate,
                self.update_db,
            )  # TODO: Check name of function

            if self.generated_files:
                generated_files = self.processor_manager.generate_files(processing_requests)

            if self.upload:
                self.server_manager.transfer_files(generated_files)

        except Exception as e:
            traceback_msg = traceback.format_exc()
            self.exception_collector.record_exception(e, traceback_msg)

        if self.exception_collector.email_list:
            self.exception_collector.email()
