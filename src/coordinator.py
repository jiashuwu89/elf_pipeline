import logging
import datetime as dt
from dateutil.parser import parse as dateparser
import tempfile

from common import models, db

from request_manager import RequestManager
from processor.processor_manager import ProcessorManager
from server_manager import ServerManager


class Coordinator:

    def __init__(self):

        if db.SESSIONMAKER is None:
            db.connect("production")
        self.session = db.SESSIONMAKER()
        self.logger = logging.getLogger("Coordinator")

        self.func = None
        self.mission_ids = None
        self.times = None
        self.start_time = None
        self.end_time = None
        self.products = None
        self.calculate = None
        self.output_dir = None
        self.upload = None
        self.email = None

        self.request_manager = RequestManager(self.session)
        self.processor_manager = ProcessorManager(self.session)
        self.server_manager = ServerManager()

    def handle_args(self, args):

        # Function to execute
        if args.func == "run_daily":
            self.func = self.run_daily
        elif args.func == "run_dump":
            self.func = self.run_dump
        elif args.func == "run_downlinks":
            self.func = self.run_downlinks
        else:
            raise Exception("No function specified")

        # Mission IDs
        if args.ela:
            self.mission_ids.append(1)
        if args.elb:
            self.mission_ids.append(2)
        if args.em3:
            self.mission_ids.append(3)
        if not self.mission_ids:
            raise Exception("No missions specified")

        # Time range
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

        # Products
        if not args.products:
            raise Exception("No products specified")
        self.products = args.products

        # Calculate
        self.calculate = args.calculate

        # Output Directory (For produced files)
        if args.output_dir:
            self.output_dir = args.output_dir
        else:
            self.output_dir = tempfile.mkdtemp()

        self.upload = not args.no_upload
        self.email = not args.no_email

    def run_func(self):
        processing_requests = self.request_manager.get_processing_requests(
            self.mission_ids,
            self.times,
            self.start_time,
            self.end_time,
            self.calculate)  # TODO: Check name of function

        generated_files = self.processor_manager.generate_files(processing_requests)

        self.server_manager.transfer_files(generated_files)
