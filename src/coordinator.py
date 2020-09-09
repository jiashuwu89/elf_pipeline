"""A class to coordinate the main tasks of the pipeline.
These tasks are:
    - (Extract) Determining what new data was obtained and what files should
    be created
    - (Transform) Creation of new files
    - (Load) Uploading of new files, and reporting errors
"""


import datetime as dt
import logging
import os
import tempfile
import traceback
from typing import List, Optional

from dateutil.parser import parse as dateparser

from common import db
from db.request_manager import RequestManager
from output.exception_collector import ExceptionCollector
from output.server_manager import ServerManager
from processor.processor_manager import ProcessorManager
from util.constants import ALL_MISSIONS, DAILY_EMAIL_LIST

# TODO: self.times should be an enum


class Coordinator:
    """Coordinator class to coordinate the pipeline.

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

    def __init__(self, args):
        self.logger: logging.Logger = logging.getLogger(self.__class__.__name__)

        # Initialize DB connection
        if db.SESSIONMAKER is None:
            db.connect("production")
        self.session = db.SESSIONMAKER()

        # Initialize parameters/options from command line
        self.mission_ids = self.get_mission_ids(args.ela, args.elb, args.em3)
        self.times, self.start_time, self.end_time = self.get_times(args.func, args.d, args.c)
        self.products = self.get_data_products(args.products)
        self.calculate = self.downlink_calculation_necessary(self.times, args.calculate)
        self.upload_to_db = self.downlink_upload_necessary(args.func, args.calculate)
        self.generate_files = self.file_generation_necessary(args.func)
        self.output_dir = self.get_output_dir(args.output_dir)
        self.upload = self.upload_necessary(args.no_upload, args.generate_files)
        self.email = self.email_necessary(args.no_email)

        # Initialize Pipeline Managers
        self.request_manager = RequestManager(self.session, self.calculate, self.upload_to_db)
        self.processor_manager = ProcessorManager(self.session)
        self.server_manager = ServerManager()
        self.exception_collector = ExceptionCollector(DAILY_EMAIL_LIST)

    def run_func(self):
        """Execute the pipeline"""
        try:
            # Extract
            processing_requests = self.request_manager.get_processing_requests(
                self.mission_ids,
                self.data_products,  # TODO: Sort out product name vs idpu_type, not 1 to 1
                self.times,
                self.start_time,
                self.end_time,
                self.calculate,
                self.update_db,
            )

            # Transform
            if self.generated_files:
                generated_files = self.processor_manager.generate_files(processing_requests)

            # Load
            if self.upload:
                self.server_manager.transfer_files(generated_files)

        except Exception as e:
            traceback_msg = traceback.format_exc()
            self.exception_collector.record_exception(e, traceback_msg)

        if self.exception_collector.email_list:
            self.exception_collector.email()

    def get_mission_ids(self, ela, elb, em3):
        """Determine which missions to process, defaulting to ELA and ELB only"""
        mission_ids = []

        if ela:
            mission_ids.append(1)
        if elb:
            mission_ids.append(2)
        if em3:
            mission_ids.append(3)
        if len(mission_ids) == 0:
            self.logger.info("No missions specified, defaulting to ELA and ELB")
            mission_ids = [mission_id for mission_id in ALL_MISSIONS]

        return mission_ids

    def get_times(self, func, d, c):
        if func == "run_daily":
            times = "downlink"
            end_time = dt.datetime(*dt.datetime.utcnow().timetuple()[:4])
            start_time = end_time - dt.timedelta(hours=5)
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

    def get_data_products(self, products):
        if not products:
            raise Exception("No products specified")
        return products

    def downlink_calculation_necessary(self, times, calculate):
        return times == "downlink" or calculate in ["yes", "nodb"]

    def downlink_upload_necessary(self, func, calculate):
        return func == "run_daily" or calculate == "yes"

    def file_generation_necessary(self, func):
        return func in ["run_daily", "run_dump"]

    def get_output_dir(self, output_dir):
        if output_dir and not os.path.isdir(output_dir):
            raise ValueError(f"Bad Output Directory: {output_dir}")
        else:
            output_dir = tempfile.mkdtemp()
            self.log.debug(f"Temporary output directory created: {output_dir}")
        return output_dir

    def upload_necessary(self, no_upload, generate_files):
        return not no_upload and generate_files

    def email_necessary(self, no_email):
        return not no_email
