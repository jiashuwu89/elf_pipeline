import argparse
import os
from abc import ABC, abstractmethod

import sqlalchemy
from elfin.common import db

from util.constants import STATE_DEFN_CSV_DIR, STATE_PRED_CSV_DIR


class PipelineConfig(ABC):
    """A class to hold configurations of the pipeline."""

    @property
    @abstractmethod
    def session(self) -> sqlalchemy.orm.session.Session:
        """From elfin.common.db, used to make queries to the database"""
        raise NotImplementedError

    @property
    @abstractmethod
    def update_db(self) -> bool:
        """Specifies if the science_downlinks table should be updated"""
        raise NotImplementedError

    @property
    @abstractmethod
    def generate_files(self) -> bool:
        """Specifies if files should be generated"""
        raise NotImplementedError

    @property
    @abstractmethod
    def output_dir(self) -> str:
        """The directory where generated files should be stored"""
        raise NotImplementedError

    @property
    @abstractmethod
    def state_defn_csv_dir(self) -> str:
        """Directory that contains csv files for State CDF generation"""
        raise NotImplementedError

    @property
    @abstractmethod
    def state_pred_csv_dir(self) -> str:
        """Directory that contains csv files for predicted State CDF generation"""
        raise NotImplementedError

    @property
    @abstractmethod
    def upload(self) -> bool:
        """Specifies if files should be uploaded to the server"""
        raise NotImplementedError

    @property
    @abstractmethod
    def email(self):
        """Specifies if warnings should be emailed, if problems occur"""
        raise NotImplementedError


class ArgparsePipelineConfig(PipelineConfig):
    """An object that uses args from argparse to provide a PipelineConfig.

    Parameters
    ----------
    args
        Arguments from argparse (obtained from CLI)
    """

    def __init__(self, args: argparse.Namespace):
        # Initialize DB connection
        if db.SESSIONMAKER is None:
            db.connect("production")
        self._session = db.SESSIONMAKER()

        # Initialize parameters/options from command line
        self._update_db = self.db_update_necessary(args.abandon_calculated_products)
        self._generate_files = self.file_generation_necessary(args.subcommand)
        self._output_dir = self.validate_output_dir(args.output_dir)
        self._upload = self.upload_necessary(args.withhold_files, self.generate_files)
        self._email = self.email_necessary(args.quiet)

    @property
    def session(self) -> sqlalchemy.orm.session.Session:
        return self._session

    @property
    def update_db(self) -> bool:
        return self._update_db

    @property
    def generate_files(self) -> bool:
        return self._generate_files

    @property
    def output_dir(self) -> str:
        return self._output_dir

    @property
    def state_defn_csv_dir(self) -> str:
        return STATE_DEFN_CSV_DIR

    @property
    def state_pred_csv_dir(self) -> str:
        return STATE_PRED_CSV_DIR

    @property
    def upload(self) -> bool:
        return self._upload

    @property
    def email(self) -> bool:
        return self._email

    @staticmethod
    def db_update_necessary(abandon_calculated_products: bool) -> bool:
        """Determines if it is necessary to upload products that were calculated."""
        return not abandon_calculated_products

    @staticmethod
    def file_generation_necessary(subcommand: str) -> bool:
        return subcommand in ["daily", "dump"]

    @staticmethod
    def validate_output_dir(output_dir: str) -> str:
        if output_dir:
            if not os.path.isdir(output_dir):
                raise ValueError(f"Bad Output Directory: {output_dir}")
        return output_dir

    @staticmethod
    def upload_necessary(withhold_files: bool, generate_files: bool) -> bool:
        return not withhold_files and generate_files

    @staticmethod
    def email_necessary(quiet: bool) -> bool:
        return not quiet
