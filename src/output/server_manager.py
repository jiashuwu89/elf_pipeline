import logging
from typing import Iterable

import paramiko

from output.file_name_converter import FileNameConverter

try:
    from util.credentials import HOST, PASSWORD, USERNAME
except ModuleNotFoundError:
    HOST, PASSWORD, USERNAME = "", "", ""


# TODO: Typing
class ServerManager:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.info("Create Transport")
        transport = paramiko.Transport(HOST)

        self.logger.debug("Connecting")
        transport.connect(username=USERNAME, password=PASSWORD)

        self.logger.debug("Getting SFTP Client from Transport")
        self.sftp_client = paramiko.SFTPClient.from_transport(transport)

        self.file_name_converter = FileNameConverter()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sftp_client.close()

    def transfer_file(self, source: str, dest: str) -> bool:
        try:
            self.logger.info(f"Transferring {source} to {dest}")
            self.sftp_client.put(source, dest)
            return True
        except Exception as e:
            self.logger.critical(f"⚠️\tFailed to transfer {source} to {dest}: {e}")
            return False

    def transfer_files(self, files: Iterable[str]) -> int:
        """ Transfer all files to server, return # of files transferred successfully """
        count = 0
        for f in files:
            count += self.transfer_file(f, self.file_name_converter.convert(f))

        return count
