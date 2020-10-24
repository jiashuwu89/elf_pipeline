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
    """A structure to copy local files to the server.

    TODO: Consider adding a method to copy a file on the server to the local
    machine - it may not be necessary for the pipeline, but may be useful in
    other situations?
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

        self.logger.debug("Create Transport")
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
        """Copies a single local file to the server, at the specified dest.

        Parameters
        ----------
        source : str
            The path to the file, locally
        dest : str
            The path to which the file should be sent

        Returns
        -------
        bool
            True if the file was successfully sent, otherwise False
        """
        try:
            self.logger.info(f"Transferring {source} to {dest}")
            self.sftp_client.put(source, dest)
            return True
        except Exception as e:
            self.logger.critical(f"⚠️\tFailed to transfer {source} to {dest}: {e}")
            return False

    def transfer_files(self, files: Iterable[str]) -> int:
        """Transfer all files to server.

        Uses calls to transfer_file, and a FileNameConverter to get the
        destination.

        Parameters
        ----------
        files : Iterable[str]
            All files to be copied

        Returns
        -------
        int
            The number of successfully sent files
        """
        count = 0
        for f in files:
            count += self.transfer_file(f, self.file_name_converter.convert(f))

        return count
