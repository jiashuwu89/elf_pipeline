import os
import re

from elfin.transfer.server_manager import ServerManager


class PipelineServerManager(ServerManager):
    """Local copy of the ServerManager that deletes old L0 data products

    Parameters
    ----------
    host
    username : str
    password : str

    """

    def put(self, local_path: str, remote_path: str) -> bool:
        """Copies a single local file to the server, at the specified destination
        If the file is l0, deletes old l0 .pkts on the server beforehand


        Parameters
        ----------
        local_path : str
            The path to the file, locally
        remote_path : str
            The path to which the file should be sent

        Returns
        -------
        bool
            True if the file was successfully sent, otherwise False
        """

        remote_directory, file_name = os.path.split(remote_path)

        # Only l0 files are not being automatically overwritten
        if file_name.endswith(".pkt"):
            outdated_file_pattern = re.sub(r"_[0-9]+\.pkt$", "_[0-9]+\\.pkt$", file_name)
            outdated_file_regex = re.compile(outdated_file_pattern)

            file_names = self.sftp_client.listdir(remote_directory)
            outdated_files = [file_name for file_name in file_names if outdated_file_regex.match(file_name)]
            for outdated_file in outdated_files:
                self.logger.info(f"Deleting previously generated file {outdated_file}")
                try:
                    self.sftp_client.remove(f"{remote_directory}/{outdated_file}")
                except Exception as e:
                    self.logger.critical(f"⚠️\tFailed delete: {outdated_file} ({e})")

        return super().put(local_path, remote_path)
