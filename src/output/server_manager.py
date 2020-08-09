import paramiko
import logging

from util.credentials import HOST, USERNAME, PASSWORD

# TODO: Logging in separate file


class ServerManager:

    def __init__(self):
        host = HOST
        transport = paramiko.Transport(host)
        transport.connect(username=USERNAME, password=PASSWORD)
        self.sftp_client = paramiko.SFTPClient.from_transport(transport)

        self.logger = logging.getLogger("Server Manager")

        self.file_name_converter = FileNameConverter()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.sftp_client.close()

    def transfer_file(self, source, dest):
        try:
            self.logger.info(f"Transferring {source} to {dest}")
            self.sftp_client.put(source, dest)
            return True
        except Exception as e:
            self.logger.critical(f'⚠️\tFailed to transfer {source} to {dest}: {e}')
            return False

    def transfer_files(self, files):
        """ Transfer all files to server, return # of files transferred successfully """

        count = 0
        for f in files:
            count += self.transfer_file(f, self.file_name_converter.convert(f))

        return count


class FileNameConverter:
    def __init__(self):
        self.data_product_paths = {
            "eng": ["eng", "eng"],
            "epde": ["epd", "epd"],
            "epdi": ["epd", "epd"],
            "epdef": ["epd", "epd/fast/electron"],
            "epdes": ["epd", "epd/survey/electron"],
            "epdif": ["epd", "epd/fast/ion"],
            "epdis": ["epd", "epd/survey/ion"],
            "fgs": ["fgm", "fgm/survey"],
            "fgf": ["fgm", "fgm/fast"],
            "fgm": ["fgm", "fgm"],
            "mrma": ["mrma", "mrma"],
            "mrmi": ["mrmi", "mrmi"],
            "state_defn": ["state/defn", "state/defn"],
            "state_pred": ["state/pred", "state/pred"]
        }

    def convert(self, source):
        file_info = self.get_file_info(source)
        return self.get_dest(file_info)

    def get_file_info(self, file_path):
        """
        returns a tuple given the file path to a file.
        The tuple has the following format (name_of_file, probe, level, data_type, date)
        """
        file_name = file_path.split("/")[-1]
        if "state" not in file_name:
            mission, level, data_type, _ = file_name.split("_")
        else:
            mission, level, _, data_type, _ = file_name.split("_")
            data_type = "state_" + data_type

        return FileInfo(file_name, mission, int(level[-1]), data_type)

    def get_dest(self, file_info):
        return f"/themis/data/elfin/\
            {file_info.mission}/\
            {file_info.level}/\
            {self.data_product_paths[file_info.data_type][file_info.level]}/\
            {file_info.file_name}"


class FileInfo:
    def __init__(self, file_name, mission, level, data_type):
        self.file_name: str = file_name
        self.mission: str = mission
        self.level: int = level
        self.data_type: str = data_type
