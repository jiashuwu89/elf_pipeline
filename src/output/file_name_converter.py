"""Maybe this is over-engineering, maybe it is not"""
import logging

from util.constants import SERVER_BASE_DIR


class FileNameConverter:
    """Convert local file names to server file names for transferring"""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
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
            "state_pred": ["state/pred", "state/pred"],
        }

    def convert(self, source):
        file_info = self.get_file_info(source)
        return self.get_dest(file_info)

    def get_file_info(self, file_path):
        """
        returns a tuple given the file path to a file.
        The tuple has the following format (name_of_file, probe, level, data_type, date)
        """
        # TODO: Give example file names and how they are interpreted
        file_name = file_path.split("/")[-1]
        if "state" not in file_name:
            mission, level, data_type, _ = file_name.split("_")
        else:
            mission, level, _, data_type, _ = file_name.split("_")
            data_type = "state_" + data_type

        self.logger.debug(f"Path {file_path} -> file={file_name} mission={mission} level={level} data_type={data_type}")
        return FileInfo(file_name, mission, int(level[-1]), data_type)

    def get_dest(self, file_info):
        return f"{SERVER_BASE_DIR}/\
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
