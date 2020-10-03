"""Maybe this is over-engineering, maybe it is not"""
import logging

from util.constants import SERVER_BASE_DIR


class FileInfo:
    def __init__(self, file_name: str, mission: str, level: int, data_type: str):
        self.file_name = file_name
        self.mission = mission
        self.level = level
        self.data_type = data_type


class FileNameConverter:
    """Convert local file names to server file names for transferring"""

    def __init__(self) -> None:
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

    def convert(self, source: str) -> str:
        file_info = self.get_file_info(source)
        return self.get_dest(file_info)

    def get_file_info(self, file_path: str) -> FileInfo:
        """
        returns a FileInfo object, given the file path to a file.
        The object contains information about: (name_of_file, probe, level, data_type, date)
        """
        # TODO: Give example file names and how they are interpreted
        file_name = file_path.split("/")[-1]
        if "state" not in file_name:
            mission, level, data_type, date, _ = file_name.split("_")
        else:
            mission, level, _, data_type, date, _ = file_name.split("_")
            data_type = "state_" + data_type

        self.logger.debug(
            f"Path {file_path} -> file={file_name} mission={mission}"
            + f"level={level} data_type={data_type} date={date}"
        )
        return FileInfo(file_name, mission, int(level[-1]), data_type)

    def get_dest(self, file_info: FileInfo) -> str:
        return (
            f"{SERVER_BASE_DIR}/"
            + f"{file_info.mission}/"
            + f"{file_info.level}/"
            + f"{self.data_product_paths[file_info.data_type][file_info.level]}/"
            + f"{file_info.file_name}"
        )
