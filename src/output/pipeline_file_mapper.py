import os
from typing import Dict, List

from elfin.transfer.file_mapper import FileMapper


class PipelineFileMapper(FileMapper):
    """A FileMapper to map locally-generated CSVs/CDFs to remote paths.

    Parameters
    ----------
    server_base_dir : str
        The base directory on the server, in which all files will be stored
    data_product_paths : Dict[str, str]
        A data structure with data products as keys. The values are lists of
        partial paths, with the item at position i representing the
        path corresponding to the level i file of the data product specified
        by the key
    """

    def __init__(self, server_base_dir: str, data_product_paths: Dict[str, List[str]]):
        super().__init__()
        self.server_base_dir = server_base_dir
        self.data_product_paths = data_product_paths

    def map_file(self, file: str) -> str:
        """Maps a path to a file, to the path on the server.

        TODO: Give example file names and how they are interpreted, testing!

        Parameters
        ----------
        file : str
            The path to a file to be converted

        Returns
        -------
        str
            The path on the server to which a file should be transferred.
        """
        basename = os.path.basename(file)

        if "state" not in basename:
            mission, level, data_type, date, _ = basename.split("_")
        else:
            mission, level, _, data_type, date, _ = basename.split("_")
            data_type = f"state_{data_type}"
        level_num = int(level[-1])
        data_product_path = self.data_product_paths[data_type][level_num]

        self.logger.debug(
            f"Parsed {file}: basename={basename} mission={mission} " + f"level={level} data_type={data_type} date={date}"
        )

        return f"{self.server_base_dir}/{mission}/l{level_num}/{data_product_path}/{basename}"
