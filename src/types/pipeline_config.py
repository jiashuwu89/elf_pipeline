from abc import ABC


class PipelineConfig(ABC):
    @property
    def session(self):
        raise NotImplementedError

    @property
    def calculate(self):
        raise NotImplementedError

    @property
    def update_db(self):
        raise NotImplementedError

    @property
    def generate_files(self):
        raise NotImplementedError

    @property
    def output_dir(self):
        raise NotImplementedError

    @property
    def upload(self):
        raise NotImplementedError

    @property
    def email(self):
        raise NotImplementedError
