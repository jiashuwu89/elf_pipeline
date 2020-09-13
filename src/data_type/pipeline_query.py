from abc import ABC


class PipelineQuery(ABC):
    @property  # TODO: use this strategy in all ABCs
    def mission_ids(self):
        raise NotImplementedError

    @property
    def data_products(self):
        raise NotImplementedError

    @property
    def times(self):
        raise NotImplementedError

    @property
    def start_time(self):
        raise NotImplementedError

    @property
    def end_time(self):
        raise NotImplementedError
