import datetime as dt


class CompletenessConfig:
    def __init__(self, data_product):
        if data_product == "mrm":
            self.idpu_type = -1
            self.data_type = "MRM"
            self.intent_type = "AttitudeCollection"
            self.start_delay = dt.timedelta(seconds=0)
            self.start_margin = dt.timedelta(seconds=3)
            self.median_diff = 0.32  # (mrm sample rate = 56.25/18)
            self.expected_collection_duration = dt.timedelta(minutes=24)
        elif data_product == "fgm":
            self.idpu_type = 2
            self.data_type = "FGM"
            self.intent_type = "ScienceCollection"
            self.start_delay = dt.timedelta(seconds=50)  # 3 second margin
            self.start_margin = dt.timedelta(seconds=3)
            self.median_diff = None
            self.expected_collection_duration = dt.timedelta(minutes=6, seconds=5)
        elif data_product == "epde":
            self.idpu_type = 4
            self.data_type = "EPDE"
            self.intent_type = "ScienceCollection"
            # first two spin periods discarded, with 3 seconds margin
            self.start_delay = dt.timedelta(seconds=50)
            self.start_margin = dt.timedelta(seconds=9)
            self.median_diff = None
            self.expected_collection_duration = dt.timedelta(minutes=6, seconds=5)
        elif data_product == "epdi":
            self.idpu_type = 6
            self.data_type = "EPDI"
            self.intent_type = "ScienceCollection"
            self.start_delay = dt.timedelta(seconds=50)
            self.start_margin = dt.timedelta(seconds=9)
            self.median_diff = None
            self.expected_collection_duration = dt.timedelta(minutes=6, seconds=5)
        else:
            raise ValueError(f"Bad Data Product: {data_product}")
