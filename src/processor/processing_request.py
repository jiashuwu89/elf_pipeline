class ProcessingRequest:
    def __init__(self, mission_id, data_product, date):
        """
        mission_id = 1, 2, 3
        data_product
        date = date for which to generate file (ex. 2020-08-05)
        """
        self.mission_id = mission_id
        self.data_product = data_product
        self.date = date

    def to_string(self):
        return f"ProcessingRequest(\
            mission_id={self.mission_id}, \
            data_product={self.data_product}, \
            date={self.date}"
