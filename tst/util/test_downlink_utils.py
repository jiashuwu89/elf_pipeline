import pandas as pd

from util import downlink_utils
from util.constants import TEST_DATA_DIR


class TestDownlinkUtils:
    df_0 = pd.read_csv(f"{TEST_DATA_DIR}/csv/df/df_0.csv")
    df_1 = pd.read_csv(f"{TEST_DATA_DIR}/csv/df/df_1.csv")
    df_3 = pd.read_csv(f"{TEST_DATA_DIR}/csv/df/df_3.csv")
    df_4 = pd.read_csv(f"{TEST_DATA_DIR}/csv/df/df_4.csv")
    merged_01 = pd.read_csv(f"{TEST_DATA_DIR}/csv/df/merged_01.csv")
    merged_34 = pd.read_csv(f"{TEST_DATA_DIR}/csv/df/merged_34.csv")

    def test_calculate_offset(self):
        assert downlink_utils.calculate_offset(self.df_0, self.df_0) == 0
        assert downlink_utils.calculate_offset(self.df_0, self.df_1) == 2674
        assert downlink_utils.calculate_offset(self.df_1, self.df_0) == -2674
        assert downlink_utils.calculate_offset(self.df_3, self.df_4) == 0
        assert downlink_utils.calculate_offset(self.df_4, self.df_3) == 0

    def test_merge_downlinks(self):
        columns_to_check = [
            "packet_data",
            "data",
            "id",
            "packet_id",
            "mission_id",
            "timestamp",
            "idpu_time",
            "numerator",
            "denominator",
        ]

        merged_01 = downlink_utils.merge_downlinks(self.df_0, self.df_1, 2674)
        for column in columns_to_check:
            try:
                pd.testing.assert_series_equal(merged_01[column], self.merged_01[column], check_dtype=False)
            except Exception as e:
                raise AssertionError(f"Column {column} does not match: {e}")

        merged_34 = downlink_utils.merge_downlinks(self.df_3, self.df_4, 0)
        for column in columns_to_check:
            try:
                pd.testing.assert_series_equal(merged_34[column], self.merged_34[column], check_dtype=False)
            except Exception as e:
                raise AssertionError(f"Column {column} does not match: {e}")
