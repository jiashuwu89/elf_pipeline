"""Tools to analyze packet times to estimate completeness"""
import datetime as dt
import logging
import math
import statistics
from typing import Any, Dict, List, Optional, Tuple, Type

import intervaltree
import numpy as np
import pandas as pd
import sqlalchemy
from elfin.common import models

from data_type.completeness_config import CompletenessConfig
from data_type.processing_request import ProcessingRequest
from util.constants import (
    COMPLETENESS_TABLE_PRODUCT_MAP,
    GAP_CATEGORIZATION_DATA_TYPES,
    MRM_PRODUCTS,
    SCIENCE_ZONE_SECTIONS,
    SMALL_LARGE_GAP_MULTIPLIERS,
)
from util.science_utils import s_if_plural

INTERVAL_TYPE = "ExecutionTime"
INTENT_TYPE = "ScienceCollection"
REFACTOR_CUTOFF_DATE = dt.date(2020, 6, 12)


class CompletenessUpdater:
    """An object to calculate and report the completeness of data.

    Parameters
    ----------
    session
    completeness_config : CompletenessConfig
        An object storing configuration options to be used when calculating
        completeness
    """

    def __init__(
        self, session: sqlalchemy.orm.session.Session, completeness_config_map: Dict[Any, Type[CompletenessConfig]]
    ):
        self.session = session
        self.completeness_config_map = completeness_config_map
        self.logger = logging.getLogger(self.__class__.__name__)

    def update_completeness_table(
        self, processing_request: ProcessingRequest, df: pd.DataFrame, update_table: bool
    ) -> None:
        """Update ScienceZoneCompleteness table, if possible

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame
            A DataFrame with two columns, "times" and "idpu_type". The
            "times" column should contain sorted, not-null times...
        update_table : bool
            Determines if completeness entries are written to the database
        """

        if df.empty:
            self.logger.warning("Empty DataFrame, cannot update completeness table")
            return

        for idpu_type in df["idpu_type"].unique():
            cur_df = df.loc[df["idpu_type"] == idpu_type]
            self.update_completeness_table_with_single_idpu_type(processing_request, cur_df, update_table)

    def update_completeness_table_with_single_idpu_type(
        self, processing_request: ProcessingRequest, df: pd.DataFrame, update_table: bool
    ) -> bool:
        """Update ScienceZoneCompleteness table, if possible

        Serves as a helper for method update_completeness_table.

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame
            A DataFrame with two columns, "times" and "idpu_type". The
            "idpu_type" column should consist of a single type. The
            "times" column should contain sorted, not-null times...
        update_table : bool
            Determines if completeness entries are written to the database

        Returns
        -------
        bool
            True if the completeness table could be successfully updated,
            False otherwise
        """
        if df.empty:
            self.logger.warning("Empty DataFrame, cannot update completeness table")
            return False

        idpu_types = df["idpu_type"].unique()
        if len(idpu_types) != 1:
            self.logger.warning(f"Expected a single unique idpu type, not {len(idpu_types)} - skipping completeness!")
            return False

        idpu_type = int(idpu_types[0])
        data_type = COMPLETENESS_TABLE_PRODUCT_MAP[processing_request.data_product][idpu_type]
        completeness_config = self.completeness_config_map[data_type]

        times = df["times"]
        szs = self.split_science_zones(processing_request, completeness_config, times)

        median_diff = self.get_median_diff(completeness_config, szs)
        if median_diff is None:
            self.logger.warning("Could not calculate median diff, so could not calculate completeness")
            return False

        small_gap_bound = SMALL_LARGE_GAP_MULTIPLIERS[0] * median_diff
        large_gap_bound = SMALL_LARGE_GAP_MULTIPLIERS[1] * median_diff

        # Update completeness for each science zone
        for sz in szs:
            start_time, end_time, found_interval = self.estimate_time_range(processing_request, completeness_config, sz)

            # Get Obtained and Expected Counts
            collection_duration = (end_time - start_time).total_seconds()
            obtained = len(sz)
            estimated_total = max(1, math.ceil(collection_duration / median_diff))

            # Store data about gaps
            small_gaps, large_gaps = [], []
            small_gap_duration, large_gap_duration = 0, 0
            sections = {
                "in beginning": 0,
                "in middle": 0,
                "in end": 0,
                "in beginning/middle": 0,
                "in middle/end": 0,
                "throughout": 0,
            }

            # Count number of small, medium, and large gaps and their locations
            # Keep track of number of gaps in each section of science zone
            for i, j in zip(sz[:-1], sz[1:]):
                gap_size = (j - i).total_seconds()
                gap = (i, j)
                if gap_size > large_gap_bound:
                    large_gaps.append(gap)
                    large_gap_duration += gap_size
                elif gap_size > small_gap_bound:
                    small_gaps.append(gap)
                    small_gap_duration += gap_size
                if gap_size > small_gap_bound:
                    sections[self.get_gap_position(start_time, end_time, gap)] += 1

            gap_category = ""
            max_gaps, max_section = 0, "None"

            # Form string describing the gaps within current science zone
            if large_gap_duration == 0 and small_gap_duration == 0:
                gap_category = (
                    "Cut short"
                    if obtained / estimated_total < 0.4 or collection_duration < 90
                    else "No significant gaps"
                )
            else:
                if large_gap_duration >= small_gap_duration:
                    gap_category += f"{str(len(large_gaps))} large "
                    gaps = large_gaps
                else:
                    gap_category += f"{str(len(small_gaps))} small "
                    gaps = small_gaps

                gap_category += "gap, " if len(gaps) == 1 else "gaps, "

                # Determine section with most gaps
                for section, count in sections.items():
                    if count >= max_gaps:
                        max_gaps = count
                        max_section = section

                if max_section:
                    gap_category += max_section if max_section == "throughout" else f"mainly {max_section}"

            if data_type not in GAP_CATEGORIZATION_DATA_TYPES:
                gap_category = ""

            if update_table:
                self.session.add(
                    models.ScienceZoneCompleteness(
                        mission_id=processing_request.mission_id,
                        idpu_type=idpu_type,
                        data_type=data_type,
                        sz_start_time=str(start_time),
                        sz_end_time=str(end_time),
                        num_received=obtained,
                        num_expected=estimated_total if found_interval else None,
                        insert_date=str(dt.datetime.now()),
                        gap_category=gap_category,
                    )
                )

        if update_table:
            self.session.flush()
            self.session.commit()
            self.logger.info("All completeness entries committed")
        else:
            self.logger.info("No completeness entries uploaded, as specified")

        return True

    def split_science_zones(
        self, processing_request: ProcessingRequest, completeness_config: CompletenessConfig, times: pd.Series
    ) -> List[List[np.datetime64]]:
        """Given a series of times, group them into estimated science zones.

        Parameters
        ----------
        processing_request : ProcessingRequest
        completeness_config : CompletenessConfig
        times : pd.Series
            A series of times corresponding to packets (TODO: is this frames?)

        Returns
        -------
        List[List[dt.datetime]]
            A list of science zones, which are each a list of times
        """

        time_intervals = (
            self.session.query(models.TimeIntervals)
            .filter(
                models.TimeIntervals.mission_id == processing_request.mission_id,
                models.TimeIntervals.interval_type == INTERVAL_TYPE,
                models.TimeIntervals.start_time <= times.iloc[-1].to_pydatetime(),
                models.TimeIntervals.end_time >= times.iloc[0].to_pydatetime(),
                models.Intent.intent_type == INTENT_TYPE,
            )
            .join(models.Intent, models.TimeIntervals.intent_id == models.Intent.id)
        )

        def different_science_zones(cur_time, prev_time):
            return abs(cur_time - prev_time) > completeness_config.classic_science_zone_gap_size

        # After this date, the refactor changes have stabilized to the point where it
        # is feasible to use the `time_intervals` table for its time ranges. Before,
        # we know for sure that collections occur far apart enough that our original
        # criteria suffices.
        if (
            time_intervals.count() != 0
            and processing_request.date > REFACTOR_CUTOFF_DATE
            and processing_request.data_product not in MRM_PRODUCTS
        ):
            it = intervaltree.IntervalTree()
            for interval in time_intervals:
                predicted_start_time = (
                    interval.start_time + completeness_config.start_delay + completeness_config.start_margin
                )
                predicted_end_time = (
                    interval.start_time
                    + completeness_config.start_delay
                    + completeness_config.expected_collection_duration
                )
                delta = (predicted_end_time - predicted_start_time) / 2
                it[predicted_start_time:predicted_end_time] = predicted_start_time + delta

            it.merge_overlaps(data_reducer=lambda x, y: x + ((y - x) / 2))

            center_times = {}
            boundary_times = []
            for interval in it:
                boundary_times += [interval.begin, interval.end]
                center_times[interval.begin] = interval.data
                center_times[interval.end] = interval.data

            orig_different_science_zones = different_science_zones

            def different_science_zones(cur_time, prev_time):
                def closest_center(t):
                    """The center of the interval closest to the current point."""
                    return center_times[min(boundary_times, key=lambda x: abs((x - t).total_seconds()))]

                # The delta threshold can probably be lowered if the two times are "close" to different intervals
                # Note that 1 minute was chosen arbitrarily, but seemed ok when testing
                return orig_different_science_zones(cur_time, prev_time) or (
                    (closest_center(cur_time) != closest_center(prev_time))
                    and (abs(cur_time - prev_time) > dt.timedelta(minutes=1))
                )

        szs = []
        prev_time = times.iloc[0]
        sz = [prev_time]

        for i in range(1, times.shape[0]):
            cur_time = times.iloc[i]
            if different_science_zones(cur_time, prev_time):
                if any(t.date() == processing_request.date for t in sz):
                    szs.append(sz.copy())
                sz = [cur_time]
            else:
                sz.append(cur_time)
            prev_time = cur_time
        if any(t.date() == processing_request.date for t in sz):
            szs.append(sz)

        self.logger.info(f"Found {len(szs)} science zone{s_if_plural(szs)}")
        return szs

    def get_median_diff(
        self, completeness_config: CompletenessConfig, szs: List[List[np.datetime64]]
    ) -> Optional[float]:
        """Calculates the median diff (delta) between packets grouped by zone.

        We find the time delta between a packet and its immediate neighbors,
        for each packet in a science zone, for each science zone in the given
        science zones. This value can be used to help estimate the total
        number of packets that can be expected from a science zone.

        Parameters
        ----------
        szs : List[List[dt.datetime]]
            A list of science zones

        Returns
        -------
        Optional[float]
            The median diff, if a median diff can be calculated. Otherwise,
            returns None.
        """
        if completeness_config.median_diff is not None:
            return completeness_config.median_diff

        diffs = []
        for sz in szs:
            diffs += [(j - i).total_seconds() for i, j in zip(sz[:-1], sz[1:])]

        return statistics.median(diffs) if diffs else None

    # TODO: Get better return type
    def estimate_time_range(
        self, processing_request: ProcessingRequest, completeness_config: CompletenessConfig, sz: List[np.datetime64]
    ) -> Tuple[Any, Any, bool]:
        """Estimates the start and end time of a collection.

        The start and end times are estimated by using the first and last
        packet times and comparing them with values obtained by querying the
        `time_intervals` table and applying some calculations using the given
        CompletenessConfig

        Parameters
        ----------
        processing_request : ProcessingRequest
        completeness_config : CompletenessConfig
        sz : List[np.datetime64]

        Returns
        -------
        (start_time, end_time, found_interval)
            The estimated start and end times of the collection, along with
            a bool representing whether a corresponding time interval had been
            found to compare with.
        """
        sz_start_time = sz[0]
        sz_end_time = sz[-1]

        # Find corresponding collection (for the time range)
        # Assumes that only one execution time will be found
        # TODO: Check this assumption, may not hold anymore
        q = (
            self.session.query(models.TimeIntervals)
            .filter(
                models.TimeIntervals.start_time <= sz_end_time.to_pydatetime(),
                models.TimeIntervals.end_time >= sz_start_time.to_pydatetime(),
                models.TimeIntervals.mission_id == processing_request.mission_id,
                models.TimeIntervals.interval_type == "ExecutionTime",
                models.Intent.intent_type == completeness_config.intent_type,
            )
            .join(models.Intent, models.TimeIntervals.intent_id == models.Intent.id)
            .first()
        )
        if not q:
            self.logger.warning(f"Empty Query, skipping interval {sz_start_time} to {sz_end_time}")
            return sz_start_time, sz_end_time, False

        start_time = min(
            sz_start_time,
            q.start_time + completeness_config.start_delay + completeness_config.start_margin,
        )
        end_time = max(
            sz_end_time,
            q.start_time + completeness_config.start_delay + completeness_config.expected_collection_duration,
        )

        return start_time, end_time, True

    def get_gap_position(
        self, start_time: np.datetime64, end_time: np.datetime64, gap: Tuple[np.datetime64, np.datetime64]
    ) -> str:
        """Determines which section in the science zone a gap occurs in.

        The start and end time of the gap can be located in one of three sections.
        Depending on this combination of sections, a string is returned.

        Parameters
        ----------
        duration : float
            The duration of collection for the science zone.
        start_time : float
            The start time of collection for the science zone.
        gap : Tuple[np.datetime64, np.datetime64]
            A tuple holding the start and end time of the gap.

        Returns
        -------
        str
            A string is returned, representing the qualitative section
            in which the gap is located.
        """
        gap_start, gap_end = gap
        duration = end_time - start_time
        FIRST_LIMIT = start_time + duration * SCIENCE_ZONE_SECTIONS[0]
        SECOND_LIMIT = start_time + duration * SCIENCE_ZONE_SECTIONS[1]

        if gap_start < FIRST_LIMIT and gap_end < FIRST_LIMIT:
            return "in beginning"
        if gap_start < FIRST_LIMIT and gap_end < SECOND_LIMIT:
            return "in beginning/middle"
        if gap_start < FIRST_LIMIT and gap_end >= SECOND_LIMIT:
            return "throughout"
        if gap_start < SECOND_LIMIT and gap_end < SECOND_LIMIT:
            return "in middle"
        if gap_start < SECOND_LIMIT and gap_end >= SECOND_LIMIT:
            return "in middle/end"
        return "in end"
