"""Tools to analyze packet times to estimate completeness"""
import datetime as dt
import logging
import math
import statistics
from typing import Any, List, Optional, Tuple, Type

import numpy as np
import pandas as pd
import sqlalchemy
from elfin.common import models

from data_type.completeness_config import CompletenessConfig
from data_type.processing_request import ProcessingRequest
from util.constants import COMPLETENESS_TABLE_PRODUCT_MAP, SCIENCE_ZONE_SECTIONS, SMALL_LARGE_GAP_MULTIPLIERS
from util.science_utils import s_if_plural


class CompletenessUpdater:
    """An object to calculate and report the completeness of data.

    Parameters
    ----------
    session
    completeness_config : CompletenessConfig
        An object storing configuration options to be used when calculating
        completeness
    """

    def __init__(self, session: sqlalchemy.orm.session.Session, completeness_config: Type[CompletenessConfig]):
        self.session = session
        self.completeness_config = completeness_config
        self.logger = logging.getLogger(self.__class__.__name__)

    def update_completeness_table(
        self, processing_request: ProcessingRequest, df: pd.DataFrame, update_table: bool
    ) -> bool:
        """Update ScienceZoneCompleteness table, if possible

        Parameters
        ----------
        processing_request : ProcessingRequest
        df : pd.DataFrame
            A DataFrame with two columns, "times" and "idpu_types". The
            "times" column should contain sorted, not-null times...
        update_table : bool
            Determines if completeness entries are written to the database

        Returns
        -------
        bool
            Returns True is the table is uploaded, False if not
        """

        if df.empty:
            self.logger.warning("Empty DataFrame, cannot update completeness table")
            return False

        # Get IDPU Type so that DCT can determine if data is compressed (MRM's idpu_type will be -1)
        # TODO: Handle multiple IDPU Types, this assumes only a single IDPU Type (seems ok for now, usually don't
        # collect compressed and uncompressed)
        idpu_types = df["idpu_type"].unique()
        if len(idpu_types) != 1:
            self.logger.warning(f"Expected only a single unique idpu type, but instead got {len(idpu_types)}")
            return False
        idpu_type = int(idpu_types[0])
        times = df["times"]

        data_type = COMPLETENESS_TABLE_PRODUCT_MAP[processing_request.data_product]

        szs = self.split_science_zones(times)

        median_diff = self.get_median_diff(szs)
        if median_diff is None:
            self.logger.warning("Could not calculate median diff, so could not calculate completeness")
            return False

        small_gap_bound = SMALL_LARGE_GAP_MULTIPLIERS[0] * median_diff
        large_gap_bound = SMALL_LARGE_GAP_MULTIPLIERS[1] * median_diff

        # Update completeness for each science zone
        for sz in szs:
            start_time, end_time = self.estimate_time_range(processing_request, sz)
            if start_time is None and end_time is None:
                continue

            # Get Obtained and Expected Counts
            collection_duration = (end_time - start_time).total_seconds()
            obtained = len(sz)
            estimated_total = math.ceil(collection_duration / median_diff)

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
                    sections[get_gap_position(start_time, end_time, gap)] += 1

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

            self.logger.info(
                "Created completeness entry:\n\t\t"
                + f"mission id {processing_request.mission_id}, data type {data_type}\n\t\t"
                + f"science zone times: {str(start_time)} - {str(end_time)}\n\t\t"
                + f"completeness: {obtained} / {estimated_total}\n\t\t"
                + f"max gap section: {max_section}\n\t\t"
                + f"gap category: {gap_category}\n\t\t"
            )

            if update_table:
                self.session.add(
                    models.ScienceZoneCompleteness(
                        mission_id=processing_request.mission_id,
                        idpu_type=idpu_type,
                        data_type=data_type,
                        sz_start_time=str(start_time),
                        sz_end_time=str(end_time),
                        num_received=obtained,
                        num_expected=estimated_total,
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

    def split_science_zones(self, times: pd.Series) -> List[List[np.datetime64]]:
        """Given a series of times, group them into estimated science zones.

        Parameters
        ----------
        times : pd.Series
            A series of times corresponding to packets (TODO: is this frames?)

        Returns
        -------
        List[List[dt.datetime]]
            A list of science zones, which are each a list of times
        """
        szs = []
        prev_time = times.iloc[0]
        sz = [prev_time]

        for i in range(1, times.shape[0]):
            cur_time = times.iloc[i]
            if cur_time - prev_time > dt.timedelta(minutes=20):
                szs.append(sz.copy())
                sz = [cur_time]
            else:
                sz.append(cur_time)
            prev_time = cur_time
        szs.append(sz)

        self.logger.info(f"Found {len(szs)} science zone{s_if_plural(szs)}")
        return szs

    def get_median_diff(self, szs: List[List[np.datetime64]]) -> Optional[float]:
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
        if self.completeness_config.median_diff is not None:
            return self.completeness_config.median_diff

        diffs = []
        for sz in szs:
            diffs += [(j - i).total_seconds() for i, j in zip(sz[:-1], sz[1:])]

        return statistics.median(diffs) if diffs else None

    # TODO: Get better return type
    def estimate_time_range(self, processing_request: ProcessingRequest, sz: List[np.datetime64]) -> Tuple[Any, Any]:
        """Estimates the start and end time of a collection.

        The start and end times are estimated by using the first and last
        packet times and comparing them with values obtained by querying the
        `time_intervals` table and applying some calculations using the given
        CompletenessConfig

        Parameters
        ----------
        processing_request
        sz

        Returns
        -------
        (start_time, end_time)
            The estimated start and end times of the collection
        """
        sz_start_time = sz[0]
        sz_end_time = sz[-1]

        # Find corresponding collection (for the time range)
        # Assumes that only one execution time will be found
        q = (
            self.session.query(models.TimeIntervals)
            .filter(
                models.TimeIntervals.start_time <= sz_end_time.to_pydatetime(),
                models.TimeIntervals.end_time >= sz_start_time.to_pydatetime(),
                models.TimeIntervals.mission_id == processing_request.mission_id,
                models.TimeIntervals.interval_type == "ExecutionTime",
                models.Intent.intent_type == self.completeness_config.intent_type,
            )
            .join(models.Intent, models.TimeIntervals.intent_id == models.Intent.id)
            .first()
        )
        if not q:
            self.logger.warning(f"Empty Query, skipping interval {sz_start_time} to {sz_end_time}")
            return None, None

        start_time = min(
            sz_start_time,
            q.start_time + self.completeness_config.start_delay + self.completeness_config.start_margin,
        )
        end_time = max(
            sz_end_time,
            q.start_time + self.completeness_config.start_delay + self.completeness_config.expected_collection_duration,
        )

        return start_time, end_time


def get_gap_position(
    start_time: np.datetime64, end_time: np.datetime64, gap: Tuple[np.datetime64, np.datetime64]
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
