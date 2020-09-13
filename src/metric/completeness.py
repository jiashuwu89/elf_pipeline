"""Tools to analyze packet times to estimate completeness"""
import datetime as dt
import logging
import math
import statistics

from common import models
from util.science_utils import s_if_plural

# TODO: Remove IDPU_Type
# TODO: Convert to Enum?


class CompletenessUpdater:
    def __init__(self, session, completeness_config):
        self.session = session
        self.completeness_config = completeness_config
        self.logger = logging.getLogger(self.__class__.__name__)

    def update_completeness_table(self, times):
        """Update ScienceZoneCompleteness table, if possible

        Input: series of sorted, not-none times

        TODO: update to take in CompletenessConfig
        """
        # Edge case: empty Series
        if times.empty:
            self.logger.warning("Empty Time Series, cannot update completeness table")
            return

        # Split szs
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

        # Get median diff
        if not self.completeness_config.median_diff:
            diffs = []
            for sz in szs:
                diffs += [(j - i).total_seconds() for i, j in zip(sz[:-1], sz[1:])]
            median_diff = statistics.median(diffs)

        # Update completeness for each science zone
        for sz in szs:
            sz_start_time = sz[0]
            sz_end_time = sz[-1]

            # Find corresponding collection (for the time range)
            # Assumes that only one execution time will be found
            q = (
                self.session.query(models.TimeIntervals)
                .filter(
                    models.TimeIntervals.start_time <= sz_end_time.to_pydatetime(),
                    models.TimeIntervals.end_time >= sz_start_time.to_pydatetime(),
                    models.TimeIntervals.mission_id == self.completeness_config.mission_id,
                    models.TimeIntervals.interval_type == "ExecutionTime",
                    models.Intent.intent_type == self.completeness_config.intent_type,
                )
                .join(models.Intent, models.TimeIntervals.intent_id == models.Intent.id)
                .first()
            )
            if not q:
                self.logger.warning(f"Empty Query, skipping interval {sz_start_time} to {sz_end_time}")
                continue

            start_time = min(
                sz_start_time,
                q.start_time + self.completeness_config.start_delay + self.completeness_config.start_margin,
            )
            end_time = max(
                sz_end_time,
                q.start_time
                + self.completeness_config.start_delay
                + self.completeness_config.expected_collection_duration,
            )
            collection_duration = (end_time - start_time).total_seconds()

            # Get Percent Completeness
            obtained = len(sz)
            estimated_total = math.ceil(collection_duration / median_diff)
            percent_completeness = 100 * obtained / estimated_total

            # Remove previous entries that correspond to this new entry
            self.session.query(models.ScienceZoneCompleteness).filter(
                models.ScienceZoneCompleteness.mission_id == self.completeness_config.mission_id,
                models.ScienceZoneCompleteness.idpu_type == self.completeness_config.idpu_type,
                models.ScienceZoneCompleteness.data_type == self.completeness_config.data_type,
                models.ScienceZoneCompleteness.sz_start_time <= sz_end_time.to_pydatetime(),
                models.ScienceZoneCompleteness.sz_end_time >= sz_start_time.to_pydatetime(),
            ).delete()

            entry = models.ScienceZoneCompleteness(
                mission_id=self.completeness_config.mission_id,
                idpu_type=self.completeness_config.idpu_type,  # TODO: Need to deprecate this column at some point
                data_type=self.completeness_config.data_type,
                sz_start_time=str(start_time),
                sz_end_time=str(end_time),
                completeness=float(percent_completeness),  # TODO: Need to deprecate this column at some point
                num_received=obtained,
                num_expected=estimated_total,
                insert_date=str(dt.datetime.now()),
            )

            self.session.add(entry)
            self.session.commit()
