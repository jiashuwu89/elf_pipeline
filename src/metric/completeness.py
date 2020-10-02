"""Tools to analyze packet times to estimate completeness"""
import datetime as dt
import logging
import math
import statistics

from elfin.common import models

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

    def __init__(self, session, completeness_config):
        self.session = session
        self.completeness_config = completeness_config
        self.logger = logging.getLogger(self.__class__.__name__)

    def update_completeness_table(self, processing_request, times) -> None:
        """Update ScienceZoneCompleteness table, if possible

        Parameters
        ----------
        processing_request : ProcessingRequest
        times : pd.Series
            A series of sorted, not-null times...

        Returns
        -------
        bool
            Returns True is the table is uploaded, False if not
        """
        if times.empty:
            self.logger.warning("Empty Time Series, cannot update completeness table")
            return False

        szs = self.split_science_zones(times)

        median_diff = self.get_median_diff(szs)
        if median_diff is None:
            self.logger.warning("Could not calculate median diff, so could not calculate completeness")
            return False

        # Update completeness for each science zone
        for sz in szs:
            start_time, end_time = self.estimate_time_range(processing_request, sz)
            if start_time is None and end_time is None:
                continue

            # Get Obtained and Expected Counts
            collection_duration = (end_time - start_time).total_seconds()
            obtained = len(sz)
            estimated_total = math.ceil(collection_duration / median_diff)

            # Remove previous entries that correspond to this new entry
            self.session.query(models.ScienceZoneCompleteness).filter(
                models.ScienceZoneCompleteness.mission_id == processing_request.mission_id,
                models.ScienceZoneCompleteness.data_type == processing_request.data_type,
                models.ScienceZoneCompleteness.sz_start_time <= sz[-1].to_pydatetime(),
                models.ScienceZoneCompleteness.sz_end_time >= sz[0].to_pydatetime(),
            ).delete()

            self.logger.info(
                "Inserting completeness entry:\n\t\t"
                + f"mission id {processing_request.mission_id}, data type {processing_request.data_type}\n\t\t"
                + f"science zone times: {str(start_time)} - {str(end_time)}\n\t\t"
                + f"completeness: {obtained} / {estimated_total}"
            )

            self.session.add(
                models.ScienceZoneCompleteness(
                    mission_id=processing_request.mission_id,
                    data_type=processing_request.data_type,
                    sz_start_time=str(start_time),
                    sz_end_time=str(end_time),
                    num_received=obtained,
                    num_expected=estimated_total,
                    insert_date=str(dt.datetime.now()),
                )
            )

        self.session.flush()
        self.session.commit()

        return True

    def split_science_zones(self, times):
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

    def get_median_diff(self, szs):
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
        median_diff
            The median diff (TODO: Check the type)
        """
        if self.completeness_config.median_diff is not None:
            return self.completeness_config.median_diff

        diffs = []
        for sz in szs:
            diffs += [(j - i).total_seconds() for i, j in zip(sz[:-1], sz[1:])]

        if not diffs:
            return None

        return statistics.median(diffs)

    def estimate_time_range(self, processing_request, sz):
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
