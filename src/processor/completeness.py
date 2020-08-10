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


class CompletenessUpdater:
    def __init__(self, session, completeness_config):
        self.session = session
        self.completeness_config = completeness_config
        self.logger = logging.getLogger(f"CompletenessUpdater")

    def update_completeness_table(self, times):
        """
        Update ScienceZoneCompleteness table, if possible

        Input: series of sorted, not-none times

        TODO: update to take in CompletenessConfig
        """
        # Edge case: empty Series
        if times.shape[0] == 0:
            self.logger.warning("Empty Time Series, cannot update completeness table")
            return

        # Split szs
        szs = []
        prev_time = times.iloc[0]
        sz = [prev_time]
        for i in range(1, times.shape[0]):
            cur_time = times.iloc[i]
            if cur_time - prev_time > dt.timedelta(minutes=20):
                szs.append([x for x in sz])
                sz = [cur_time]
            else:
                sz.append(cur_time)
            prev_time = cur_time
        szs.append(sz)
        self.logger.info(f"Found {len(szs)} science zone{s_if_plural(szs)}")

        # Get median diff
        if not median_diff:
            diffs = []
            for sz in szs:
                diffs += [(j - i).total_seconds() for i, j in zip(sz[:-1], sz[1:])]
            median_diff = median(diffs)

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
                    models.TimeIntervals.mission_id == self.mission_id,
                    models.TimeIntervals.interval_type == "ExecutionTime",
                    models.Intent.intent_type == intent_type,
                )
                .join(models.Intent, models.TimeIntervals.intent_id == models.Intent.id)
                .first()
            )
            if not q:
                self.logger.warning(f"Empty Query, skipping interval {sz_start_time} to {sz_end_time}")
                continue

            start_time = min(sz_start_time, q.start_time + start_delay + start_margin)
            end_time = max(sz_end_time, q.start_time + start_delay + expected_collection_duration)
            collection_duration = (end_time - start_time).total_seconds()

            # Get Percent Completeness
            obtained = len(sz)
            estimated_total = ceil(collection_duration / median_diff)
            percent_completeness = 100 * obtained / estimated_total

            # Remove previous entries that correspond to this new entry
            self.session.query(models.ScienceZoneCompleteness).filter(
                models.ScienceZoneCompleteness.mission_id == self.mission_id,
                models.ScienceZoneCompleteness.idpu_type == idpu_type,
                models.ScienceZoneCompleteness.data_type == data_type,
                models.ScienceZoneCompleteness.sz_start_time <= sz_end_time.to_pydatetime(),
                models.ScienceZoneCompleteness.sz_end_time >= sz_start_time.to_pydatetime(),
            ).delete()

            entry = models.ScienceZoneCompleteness(
                mission_id=self.mission_id,
                idpu_type=idpu_type,  # TODO: Need to deprecate this column at some point
                data_type=data_type,
                sz_start_time=str(start_time),
                sz_end_time=str(end_time),
                # TODO: Need to deprecate this column at some point
                completeness=float(percent_completeness),
                num_received=obtained,
                num_expected=estimated_total,
                insert_date=str(dt.datetime.now()),
            )

            self.session.add(entry)
            self.session.commit()
