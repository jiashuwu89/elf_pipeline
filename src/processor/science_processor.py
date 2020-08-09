import datetime as dt
import logging
from abc import ABC
from spacepy import pycdf
import pandas as pd

from util.constants import MISSION_DICT, MASTERCDF_DIR
from util.completeness import CompletenessConfig
from util.science_utils import dt_to_tt2000, s_if_plural


class ScienceProcessor(ABC):
    """
    Base class used for all data product processing from the database.
    Implements some basic functionalities common to all data products.
    """

    def __init__(self, session, output_dir, processor_name):

        self.session = session
        self.output_dir = output_dir

        self.downlink_manager = DownlinkManager(session)
        self.logger.er = logging.getLogger(f'science.processor.{processor_name}')

    def generate_files(self, processing_request):
        l0_file_name, l0_df = self.generate_l0_products(processing_request)
        l1_file_name, l1_df = self.generate_l1_products(processing_request, l0_df)

        return [l0_file_name, l1_file_name]

    def generate_l0_products(self, processing_request):
        self.logger.info('>>> Generating Level 0 Products...')
        l0_df = self.generate_l0_df(processing_request)
        l0_file_name = self.generate_l0_file(processing_request, l0_df.copy())
        return l0_file_name, l0_df

    def generate_l0_df(self, processing_request):
        """
        Generate a dataframe of processed level 0 data given a specific collection date.

        All relevant downlinks are fetched, merged, and concatenated, and then
        passed separately (as a list) through process_l0.
        Finally, the individual dataframes are merged and duplicates/empty packets dropped.
        """
        self.logger.info(f"Creating level 0 DataFrame: {processing_request.to_string()}")

        # Get Downlinks by collection time
        # Downlink Format: mission id, idpu_type, first/last downlink time, first/last id, first/last collection time
        list_of_downlinks = self.get_relevant_downlinks(processing_request.date)

        self.logger.info("Relevant downlinks:")
        for _, p, ft, lt, d, fid, lid, _, _ in list_of_downlinks:
            self.logger.info(f"-- {p} {str(ft)} - {str(lt)} [{fid}-{lid}]")

        self.logger.info(f"Initial merging of {len(list_of_downlinks)} downlink{s_if_plural(list_of_downlinks)}...")
        merged_dfs = self.get_merged_dataframes(list_of_downlinks)
        self.logger.info(f"✔️ Merged to {len(merged_dfs)} downlink{s_if_plural(merged_dfs)}")

        self.logger.info("Rejoining frames into packets...")
        rejoined_dfs = [self.rejoin_data(df) for df in merged_dfs]
        self.logger.info("✔️ Done rejoining frames")

        self.logger.info("Processing Level 0 packets...")
        dfs = [self.process_rejoined_data(df) for df in rejoined_dfs]
        self.logger.info("✔️ Done processing Level 0 packets")

        self.logger.info("Final merge...")
        df = self.merge_processed_dataframes(dfs)
        self.logger.info("✔️ Done with final merge")

        if df.shape[0] == 0:
            raise RuntimeError(f"Final Dataframe is empty: {processing_request.to_string()}")

        return df

    def generate_l0_file(self, processing_request, l0_df):
        # Filter fields and duplicates
        l0_df = l0_df[["idpu_time", "data"]]
        l0_df = l0_df.drop_duplicates().dropna()

        # Select Data belonging only to a certain day
        l0_df = l0_df[
                    (l0_df['idpu_time'] >= processing_request.date) &
                    (l0_df['idpu_time'] < processing_request.date + dt.timedelta(days=1))]

        # TT2000 conversion
        l0_df['idpu_time'] = l0_df['idpu_time'].apply(pycdf.lib.datetime_to_tt2000)

        if l0_df.shape[0] == 0:
            raise RuntimeError(f"Empty level 0 DataFrame: {processing_request.to_string()}")

        # Generate L0 file
        fname = self.make_filename(0, processing_request.date, size=l0_df.shape[0])
        l0_df.to_csv(fname, index=False)

        return fname, l0_df

    # Input: series of sorted, not-none times
    def update_completeness_table(self, times, config):
        '''
        Update ScienceZoneCompleteness table, if possible

        May be overriden in derived classes

        TODO: update to take in CompletenessConfig
        '''
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
                diffs += [(j-i).total_seconds() for i, j in zip(sz[:-1], sz[1:])]
            median_diff = median(diffs)

        # Update completeness for each science zone
        for sz in szs:
            sz_start_time = sz[0]
            sz_end_time = sz[-1]

            # Find corresponding collection (for the time range)
            # Assumes that only one execution time will be found
            q = self.session.query(models.TimeIntervals).filter(
                    models.TimeIntervals.start_time <= sz_end_time.to_pydatetime(),
                    models.TimeIntervals.end_time >= sz_start_time.to_pydatetime(),
                    models.TimeIntervals.mission_id == self.mission_id,
                    models.TimeIntervals.interval_type == 'ExecutionTime',
                    models.Intent.intent_type == intent_type
                ).join(models.Intent, models.TimeIntervals.intent_id == models.Intent.id).first()
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
                models.ScienceZoneCompleteness.sz_end_time >= sz_start_time.to_pydatetime()
            ).delete()

            entry = models.ScienceZoneCompleteness(
                mission_id=self.mission_id,
                idpu_type=idpu_type,    # TODO: Need to deprecate this column at some point
                data_type=data_type,
                sz_start_time=str(start_time),
                sz_end_time=str(end_time),
                completeness=float(percent_completeness),   # TODO: Need to deprecate this column at some point
                num_received=obtained,
                num_expected=estimated_total,
                insert_date=str(dt.datetime.now())
            )

            self.session.add(entry)
            self.session.commit()

    def generate_l1_products(self, processing_request, l0_df=None):
        """
        Generates level 1 CDFs for given collection time, optionally provided
        a level 0 dataframe (otherwise, create_l0_df will be called again).

        Returns the path and name of the generated level 1 file.
        """
        l1_df = self.generate_l1_df(processing_request, l0_df)
        l1_file_name = self.generate_l1_file(processing_request, l1_df.copy())
        return l1_file_name, l1_df

    def generate_l1_df(self, processing_request, l0_df):
        if l0_df is None:
            l0_df = self.create_l0_df(processing_request.date)

        # Allow derived class to transform data
        l1_df = self.transform_l0(l0_df, processing_request.date)

        # Timestamp conversion
        try:
            l1_df = l1_df[
                (l1_df['idpu_time'] >= processing_request.date) &
                (l1_df['idpu_time'] < processing_request.date + dt.timedelta(days=1))]
            l1_df['idpu_time'] = l1_df['idpu_time'].apply(dt_to_tt2000)
            if l1_df.shape[0] == 0:
                raise RuntimeError(f"Final Dataframe is empty: {processing_request.to_string()}")

        except KeyError:
            self.logger.debug('The column \'idpu_time\' does not exist, but it\'s probably OK')

        return l1_df

    def generate_l1_file(self, processing_request, l1_df):
        fname = self.make_filename(1, processing_request.date)
        cdf = self.create_CDF(fname, l1_df)
        self.fill_CDF(1, cdf, l1_df)
        cdf.close()

        return fname, l1_df

    # Level 2 Generation
    def generate_l2_products(self):
        pass


    def get_relevant_downlinks(self, collection_date):
        """
        Gets a list of downlinks that occured for the collection date.
        This list has POSSIBLE downlinks based off of the list of data types.
        Filtering must be done externally if a downlink does not fit criteria of data product

        Returns:
        - tuple of form `(mission_id, packet_type, first_time, last_time, denom, first_id,
            last_id, first collection time, last collection time)`
        """

        queried_downlinks   = []
        start               = collection_date + dt.timedelta(microseconds=0)
        end                 = collection_date + dt.timedelta(days=1) - dt.timedelta(microseconds = 1)

        for data_type in self.idpu_types:
            queried_downlinks.extend(
                self.downlink_manager.fetch_downlinks_from_range(self.mission_id, data_type, start, end))

        if len(queried_downlinks) == 0:
            raise exceptions.EmptyError
        return queried_downlinks


    def get_merged_dataframes(self, downlinks):
        """
        Merge a list of downlinks, and retrieve their associated dataframes.
        Downlinks are merged only if they refer to the same physical range of
        packets onboard the MSP (they have matching IDPU_TYPE and overlapping IDPU_TIME)

        The resulting dataframe is sorted by:
        1. Packet Type
        2. IDPU Time

        Parameters:
        - downlinks: a tuple of the form `(mission_id, packet_type, first_time, last_time, denom,
            first_id, last_id, first_collect_time, last_collect_time)`

        Returns:
        - a tuple of: (concatenated dataframe, list of merged dataframes)
        """

        # Sort Downlinks by Downlink Time, and then by size
        downlinks = sorted(downlinks, key=lambda x: (x[2], x[6]-x[5]))
        if not downlinks:
            raise exceptions.EmptyError

        merged_downlinks = []
        _, m_ptype, m_first_time, m_last_time, _, _, _, _, _ = downlinks[0]
        m_df = self.downlink_manager.get_formatted_df_from_downlink(downlinks[0])

        for i,downlink in enumerate(downlinks[1:]):
            _, ptype, first_time, last_time, _, _, _, _, _ = downlink
            df = self.downlink_manager.get_formatted_df_from_downlink(downlink)

            # Merge if we have found a good offset (downlink overlaps with the current one and packet type matches)
            offset = self.downlink_manager.calculate_offset(m_df,df)
            if ptype == m_ptype and offset != None and m_first_time <= first_time <= m_last_time:
                m_last_time     = max(m_last_time, last_time)
                m_df            = self.downlink_manager.merge_downlinks(m_df, df, offset)

            else:
                merged_downlinks.append(m_df)
                m_first_time    = first_time
                m_last_time     = last_time
                m_ptype         = ptype
                m_df            = df

        merged_downlinks.append(m_df)

        # Data Completeness Stuff
        for df in merged_downlinks:
            df['packet_id'] = df['packet_id'].apply(lambda x: [x])

        return merged_downlinks


    def rejoin_data(self, d):
        """
        Converts a dataframe of frames received from ELFIN into a dataframe of
        packets onboard the MSP's filesystem. This involves identifying the length
        of each packet, and concatenating consecutive frames into their respective packets.

        Frames partially composing an incomplete packet will be dropped, and a blank
        row will be inserted to identify that a packet is missing.
        """

        data = d['data'].apply(lambda x: None if pd.isnull(x) else bytes.fromhex(x))
        frames = d['packet_data'].apply(lambda x: None if pd.isnull(x) else bytes.fromhex(x))
        packet_ids = d['packet_id'] # Each item should be a list

        missing_numerators = []
        idpu_type = None
        denominator = 0

        final_df = pd.DataFrame()
        idx = 0

        while idx < d.shape[0]:

            numerator = d['numerator'].iloc[idx]
            cur_packets = packet_ids.iloc[idx]

            if not data.iloc[idx]:
                self.logger.debug(f"Dropping idx={idx}: Empty data")
                missing_numerators.append(numerator)
                idx += 1
                continue

            if not idpu_type: # get default data to give to missing frames
                idpu_type = d['idpu_type'].iloc[idx]
                denominator = d['denominator'].iloc[idx]

            cur_data    = data.iloc[idx]
            cur_frame   = frames.iloc[idx]
            cur_row     = d.iloc[idx].copy()

            current_length = len(cur_data)

            # Making sure this frame has a header
            try:
                # make sure the CRC is ok, then remove header
                if(_utils.compute_crc(0xFF, cur_frame[1:12]) != cur_frame[12]):
                    raise Exception(f"Bad CRC at {idx}")
                expected_length = int.from_bytes(cur_frame[1:3], 'little', signed=False)//2 - 12

            except Exception as e:
                self.logger.debug(f"Dropping idx={idx}: Probably not a header - {e}\n")
                missing_numerators.append(numerator)
                idx += 1
                continue

            try:
                while current_length < expected_length:
                    # Need to get more
                    idx += 1
                    cur_data += data.iloc[idx]
                    current_length = len(cur_data)

                    cur_packets += packet_ids.iloc[idx]

            except: # Missing packet (or something else)
                self.logger.debug(f"Dropping idx={idx}: Empty continuation\n")
                missing_numerators.append(numerator)
                idx += 1
                continue

            if current_length != expected_length:
                self.logger.debug(f"Dropping idx={idx}: Current and Expected length differ: {current_length} != {expected_length}\n")
                missing_numerators.append(numerator)
                idx += 1
                continue

            idx += 1

            # If we get this far, add to final_df
            cur_row.loc['data'] = cur_data.hex()
            cur_row.loc['packet_id'] = cur_packets
            final_df = final_df.append(cur_row)


        missing_frames = {
            'id':           None,
            'mission_id':   self.mission_id,
            'idpu_type':    idpu_type,
            'idpu_time':    None,
            'data':         None,
            'numerator':    pd.Series(missing_numerators),
            'denominator':  denominator,
            'packet_id':    None,
            'packet_data':  None,
            'timestamp':    None
        }

        final_df = final_df.append(
            pd.DataFrame(data=missing_frames), sort=False
        ).sort_values('numerator').reset_index(drop=True)

        return final_df[['timestamp', 'mission_id', 'idpu_type', 'idpu_time', 'numerator', 'denominator', 'data', 'packet_id']]

    def merge_processed_dataframes(self, dataframes):
        """
        Given a list of dataframes of identical format (decompressed/raw, level 0),
        merge them in a way such that duplicate frames are removed.

        Preference is given in the same order as which IDPU_TYPEs
        appear in the list self.idpu_types.
        """
        df = pd.concat(dataframes)

        df['idpu_type'] = df['idpu_type'].astype('category').cat.set_categories(self.idpu_types, ordered=True)
        df = df.dropna(subset=["data", "idpu_time"])
        df = df.sort_values(["idpu_time", "idpu_type"])

        # Keeping the first item means that the first/earlier idpu_type will be preserved
        # idpu_type is ordered in the same order as self.idpu_types
        df = df.drop_duplicates("idpu_time", keep='first')

        return df.reset_index()


    ################################
    # Abstract (Default) Functions #
    ################################
    def process_l0(self, data):
        """
        (Default implementation. Should be overridden in child class)
        Process a level 0 dataframe - this usually just involves decompressing it as needed.
        """
        return data

    def process_l1(self, df):
        pass

    def process_level_2(self, df):
        pass


class FileMaker:
    """ A class to make CSV and CDF files """

    def __init__(self, probe_name):
        self.probe_name = probe_name

    def make_filename(self, save_directory, data_product_name, level, collection_date, size = None):
        """Constructs the appropriate filename for a L0/L1/L2 file, and returns the full path

        Parameters
        ==========
        save_directory: str
        probe_name: str
        data_product_name: str
        level: int
        collection_date
        size
        """

        fname = self.probe_name + "_l" + str(level) + "_" + data_product_name + "_" + collection_date.strftime("%Y%m%d")
        if level == 0:
            if size is None:
                raise ValueError("No size given for level 0 naming")
            fname += "_" + str(size) + ".pkt"
        elif level == 1:
            fname += "_v01" + ".cdf"
        elif level == 2:
            pass
        else:
            raise ValueError(f"Bad level: {level}")
        return save_directory + "/" + fname


    def create_CDF(self, fname):
        """
        Gets or creates a CDF with the desired fname. If existing path is specified, it would check to see if the correct CDF exists.
        If it does not exist, a new cdf will be created with the master cdf.

        Parameters
        ==========
        fname: str
            a string that includes the  target file path along with the target file name of the desired file. The file name is of the data product format used.
        """
        fname_parts = fname.split("/")[-1].split("_")
        probe       = fname_parts[0]
        level       = fname_parts[1]
        idpu_type   = fname_parts[2]

        if os.path.isfile(fname):
            os.remove(fname)

        cdf = pycdf.CDF(fname,  MASTERCDF_DIR + f"{probe}_{level}_{idpu_type}_00000000_v01.cdf")
        return cdf


    def fill_CDF(self, cdf, cdf_fields, df):
        """ Inserts data from df into a CDF file

        Parameters
        ==========
        cdf
        cdf_fields
        df
        """
        for key in cdf_fields:
            if f"{self.probe_name}_{key}" in cdf.keys() and cdf_fields[key] in df.columns:
                data = df[cdf_map2_df[key]].values
                # numpy array with lists need to be converted to a multi-dimensional numpy array of numbers
                if isinstance(data[0], list):
                    data = np.stack(data)
                cdf[f"{self.probe_name}_{key}"] = data
