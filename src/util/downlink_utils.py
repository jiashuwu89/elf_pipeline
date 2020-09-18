"""Utility functions to merge Downlink DataFrames"""
import logging
import multiprocessing
from collections import defaultdict
from typing import Optional

import pandas as pd


def calculate_offset(df1: pd.DataFrame, df2: pd.DataFrame) -> Optional[int]:
    """Calculates offset required to merge 2 formatted downlink DataFrames.

    Offset is calculated such that df1[i + offset] aligns with df2[i]. We either
    take the most common such offset, or else state that there is no offset if
    there is no majority (> half), or we could not find a good offset

    Parameters
    ----------
    df1, df2
        DataFrames that have been formatted by the `format_downlinks` method

    Returns
    -------
    int or None
        Offset if valid offset found, None if not
    """
    logger = logging.getLogger(calculate_offset.__name__)

    s1 = df1["data"]
    s2 = df2["data"]

    # First, just check offset of 0 because it is fairly common
    # TODO: Try checking any offsets that were used before, not just 0
    half_min = min(s1[s1.notnull()].shape[0], s2[s2.notnull()].shape[0]) / 2
    count = 0
    ind1_limit = s2.shape[0] - 1
    for ind1 in s1[s1.notnull()].index:
        if ind1 > ind1_limit:
            break
        if s1[ind1] == s2[ind1] and s1[ind1]:
            count += 1
            if count > half_min:
                return 0

    offset_frequencies: defaultdict = defaultdict(int)
    indexes1 = s1[s1.notnull()].index

    # Keep track of the difference between packets in s1 and s2 that are the same
    # If any difference is found the majority of the time (gets to >50% first), we can just use that
    for ind1 in indexes1:
        indexes2 = s2[s2 == s1[ind1]].index
        for ind2 in indexes2:
            cur_offset = ind1 - ind2
            offset_frequencies[cur_offset] += 1
            if offset_frequencies[cur_offset] > half_min:
                # logger.info(f"calculate_offset info: majority found early: {cur_offset}")
                return cur_offset

    if len(offset_frequencies.items()) == 0:
        logger.warning(
            f"Could not find an offset! Good packets found:\t{list(s1.notnull()).count(True)}\t\t\
                {list(s2.notnull()).count(True)}"
        )
        return None

    # get the most frequent offset, and use that to merge both lists
    offset, count = None, -1
    for o, c in offset_frequencies.items():
        if c > count:
            offset, count = o, c
    # logger.info(f"calculate_offset info: {offset} with count {count} from offset_frequencies {offset_frequencies}")

    # Old code to find the most common offset - from when count was still checked by ScienceProcessor, not this function
    # offset, count = sorted(offset_frequencies.items(), reverse=True, key=lambda tup: tup[1])[0]

    # Warn if counts are low
    if count < min(len(df1.dropna()), len(df2.dropna())) * 0.05:
        logger.warning(
            f"Low counts found when merging downlinks: count of {count}, vs lengths {len(df1.dropna())} \
                {len(df2.dropna())}"
        )
    logger.debug(
        f"The offset {offset} had a count of {count}. Number of packets in each df: {len(df1.dropna())} \
            {len(df2.dropna())}"
    )

    # Edge Case: Not enough confidence in our offset
    # TODO: Should we raise this requirement higher?
    if count < 1:
        logger.warning("Less than 1 count, cannot find good enough offset")
        return None

    # Edge Case: Multiple offsets had maximum count, so don't return an offset!
    if list(offset_frequencies.values()).count(count) > 1:
        logger.warning(
            "Could not find a single unique offset with maximum count. "
            + f"Multiple offsets had count {count}, so prevent merging"
        )
        return None

    return offset


def merge_downlinks(sf1: pd.DataFrame, sf2: pd.DataFrame, offset: int) -> pd.DataFrame:
    """Merges two downlinks together, given an offset amount

    Steps
    - Start with sf1 and sf2
    - Look through sf2[:overlap] for not null
    - Append sf2[overlap:] to sf1

    Parameters
    ----------
    sf1, sf2 : pd.DataFrame
        Formatted Pandas DataFrames with science data
    offset : int
        The offset between DataFrames, where sf1[i + offset] aligns with
        sf2[i] for all i

    Returns
    -------
    pd.DataFrame
        The merged dataframe, of the same format

    """
    # Make sure sf1 occurs before sf2
    if offset < 0:
        sf1, sf2 = sf2, sf1
        offset = -1 * offset

    # Anything in sf1 that doesn't overlap
    df_1 = sf1.iloc[:offset]

    # Any overlap between sf1 and sf2
    overlap = min(sf1.shape[0] - offset, sf2.shape[0])
    with multiprocessing.Pool() as pool:
        x = [row for _, row in sf1.iloc[offset : (offset + overlap)].iterrows()]
        y = [row for _, row in sf2.iloc[:overlap].iterrows()]
        args = zip(x, y)
        df_2 = pd.concat(pool.starmap(merge_helper, args), ignore_index=True)

    # Anything in sf2 that doesn't overlap
    df_3 = sf2.iloc[overlap:]

    # If sf1 extends past sf2
    df_4 = sf1.iloc[(offset + sf2.shape[0]) :]

    merged = df_1.append(df_2)
    merged = merged.append(df_3)
    merged = merged.append(df_4).reset_index(drop=True)

    merged.loc[:, "denominator"] = int(merged.shape[0] - 1)
    merged["numerator"] = merged.index

    return merged


def merge_helper(a, b):
    """
    Helper function for multiprocessing portion of merge_downlinks
    Format of a, b:
    id, mission_id, idpu_type, idpu_time, data, numerator, denominator, packet_id, packet_data, timestamp
    """
    if a["data"] is None and b["data"] is None:
        to_return = a
    elif a["data"] is not None and b["data"] is None:
        to_return = a
    elif a["data"] is None and b["data"] is not None:
        to_return = b
    else:  # both have some data
        if a["data"] != b["data"]:
            _merge_helper_log = logging.getLogger(merge_helper.__name__)
            _merge_helper_log.info(
                f"WARNING: CONFLICTING DATA, keeping data with earlier timestamp (less likely to have been corrupted):\
                    \n\tPacket ID: {a['packet_id']}, Timestamp: {a['timestamp']}\tPacket ID: {b['packet_id']}, \
                    Timestamp: {b['timestamp']}"
            )
        if pd.isnull(b["idpu_time"]):  # also if neither has a timestamp
            to_return = a
        elif pd.isnull(a["idpu_time"]):
            to_return = b
        # Both have an idpu_time (compression time), so pick whichever was downlinked first
        else:
            if a["timestamp"] < b["timestamp"]:
                # if a['idpu_time'] > b['idpu_time']:  # <- Use this to check if 2019-10-09 overlaps with 2019-09-30
                to_return = a
            else:
                to_return = b

    return to_return.to_frame().T
