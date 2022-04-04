"""Utility functions to merge Downlink DataFrames"""
import logging
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
    df1, df2 : pd.DataFrame
        DataFrames that have been formatted by the `format_downlinks` method

    Returns
    -------
    Optional[int]
        Offset if valid offset found, None if not
    """
    logger = logging.getLogger(calculate_offset.__name__)

    s1 = df1["data"]
    s2 = df2["data"]
    half_min = min(s1[s1.notnull()].shape[0], s2[s2.notnull()].shape[0]) / 2

    # First, just check offset of 0 because it is fairly common
    # TODO: Try checking any offsets that were used before, not just 0
    if check_zero_offset(s1, s2, half_min):
        return 0

    # Keep track of the difference between packets in s1 and s2 that are the same
    # If any difference is found the majority of the time (gets to >50% first), we can just use that
    offset_frequencies: defaultdict = defaultdict(int)
    for ind1 in s1[s1.notnull()].index:
        for ind2 in s2[s2 == s1[ind1]].index:
            cur_offset = ind1 - ind2
            offset_frequencies[cur_offset] += 1
            if offset_frequencies[cur_offset] > half_min:
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


def check_zero_offset(s1, s2, half_min):
    count = 0
    ind1_limit = s2.shape[0] - 1
    for ind1 in s1[s1.notnull()].index:
        if ind1 > ind1_limit:
            break
        if s1[ind1] == s2[ind1] and s1[ind1]:
            count += 1
            if count > half_min:
                return True
    return False


def merge_downlinks(sf1: pd.DataFrame, sf2: pd.DataFrame, offset: int) -> pd.DataFrame:
    """Merges two downlinks together, given an offset amount.

    # TODO: Sort downlinks to merge by denominator, better merges!

    Parameters
    ----------
    sf1, sf2 : pd.DataFrame
        Formatted Pandas DataFrames with science data. These DataFrames are
        expected to overlap (at least one row from sf1 must 'match' with a row
        from sf2)
    offset : int
        The offset between DataFrames, where sf1[i + offset] aligns with
        sf2[i] for all i

    Returns
    -------
    pd.DataFrame
        The merged DataFrame, with the same columns and updated numerator and
        denominator values
    """
    # For simplicity, this ensures that the DataFrame bound to sf1 does not start after sf2
    if offset < 0:
        sf1, sf2 = sf2, sf1
        offset = -1 * offset

    # Anything in sf1 that comes before the start of sf2
    df_1 = sf1.iloc[:offset]

    num_overlap = min(sf1.shape[0] - offset, sf2.shape[0])
    sf1_overlap = sf1.iloc[offset : (offset + num_overlap)]
    sf2_overlap = sf2.iloc[:num_overlap]

    def pick_overlapping_rows(index: int) -> bool:
        a = sf1_overlap.iloc[index]
        b = sf2_overlap.iloc[index]

        # If only one has data, use that
        if pd.isnull(b["data"]):
            # If a["data"] is null, then it does not matter which one we return - return first arbitrarily
            return a
        elif pd.isnull(a["data"]):  # implicitly, b["data"] is not null
            return b

        # At this point, we know both have some data

        # TODO: we should add better checking to see if the data is similar or completely different,
        # which could give a better idea as to whether data differs due to a few bit flips or to improper
        # DataFrame creation.
        if a["data"] != b["data"]:
            temp_logger = logging.getLogger(pick_overlapping_rows.__name__)
            temp_logger.warning(
                f"🛑\tCONFLICTING DATA between packets with packet_id {a['packet_id']} and {b['packet_id']}"
            )

        # If only one has an idpu time, use that (similar pattern as above)
        if pd.isnull(b["idpu_time"]):
            # TODO: What if a["idpu_time"] is also null? Probably return whichever was downlinked first.
            # I'm leaving this as is, to keep the behavior consistent with original implementation.
            return a
        elif pd.isnull(a["idpu_time"]):
            return b

        # Both have data and idpu times, so pick whichever was downlinked first (less likely to have been corrupted)
        return a if a["timestamp"] < b["timestamp"] else b

    # Any overlap between sf1 and sf2
    df_2 = pd.Series(range(num_overlap)).apply(pick_overlapping_rows)

    # If sf2 extends past sf1
    df_3 = sf2.iloc[num_overlap:]

    # If sf1 extends past sf2
    df_4 = sf1.iloc[(offset + sf2.shape[0]) :]

    merged = df_1.append(df_2)
    merged = merged.append(df_3)
    merged = merged.append(df_4).reset_index(drop=True)

    merged.loc[:, "denominator"] = int(merged.shape[0] - 1)
    merged["numerator"] = merged.index

    return merged
