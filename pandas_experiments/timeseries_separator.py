from collections import Counter
import pandas as pd
from defaults.log import log, logging_levels

log.level = logging_levels[1]


def separator_v1(pd_df, chunk_size):

    if not isinstance(pd_df, pd.DataFrame):
        raise TypeError(f"wrong type of variable 'pd_series':{type(pd_df)}")

    grouped = pd_df.groupby("dt") if pd_df.size else []

    arr = []
    chunk = pd.DataFrame()

    for idx, frame in grouped:
        log.debug(f"idx : {idx}, size = {frame.size}")
        chunk = pd.concat([chunk, pd.DataFrame(frame)]).reset_index(drop=True)['dt']
        log.debug(chunk)

        if chunk.size >= chunk_size:
            arr.append(chunk)
            chunk = pd.DataFrame()

    if chunk.size:
        arr.append(chunk)

    return arr


def separator_v2(pd_df: pd.DataFrame, chunk_size: int):
    if not isinstance(pd_df, pd.DataFrame):
        raise TypeError(f"wrong type of variable 'pd_series':{type(pd_df)}")

    c = Counter(pd_df['dt'].to_list())

    arr = []
    cur_chunk = pd.Series()

    for i, x in enumerate(c):
        new_ser = pd.Series([x]*c[x]).dt.strftime('%Y-%m-%d %H:%M:%S')
        cur_chunk = pd.concat([cur_chunk, new_ser], ignore_index=True)

        if cur_chunk.size >= chunk_size:
            log.debug(cur_chunk)

            arr.append(cur_chunk)
            cur_chunk = pd.Series()

    if cur_chunk.size:
        arr.append(cur_chunk)

    return arr








