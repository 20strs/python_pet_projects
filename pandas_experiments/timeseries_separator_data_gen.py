from random import randint
from datetime import datetime, timedelta
import pandas as pd


def generate_timeseries_ds(start_date='2024-01-01 00:00:00', records_size=100, random=True):
    print("generate timeseries")
    result_df = pd.DataFrame()

    start_dt = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')

    if random:
        while True:
            dif = randint(1, 100)

            date_range = pd.date_range(
                start=start_dt.strftime('%Y-%m-%d %H:%M:%S'),
                end=(start_dt + timedelta(seconds=dif)).strftime('%Y-%m-%d %H:%M:%S'),
                freq="s"
            )

            new_df = pd.DataFrame(
                {"dt": date_range.repeat(randint(1, 100))}
            )

            if result_df.size + new_df.size < records_size:
                result_df = pd.concat([result_df, new_df])

            else:
                result_df = pd.concat([result_df, new_df.head(records_size-int(result_df.size))])
                break

            start_dt = start_dt + timedelta(seconds=dif+1)
    else:
        ...

    return result_df


def generate_default_timeseries_ds():
    date_range = pd.date_range(
        start='2023-01-01 00:00:01',
        end='2023-01-01 00:00:02',
        freq="s"
    )

    new_df = pd.DataFrame(
        {"dt": date_range.repeat(2)}
    )

    date_range = pd.date_range(
        start='2023-01-01 00:00:02',
        end='2023-01-01 00:00:03',
        freq="s"
    )
    df2 = pd.DataFrame({"dt": date_range})

    return pd.concat([new_df, df2])