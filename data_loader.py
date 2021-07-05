from dask import dataframe as ddf


def read_merge_csv() -> ddf.DataFrame:
    requests_df = ddf.read_csv("data/requests.csv",
                               header=None,
                               names=["timestamp_request", "session_id", "partner", "user_id", "bid", "win"],
                               blocksize=1000000,
                               )
    clicks_df = ddf.read_csv("data/clicks.csv",
                             header=None,
                             names=["timestamp_clicks", "session_id", "time"],
                             blocksize=1000000,
                             )
    impressions_df = ddf.read_csv("data/impressions.csv",
                                  header=None,
                                  names=["timestamp_impressions", "session_id", "duration"],
                                  blocksize=1000000,
                                  )
    merged = ddf.merge(requests_df, clicks_df, how='left', on='session_id')
    merged = ddf.merge(merged, impressions_df, how='left', on='session_id')
    merged = merged.categorize(columns=['session_id', 'user_id'])
    return merged


def get_users_pivot():
    users_pivot_df = read_merge_csv()
    users_pivot_df["actual_bid"] = users_pivot_df.apply(
        lambda x: x['bid'] if x['win'] is True else None, axis=1, meta='float64')
    users_pivot_final = users_pivot_df.groupby('user_id').agg(
        {'session_id': 'count', 'duration': 'count', 'timestamp_clicks': 'count', 'actual_bid': 'mean', 'time': 'max'})
    print("You've entered the user pivot")
    users_pivot_final = users_pivot_final.compute()
    cols = {'session_id': 'num_of_requests', 'duration': 'num_of_impressions', 'timestamp_clicks': 'num_of_clicks',
            'actual_bid': 'avg_bid_price', 'time': 'max_time_till_click'}
    return users_pivot_final.rename(columns=cols)


def get_session_pivot():
    session_pivot_df = read_merge_csv()
    session_pivot_df["finish"] = session_pivot_df.apply(
        lambda x: max(x['timestamp_request'], x['timestamp_clicks'], x['timestamp_impressions']), axis=1, meta='float64')
    session_pivot_final = session_pivot_df.groupby('session_id').agg(
        {'timestamp_request': 'first', 'finish': 'first', 'partner': 'first'})
    session_pivot_final = session_pivot_final.compute()
    cols = {'timestamp_request': 'begin', 'partner': 'partner_name'}
    return session_pivot_final.rename(columns=cols)

