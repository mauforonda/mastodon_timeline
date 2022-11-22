#!/usr/bin/env python3

import pandas as pd
import datetime as dt
import pyarrow.parquet as pq
import pyarrow as pa
# import numpy as np
import json

TIMELINE = 'data/timeline.parquet'
HOURS_AGO = [6, 12, 24, 72]
DATE_FORMAT = '%Y-%m-%d %H:%M'
# AGGREGATE_SINCE = dt.datetime(2022,11,21).date()

df = pq.read_table(TIMELINE).to_pandas()

def describe_all(df):
    return df.groupby('updated_at')[['users', 'statuses']].sum()

def describe_instances(df):

    def get_times(df):
        times = pd.DataFrame({'time': df.updated_at.unique()})
        latest = times.time.max()
        timedata = {0: latest}
        for h in HOURS_AGO:
            times[f'ago_{h}'] = times.time.apply(lambda x: x - (latest - dt.timedelta(hours=h)))
        for h in HOURS_AGO:
            timedata[h] = times[abs(times[f'ago_{h}']) == abs(times[f'ago_{h}']).min()].time.tolist()[0]
        return timedata
    
    times = get_times(df)
    instances = [df[df.updated_at == df.updated_at.max()][['name', 'users', 'statuses']].set_index('name')]
    dfi = df.pivot_table(index='name', columns='updated_at', values='users')

    for h in HOURS_AGO:
        instances.append(
            (dfi[times[0]] - dfi[times[h]]).rename(index=h)
        )

    instances = pd.concat(instances, axis=1)
    instances = instances[instances.users > 0].sort_values('users', ascending=False)
    
    return instances

def describe_each(df):
    for i, g in df.groupby('name'):
        g.sort_values('updated_at')[['updated_at', 'users', 'statuses']].to_csv(f'data/instance/{i}.csv', index=False, date_format=DATE_FORMAT)

def describe_concentration(df):
    def estimate_gini(users):
        u = users.to_numpy()
        u = np.sort(u)
        n = len(u)
        i = np.arange(1, n + 1)
        return ((2 * i - n - 1) * u).sum() / (n * u.sum())
    dfi = df[df.updated_at.dt.date > AGGREGATE_SINCE].copy()
    gini = dfi.groupby('updated_at').users.apply(estimate_gini)
    gini.to_csv('data/gini.csv', date_format=DATE_FORMAT)

def describe_domain(df):
    domain = {'start': df.updated_at.min().strftime(DATE_FORMAT),
              'end': df.updated_at.max().strftime(DATE_FORMAT)}
    with open('data/timedomain.json', 'w+') as f:
        json.dump(domain, f)
    
mastodon = describe_all(df)
mastodon.to_csv('data/mastodon.csv', date_format=DATE_FORMAT)
instances = describe_instances(df)
instances.to_csv('data/instances.csv', date_format=DATE_FORMAT)
describe_each(df)
# describe_concentration(df)
describe_domain(df)
