#!/usr/bin/env python3

import pandas as pd
import datetime as dt
import pyarrow.parquet as pq
import pyarrow as pa

TIMELINE = 'data/timeline.parquet'
hours_ago = [6, 12, 24, 72, 168]
date_format = '%Y-%m-%d %H:%M'

df = pq.read_table(TIMELINE).to_pandas()

def describe_all(df):
    return df.groupby('updated_at')[['users', 'statuses']].sum()

def describe_instances(df):

    def get_times(df):
        times = pd.DataFrame({'time': df.updated_at.unique()})
        latest = times.time.max()
        timedata = {0: latest}
        for h in hours_ago:
            times[f'ago_{h}'] = times.time.apply(lambda x: x - (latest - dt.timedelta(hours=h)))
        for h in hours_ago:
            timedata[h] = times[abs(times[f'ago_{h}']) == abs(times[f'ago_{h}']).min()].time.tolist()[0]
        return timedata
    
    times = get_times(df)
    instances = [df[df.updated_at == df.updated_at.max()][['name', 'users', 'statuses']].set_index('name')]
    dfi = df.pivot_table(index='name', columns='updated_at', values='users')

    for h in hours_ago:
        instances.append(
            (dfi[times[0]] - dfi[times[h]]).rename(index=h)
        )

    instances = pd.concat(instances, axis=1)
    instances = instances[instances.users > 0].sort_values('users', ascending=False)
    
    return instances

def describe_each(df):
    for i, g in df.groupby('name'):
        g.sort_values('updated_at')[['updated_at', 'users', 'statuses']].to_csv(f'data/instance/{i}.csv', index=False, date_format=date_format)

mastodon = describe_all(df)
mastodon.to_csv('data/mastodon.csv', date_format=date_format)
instances = describe_instances(df)
instances.to_csv('data/instances.csv', date_format=date_format)
describe_each(df)

