#!/usr/bin/env python3

import pandas as pd
import requests
import datetime as dt
import pyarrow.parquet as pq
import pyarrow as pa
import os

STATE_FILENAME = 'data/timeline.parquet'
DIRECTORY_FILENAME = 'data/directory.parquet'
TOKEN = os.environ['INSTANCES_SOCIAL_TOKEN'].strip()
NONSENSE_INSTANCES = [
    'you-think-your-fake-numbers-are-impressive.well-this-instance-contains-all-living-humans.lubar.me',
    'angelfire-1.glitch.me',
    'angelfire.glitch.me',
    'angelfire-2.glitch.me',
    'angelfire-3.glitch.me'
]

def type_instances(dfi):
    """
    Set the right types for relevant columns
    """
    
    for col in ['users', 'statuses']:
        dfi[col] = dfi[col].astype(int)
    dfi.updated_at = pd.to_datetime(dfi.updated_at)
    return dfi

def filter_instances(dfi):
    """
    Filter out instances that
    - are manually flagged as returning meaningless numbers, I'm looking at you `you-think-your-fake-numbers-are-impressive.well-this-instance-contains-all-living-humans.lubar.me`
    - are down
    - return negative `users` or `statuses`
    - return NaN update times
    - are duplicated after normalizing names, selecting the one with more users
    """
    
    dfi = dfi[~dfi.name.isin(NONSENSE_INSTANCES)]
    dfi = dfi[dfi["up"]]
    dfi = dfi[(dfi.users > 0) & (dfi.statuses > 0) & (dfi.updated_at.notna())]
    dfi = dfi[(dfi.updated_at > (now - dt.timedelta(hours=3))) & (dfi.updated_at < (now + dt.timedelta(hours=3)))]

    dfi.name = dfi.name.apply(lambda n: n.replace('/', '').strip())
    dfi.name = dfi.name.str.replace('^.+\@', '', regex=True)
    dfi.name = dfi.name.str.replace('\@', '', regex=True)
    dfi.name = dfi.name.str.lower()
    dfi = dfi.sort_values(['name', 'users']).drop_duplicates(subset=['name'], keep='last')

    return dfi

def get_instances():
    """
    Download instance data from instances.social
    """
    
    url = 'https://instances.social/api/1.0/instances/list?count=0'
    headers = {"Authorization": f'Bearer {TOKEN}'}
    response = requests.get(url, headers=headers, timeout=20)
    response = response.json()
    return pd.DataFrame(response['instances'])    

def prepare_state(dfi):
    """
    Select columns, collapse update times to a single value for better file compression
    """
    
    state = dfi[['name', 'users', 'statuses']]
    state.insert(1, 'updated_at', now.replace(second=0, microsecond=0))
    return state

def save_timeline(state):
    """
    Join the current with all previous states, remove duplicate entries and save as a parquet
    """
    
    old = pq.read_table(STATE_FILENAME).to_pandas()
    timeline = pd.concat([old, state])
    timeline = type_instances(timeline)
    timeline = timeline.sort_values(['name', 'updated_at', 'users']).drop_duplicates(subset=['name', 'updated_at'], keep='last')
    pq.write_table(pa.Table.from_pandas(timeline), STATE_FILENAME, use_deprecated_int96_timestamps=True)


instances = get_instances()
instances = type_instances(instances)
now = dt.datetime.now(dt.timezone.utc)
instances = filter_instances(instances)
state = prepare_state(instances)
save_timeline(state)
