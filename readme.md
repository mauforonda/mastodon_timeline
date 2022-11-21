User and status counts for each mastodon instance from [instances.social](https://instances.social) queried every 3 hours since November 4, 2022.

- All data is consolidated in [timeline.parquet](data/timeline.parquet)
- Some convenient csv views:
  - [a timeseries for all users and statuses](data/mastodon.csv)
  - [the latest count for each instance](data/instances.csv)
  - [a timeseries for each instace](data/instance)
  
Some notes:
- On November 21, 2022, [instances.social](https://github.com/TheKinrar/instances/) changes how it queries and filters instances. Some instances that were long classified as dead come back to life, many are new while others are clearly duplicated. I've tried to filter out most of this noise, but the data will still show a sudden jump in about a million users. 
