# radarstudy - RADAR processing library
A python processing and visualisation package for use with offline RADAR project data.

WIP.

# Installation
The easiest way to install is via PyPI:

```
pip3 install radarstudy
```

Additional dependencies may be useful, for example to read or write
parquet files fastparquet or pyarrow is required. These are largely
in line with dask's optional dependencies.

# Quickstart

By default, a folder structure following the standard RADAR output
folder is expected. I.e. a top-level project directory that contains
participant folders, each participant folder contains data topic
folders.

```python3
import radar
project = radar.Project(name='Example Project', path='path/to/project/dir')
```

Participants in that project folder are added automatically and can be
accessed through a participants dictionary attached to the project
object:

```python3
participant = project.participants['participant_name']
for p in project.participants:
    print(p.name)
```

Each participant has a data dictionary in a similar vein:

```python3
print(participant.data)
participant.data['data_topic_name']
```

Because RADAR data is stored in hourly CSV files, both reading those
files and even finding them in the first place can be time consuming.
As such, the value of the data dictionary is delayed.
Before it is first accessed (as above), only a path to the data folder
or file is stored. When accessed, the delayed computing and task
scheduling library [dask](https://dask.org/) is used to load
a delayed representation of the data - typically a dataframe with a 
datetime index.

The dask documentation may be useful to read. If you only want to load
data directly into memory, you should call .compute() on the object.
e.g.

```python3
participant.data['android_phone_acceleration'].compute()
```

Alternatively you can apply functions to it before calling compute,
and dask will create a graph that can be run across cores/clusters/etc.

e.g. 
```python3
ddf = participant.data['android_phone_acceleration'][['x', 'y', 'z']]
accel_10s_std = ddf.rolling(window='10s').std()
accel_10s_std.compute()
```
