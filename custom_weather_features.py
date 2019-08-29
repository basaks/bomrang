from datetime import timedelta
import numpy as np
import pandas as pd
from pathlib import Path
from google.cloud.bigquery import TableReference
from gcputils.bqclient import BQClient
from bq_upload_postcode_weather_summay import weather_data_types, RAINFALL, \
    MAX_TEMPERATURE, MIN_TEMPERATURE, SOLAR_EXPOSURE

POSTCODE = 'postcode'

bqclient = BQClient()

dataset_id = 'weather'
dataset_ref_ = bqclient.client.dataset(dataset_id)

all_postcodes_table = 'historical_weather'

hist_table_ref = TableReference(dataset_ref_, all_postcodes_table)

import pickle

if Path('historical_weather.pk').exists():
    weather = pickle.load(open('historical_weather.pk', 'rb'))
else:
    weather = bqclient.bq_to_df(hist_table_ref)
    pickle.dump(weather, open('historical_weather.pk', 'wb'))

all_postcodes_cmd_table = 'historical_weather_cmd'
hist_table_ref_cmd = TableReference(dataset_ref_, all_postcodes_cmd_table)

simple_ops = {
    '_avg': np.nanmean,
    '_sum': np.nansum,
    '_min': np.nanmin,
    '_max': np.nanmax
}

range_ops = {
    'range': lambda x, y: x - y,
}

to_be_inserted = []

for p in pd.unique(weather[POSTCODE]):
    p_weather = weather[weather[POSTCODE] == p]
    # move -9 days for resampling with `right` labels to match the sunday prior
    # this is appropriate for supers
    # supers model data must come from the Sunday 2 days prior
    p_weather.index = pd.DatetimeIndex(p_weather.date - timedelta(days=9))
    p_weather.sort_index(inplace=True)
    p_weather['temp_range'] = p_weather[MAX_TEMPERATURE] - \
        p_weather[MIN_TEMPERATURE]

    weather_data_types.append('temp_range')
    p_weather_ref_dt = pd.DataFrame()
    for w in weather_data_types:
        for k in simple_ops.keys():
            p_weather_ref_dt[w + k] = p_weather[w].astype(
                float).resample(
                rule='W', label='right', closed='right').apply(simple_ops[k])

    to_be_inserted.append(p_weather_ref_dt)

final_df = pd.concat(to_be_inserted)

import IPython; IPython.embed(); import sys; sys.exit()

# write table into BQ
# bqclient.df_to_bq(final_df, hist_table_ref_cmd)
