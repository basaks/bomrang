from datetime import timedelta
from itertools import chain
import numpy as np
import pandas as pd
from pathlib import Path
from joblib import Parallel, delayed
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
weather_file_on_disc = 'historical_weather_bq.pk'
if Path(weather_file_on_disc).exists():
    weather = pickle.load(open(weather_file_on_disc, 'rb'))
else:
    weather = bqclient.bq_to_df(hist_table_ref)
    pickle.dump(weather, open(weather_file_on_disc, 'wb'))

all_postcodes_cmd_table = 'historical_weather_cmd'
hist_table_ref_cmd = TableReference(dataset_ref_, all_postcodes_cmd_table)

simple_ops = {
    '_avg': np.nanmean,
    '_min': np.nanmin,
    '_max': np.nanmax
}


def _calc_per_postcode(p, weather):
    p_weather = weather[weather[POSTCODE] == p].copy()
    # move -9 days for resampling with `right` labels to match the sunday prior
    # this is appropriate for supers
    # supers model data must come from the Sunday 2 days prior
    p_weather.index = pd.DatetimeIndex(p_weather.date - timedelta(days=9))
    p_weather.sort_index(inplace=True)
    p_weather['temp_range'] = p_weather[MAX_TEMPERATURE] - \
                              p_weather[MIN_TEMPERATURE]

    weather_data_types.append('temp_range')
    _w_ref_dt = pd.DataFrame()
    print(f"Calculating weather derivatives for postcode {p}")
    for w in weather_data_types:
        for k in simple_ops.keys():
            _w_ref_dt[w + k] = p_weather[w].astype(np.float32).resample(
                rule='W', label='right', closed='right').apply(simple_ops[k])

    _w_ref_dt[POSTCODE] = p
    return _w_ref_dt


postcodes = pd.unique(weather[POSTCODE])
processes = 10


def _calc_per_postcode_wrapper(proc):
    this_p_postcodes = np.array_split(postcodes, processes)[proc]
    # give every process it's own `weather`
    weather = pickle.load(open(weather_file_on_disc, 'rb'))
    p_weathers = [_calc_per_postcode(p, weather) for p in this_p_postcodes]
    return p_weathers


to_be_inserted = Parallel(n_jobs=processes)(delayed(
    _calc_per_postcode_wrapper)(pr) for pr in range(processes))

flat_list = list(chain.from_iterable(to_be_inserted))

final_df = pd.concat(flat_list)

# introduce the (ref) date column
final_df['date'] = final_df.index.date


bqclient.df_to_bq(final_df, hist_table_ref_cmd)
