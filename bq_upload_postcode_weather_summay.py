import logging
from collections import ChainMap
from itertools import compress
from typing import Dict, List
from scipy.spatial import cKDTree
from pathlib import Path
from joblib import Parallel, delayed
import numpy as np
import pandas as pd
from google.cloud.bigquery import TableReference
from gcputils.bqclient import BQClient


log = logging.getLogger(__name__)

RAINFALL = 'rainfall'
MIN_TEMPERATURE = 'min_temperature'
MAX_TEMPERATURE = 'max_temperature'
SOLAR_EXPOSURE = 'solar_exposure'

weather_data_types = [RAINFALL,
                      MIN_TEMPERATURE,
                      MAX_TEMPERATURE,
                      SOLAR_EXPOSURE]


def download_all_stations_data(stations: pd.DataFrame) -> \
        Dict[str, pd.DataFrame]:

    df_index = pd.DataFrame(index=pd.date_range('2016-01-01', '2019-08-29',
                                                freq='D'))
    sites = stations['site']

    def __inner(s, i, client):
        # check if weather station (data) exists in BQ
        # not all stations are active now/data may not be available
        # Supply NaN's for missing weather columns

        log.info(f"Download data for station: {i}, site: {s}")
        this_station_ref = TableReference(dataset_ref_, str(s))
        if client.table_exists(this_station_ref):
            df = client.bq_to_df(this_station_ref)
            # remove any null year/month/day, sometimes we get dirty data
            df = df[(~df['year'].isnull()) & (~df['month'].isnull()) & (~df[
                'day'].isnull())]
            df.index = df.apply(lambda x: pd.datetime(
                int(x['year']), int(x['month']), int(x['day'])), axis=1)

            for dt in weather_data_types:
                if dt not in df.columns:
                    df[dt] = np.nan

            df = df_index.join(df)
            return df[weather_data_types]
        return pd.DataFrame()  # else return empty dataframe

    def __process_inner(p, processes, sites):
        this_p_sites = np.array_split(sites, processes)[p]
        this_p_client = BQClient()
        return {s: __inner(s, i, this_p_client) for i, s in enumerate(
            this_p_sites)}

    processes = 30
    p_inners = Parallel(n_jobs=processes)(delayed(__process_inner)(
        p, processes, sites) for p in range(processes))

    return dict(ChainMap(*p_inners))


def __df_nan_mean(dfs: List[pd.DataFrame], weights: List[float]) \
        -> pd.DataFrame:

    # check empty table, i.e., this station data is not in BQ
    non_empty_dfs = [True if df.shape[0] else False for df in dfs]
    dfs = list(compress(dfs, non_empty_dfs))
    weights = list(compress(weights, non_empty_dfs))

    # make sure dfs contain same columns
    for df in dfs[1:]:
        assert set(dfs[0].columns).__eq__(set(df.columns))

    df_mean = pd.DataFrame({'date': dfs[0].index.date})

    # TODO: make sure at least 3 stations have data
    for c in dfs[0].columns:
        data = np.vstack([df[c] for df in dfs]).astype(np.float32)
        masked_data = np.ma.masked_array(data=data, mask=np.isnan(data),
                                         dtype=np.float32)
        avg_data = np.ma.average(masked_data, weights=weights, axis=0)
        df_mean[c] = avg_data.filled(np.nan)

    return df_mean


def _setup_logging():
    log = logging.getLogger('')
    log.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    log.addHandler(ch)
    return log


if __name__ == '__main__':

    log = _setup_logging()

    bqclient = BQClient()

    dataset_id = 'weather'
    dataset_ref_ = bqclient.client.dataset(dataset_id)

    stations_ref = TableReference(dataset_ref_, 'stations_site_list')

    # https://www.matthewproctor.com/australian_postcodes
    postcodes_ref = TableReference(dataset_ref_, 'australian_postcodes')

    stations = bqclient.bq_to_df(stations_ref)

    postcodes = bqclient.bq_to_df(postcodes_ref)

    kdtree = cKDTree(stations[['lat', 'lon']])

    weather_file_on_disc = 'historical_weather.pk'
    import pickle
    if Path(weather_file_on_disc).exists():
        historical_weather = pickle.load(open(weather_file_on_disc, 'rb'))
    else:
        historical_weather = download_all_stations_data(stations)
        pickle.dump(historical_weather, open(weather_file_on_disc, 'wb'))

    all_postcodes_table = 'historical_weather'

    hist_table_ref = TableReference(dataset_ref_, all_postcodes_table)

    if bqclient.table_exists(table_reference=hist_table_ref):
        log.info(f"Table {all_postcodes_table} exist. Deleting Table.")
        bqclient.client.delete_table(hist_table_ref, not_found_ok=True)
    else:
        log.info(f"Table {all_postcodes_table} does not exist. "
                 f"New table will be created")

    total_postcodes = postcodes.shape[0]

    already_inserted = set()

    to_be_inserted = []

    for i, p in enumerate(postcodes.itertuples()):
        try:
            lat_long = (float(p.lat), float(p.long))
            log.info(f"Interpolating data for postcode {p.postcode} and "
                     f"lat/long {lat_long}: {i + 1} of {total_postcodes}")
            if p.postcode in already_inserted:
                log.info(f"Data for postcode {p.postcode} already inserted")
                continue
            else:
                already_inserted.add(p.postcode)
                log.info(f"Interpolating data for postcode: {p.postcode}")

            # try 20 closest stations
            station_dists, stations_indices = kdtree.query(lat_long, k=25)
            selected_station_sites = stations.site[stations_indices]

            selected_historical_weather = [historical_weather[s]
                                           for s in selected_station_sites]
            p_df = __df_nan_mean(selected_historical_weather,
                                 1/station_dists**4)
            cols = p_df.columns
            p_df['postcode'] = p.postcode
            p_df = p_df[['postcode'] + list(cols)]
            to_be_inserted.append(p_df)
        except ValueError:
            log.debug(f"Supplied lat/long {p}")
            log.debug("Invalid lat/long Supplied. May be this is a PO Box?")

    final_df = pd.concat(to_be_inserted)

    # write table into BQ
    bqclient.df_to_bq(final_df, hist_table_ref)
