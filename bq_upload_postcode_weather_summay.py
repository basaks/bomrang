import logging
from itertools import compress
from typing import Dict, List
from scipy.spatial import cKDTree
from pathlib import Path
import numpy as np
import pandas as pd
from google.cloud.bigquery import TableReference
from gcputils.bqclient import BQClient
from bwsresponse import logger

logger.configure(verbosity=logging.DEBUG)
log = logging.getLogger(__name__)


weather_data_types = ['rainfall',
                      'min_temperature',
                      'max_temperature',
                      'solar_exposure']


def download_all_stations_data(stations: pd.DataFrame) -> \
        Dict[str, pd.DataFrame]:

    df_index = pd.DataFrame(index=pd.date_range('2016-01-01', '2019-06-30',
                                                freq='D'))

    def __inner(s, i):
        # check if weather station (data) exists in BQ
        # not all stations are active now/data may not be available
        # Supply NaN's for missing weather columns

        log.info(f"Download data for station: {i}, site: {s}")
        this_station_ref = TableReference(dataset_ref_, str(s))
        if bqclient.table_exists(this_station_ref):
            df = bqclient.client.list_rows(this_station_ref).to_dataframe(
                bqstorage_client=bqclient.storage_client)
            df.index = df.apply(lambda x: pd.datetime(
                int(x['year']), int(x['month']), int(x['day'])), axis=1)

            for dt in weather_data_types:
                if dt not in df.columns:
                    df[dt] = np.nan

            df = df_index.join(df)
            return df[weather_data_types]
        return pd.DataFrame()  # else return empty dataframe

    return {s: __inner(s, i) for i, s in enumerate(stations.site)}


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
        data = np.vstack([df[c] for df in dfs])
        masked_data = np.ma.masked_array(data=data, mask=np.isnan(data),
                                         dtype=np.float32)
        avg_data = np.ma.average(masked_data, weights=weights, axis=0)
        df_mean[c] = avg_data.filled(np.nan)

    return df_mean


if __name__ == '__main__':

    bqclient = BQClient()

    dataset_id = 'weather'
    dataset_ref_ = bqclient.client.dataset(dataset_id)

    stations_ref = TableReference(dataset_ref_, 'stations_site_list')

    # https://www.matthewproctor.com/australian_postcodes
    postcodes_ref = TableReference(dataset_ref_, 'australian_postcodes')

    stations = bqclient.client.list_rows(stations_ref).to_dataframe(
        bqstorage_client=bqclient.storage_client)

    postcodes = bqclient.client.list_rows(postcodes_ref).to_dataframe(
        bqstorage_client=bqclient.storage_client)

    kdtree = cKDTree(stations[['lat', 'lon']])

    weather_file_on_disc = 'historical_weather.pk'
    import pickle
    if Path(weather_file_on_disc).exists():
        historical_weather = pickle.load(open('historical_weather.pk', 'rb'))
    else:
        historical_weather = download_all_stations_data(stations)
        pickle.dump(historical_weather, open('historical_weather.pk', 'wb'))

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
