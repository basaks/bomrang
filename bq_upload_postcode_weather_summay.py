from typing import Dict
from scipy.spatial import cKDTree
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import Client, TableReference, DatasetReference
from google.cloud.exceptions import NotFound
from google.cloud import bigquery_storage_v1beta1
from google.auth import default

weather_data_types = ['rainfall',
                      'min_temperature',
                      'max_temperature',
                      'solar_exposure']

def bq_table_exists(client: Client, table_ref: TableReference) -> bool:
    """
    :param client:
    :param table_ref:
    :return:
    """
    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


def dataset_exists(client: Client, dataset_reference: DatasetReference) \
        -> bool:
    """
    copied from:
    https://github.com/googleapis/google-cloud-python/blob/7ba0220ff9d0d68241baa863b3152c34dc9f7a1a/bigquery/docs/snippets.py#L178
    Return True if a dataset exists.

    Args:
        client (google.cloud.bigquery.client.Client):
            A client to connect to the BigQuery API.
        dataset_reference (google.cloud.bigquery.dataset.DatasetReference):
            A reference to the dataset to look for.

    Returns:
        bool: ``True`` if the dataset exists, ``False`` otherwise.
    """
    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False


def get_df_from_query(bqclient: bigquery.Client, query_string: str) \
        -> pd.DataFrame:

    # Download query results.
    dataframe = (
        bqclient.query(query_string)
            .result()
            # Note: The BigQuery Storage API cannot be used to download small query
            # results, but as of google-cloud-bigquery version 1.11.1, the
            # to_dataframe method will fallback to the tabledata.list API when the
            # BigQuery Storage API fails to read the query results.
            .to_dataframe(bqstorage_client=bqstorageclient)
    )
    return dataframe


def __create_date_index():
    return pd.DataFrame(index=pd.date_range('2016-01-01', '2019-06-30',
                                            freq='D'))


def download_all_stations_data(stations: pd.DataFrame) -> \
        Dict[str, pd.DataFrame]:

    def __inner(s, i):
        # check if weather station (data) exists in BQ
        # not all stations are active now/data may not be available
        # Supply NaN's for missing weather columns

        print(f"Download data for station: {i}, site: {s}")
        this_station_ref = TableReference(dataset_ref_, str(s))
        if bq_table_exists(bqclient_, this_station_ref):
            df = bqclient_.list_rows(this_station_ref).to_dataframe(
                bqstorage_client=bqstorageclient)
            df.index = df.apply(lambda x: pd.datetime(
                int(x['year']), int(x['month']), int(x['day'])), axis=1)

            for dt in weather_data_types:
                if dt not in df.columns:
                    df[dt] = np.nan

            df = df_index.join(df)
            return df[weather_data_types]
        return pd.DataFrame()  # else return empty dataframe

    return {s: __inner(s, i) for i, s in enumerate(stations.site)}


def __df_nan_mean(dfs, weights):
    # make sure dfs contain same columns
    for df in dfs[1:]:
        assert set(dfs[0].columns).__eq__(set(df.columns))

    df_mean = pd.DataFrame({'date': dfs[0].index.date})

    for c in dfs[0].columns:
        data = np.vstack([df[c] for df in dfs])
        masked_data = np.ma.masked_array(data, np.isnan(data))
        df_mean[c] = np.ma.average(masked_data, weights=weights, axis=0)

    return df_mean


if __name__ == '__main__':
    credentials, your_project_id = default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    # Make clients.
    bqclient_ = bigquery.Client(credentials=credentials,
                                project=your_project_id)

    bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient(
        credentials=credentials)

    dataset_id = 'weather'
    dataset_ref_ = bqclient_.dataset(dataset_id)

    stations_ref = TableReference(dataset_ref_, 'stations_site_list')

    # https://www.matthewproctor.com/australian_postcodes
    postcodes_ref = TableReference(dataset_ref_, 'australian_postcodes')

    stations = bqclient_.list_rows(stations_ref).to_dataframe(
        bqstorage_client=bqstorageclient)

    postcodes = bqclient_.list_rows(postcodes_ref).to_dataframe(
        bqstorage_client=bqstorageclient)

    kdtree = cKDTree(stations[['lat', 'lon']].head(10))

    df_index = __create_date_index()

    historical_weather = download_all_stations_data(stations.head(10))

    # import IPython; IPython.embed(); import sys; sys.exit()

    postcode_ds = 'postcode'
    postcode_ds_ref = bqclient_.dataset(postcode_ds)

    bqclient_.create_dataset(postcode_ds_ref, exists_ok=True)

    for i, p in enumerate(postcodes.itertuples()):
        if i == 3: break
        lat_long = (p.lat, p.long)
        station_dists, stations_indices = kdtree.query(lat_long, k=3)
        selected_station_sites = stations.site[stations_indices]

        selected_historical_weather = [historical_weather[s]
                                       for s in selected_station_sites]
        p_df = __df_nan_mean(selected_historical_weather, 1/station_dists)

        p_table_ref = TableReference(postcode_ds_ref, str(str(p.postcode)))

        if bq_table_exists(bqclient_, table_ref=p_table_ref):
            bqclient_.delete_table(p_table_ref)
        print("Deleted table '{}'.".format(p_table_ref))

        load_job = bqclient_.load_table_from_dataframe(p_df, p_table_ref)
        print(f"Starting  upload job for postcode {p.postcode}, "
              f"job id: {load_job.job_id}")
        load_job.result()  # Waits for table load to complete.
        print(f"Upload Job for Postcode {p.postcode} finished.")
