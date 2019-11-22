import datetime
import pandas as pd
import sys
import os
import xarray as xr

scripts_path =  os.path.abspath("../preprocess_ml_usgs/scripts/")
sys.path.insert(0, scripts_path)
import streamflow_data_retrival as st
from utils import convert_df_to_dataset, load_s3_zarr_store

def time_function(function, n_loop, *args):
    """
    time an arbitrary function, running it a number of times and returning the
    minimum elapsed time from the number of trials
    :param function: [function] the function that you want to time
    :param n_loop: [int] the number of times to run the function
    :param *args: arguments that will forwarded into the function that will be
    timed
    """
    times = []
    for i in range(n_loop):
        start_time = datetime.datetime.now()
        function(*args)
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        times.append(elapsed_time.total_seconds())
    return min(times)


def ds_to_df(ds):
    df = ds.to_dataframe()
    df.reset_index(inplace=True)
    df = df.pivot(index='datetime', columns='site_code', values='streamflow')
    return df


def retrieve_from_nwis(site_codes, start_date, end_date):
    df_list = []
    for site_code in site_codes:
        d = st.get_streamflow_data([site_code], start_date, end_date, 'iv',
                                   '15T')
        df_list.append(d)
    df_comb = pd.concat(df_list, 1)
    return df_comb


def get_zarr_data(sites, start_date, end_date):
    """
    get and persist data from a zarr store then read it into a pandas dataframe
    """
    my_bucket = 'ds-drb-data/15min_discharge'
    zarr_store = load_s3_zarr_store(my_bucket)
    ds = xr.open_zarr(zarr_store)
    q = ds['streamflow']
    s = q.loc[start_date:end_date, sites]
    df = ds_to_df(s)
    return df


def write_zarr(df):
    ds = convert_df_to_dataset(df, 'site_codes', 'datetime', 'streamflow', 
            {'datetime': df.shape[0], 'site_codes': df.shape[1]})
    zarr_store = load_s3_zarr_store('ds-drb-data/timing_test_zarr')
    ds.to_zarr(zarr_store)


def write_csv(df):
    path = 's3://ds-drb-data/timing_test_csv'
    df.to_csv(path)


def write_parquet(df):
    path = 's3://ds-drb-data/timing_test_parquet'
    df.to_parquet(path)


def read_zarr():
    zarr_store = load_s3_zarr_store('ds-drb-data/timing_test_zarr')
    ds = xr.open_zarr(zarr_store)
    q = ds['streamflow']
    df = ds_to_df(q)
    return df


def read_csv():
    path = 's3://ds-drb-data/timing_test_csv'
    df = pd.read_csv(path, index_col='datetime', parse_dates=['datetime'],
                     infer_datetime_format=True)
    return df


def read_parquet():
    path = 's3://ds-drb-data/timing_test_parquet'
    df = pd.read_parquet(path)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    return df

def get_sites_list():
    data_file = 'data/drb_streamflow_sites_table.csv'
    site_code_df = pd.read_csv(data_file, dtype=str)
    site_code_col = 'identifier'
    site_codes = site_code_df[site_code_col].to_list()
    site_codes = [s.replace('USGS-', '') for s in site_codes]
    return site_codes


if __name__ == "__main__":
    # SETUP
    # read in all stations
    drb_site_codes = get_sites_list()
    outlet_id = '01474500'
    start_date = '2019-01-01'
    end_date = '2019-01-10'
    n_trials = 1

    # RETRIEVAL
    # retrieve data for just the outlet
    # nwis
    sites = [outlet_id]
    nwis_one_site = time_function(retrieve_from_nwis, n_trials, sites,
                                   start_date, end_date)
    print('nwis one site time:', nwis_one_site)
    # Zarr
    zarr_one_site = time_function(get_zarr_data, n_trials, sites, start_date,
                                  end_date)
    print('zarr one site time:', zarr_one_site)


    # retrieve data for all stations
    # nwis
    sites = subset_stations
    nwis_all_sites = time_function(retrieve_from_nwis, n_trials, sites,
                                   start_date, end_date)
    print('nwis all sites time:', nwis_all_sites)
    # Zarr
    zarr_all_sites = time_function(get_zarr_data, n_trials, sites, start_date,
                                   end_date)
    print('zarr all sites time:', zarr_all_sites)

    # WRITE
    # get subset from full zarr
    df = get_zarr_data(sites, start_date, end_date)

    write_zarr_time = time_function(write_zarr, n_trials, df)
    print('write zarr:', write_zarr_time)

    write_parquet_time = time_function(write_parquet, n_trials, df)
    print('write parquet:', write_parquet_time)

    write_csv_time = time_function(write_csv, n_trials, df)
    print('write csv:', write_csv_time)

    # READ
    read_zarr_time = time_function(read_zarr, n_trials)
    print('read zarr:', write_zarr_time)

    read_parquet_time = time_function(write_parquet, n_trials)
    print('read parquet:', write_parquet_time)

    read_csv_time = time_function(write_csv, n_trials)
    print('read csv:', write_csv_time)
