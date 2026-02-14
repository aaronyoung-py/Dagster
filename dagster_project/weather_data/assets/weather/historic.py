from dagster import asset, Output, MetadataValue, AssetExecutionContext
import os
import pandas as pd
from utils.file_utils import FileUtils
from datetime import datetime, timedelta, date
import urllib
from weather_data.partitions import daily_partitions
from time import sleep
import openmeteo_requests
import requests_cache
from retry_requests import retry
import numpy as np

weather_data_key = os.getenv('WEATHER_DATA_KEY')
data_loc = os.getenv('DATA_STORE_LOC')
user = os.getenv('SQL_USER')
password = os.getenv('SQL_PASSWORD')
database = os.getenv('DATABASE')
port = os.getenv('SQL_PORT')
server = os.getenv('SQL_SERVER')

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


@asset(required_resource_keys={"mysql"})
def get_calender_locations_sql_historic(context):
    query = FileUtils.file_to_query('sql_calender_data_hist')
    context.log.info(f'Query to run: \n{query}')
    with context.resources.mysql.get_connection() as conn:
        df = pd.read_sql(query, conn)
    return Output(
        value=df,
        metadata={
            'num_records': len(df),
            'markdown': MetadataValue.md(df.head().to_markdown())
        }
    )


@asset()
def get_full_weather_historic_data(context, get_calender_locations_sql_historic: pd.DataFrame):

    context.log.info('Getting forecast for: 2018-01-01 to {}'.format(date.today() - timedelta(days=1)))

    api_url_archive = "https://archive-api.open-meteo.com/v1/archive"

    location_df = get_calender_locations_sql_historic

    weather_data = pd.DataFrame()

    for location in location_df.FCST_LOCATION.unique():
        context.log.info('Getting data for {}'.format(location))

        latitude = location_df[location_df['FCST_LOCATION'] == location].LATITUDE.iloc[0]
        longitude = location_df[location_df['FCST_LOCATION'] == location].LONGITUDE.iloc[0]

        if np.isnan(longitude) or np.isnan(latitude):
            context.log.info('Skipping due to longitude or latitude being null')
            continue

        params_archive = {
            "latitude": [latitude],
            "longitude": [longitude],
            "hourly":  ["temperature_2m", "precipitation", "weather_code", "cloud_cover", "wind_speed_10m",
                        "wind_direction_10m"],
            "timezone": "GMT",
            "start_date": '2018-01-01',
            "end_date": str(date.today() - timedelta(days=2))
        }

        responses_archive = openmeteo.weather_api(api_url_archive, params=params_archive)

        response_archive = responses_archive[0]

        hourly_archive = response_archive.Hourly()
        hourly_temperature_2m_archive = hourly_archive.Variables(0).ValuesAsNumpy()
        hourly_precipitation_archive = hourly_archive.Variables(1).ValuesAsNumpy()
        hourly_weather_code_archive = hourly_archive.Variables(2).ValuesAsNumpy()
        hourly_cloud_cover_archive = hourly_archive.Variables(3).ValuesAsNumpy()
        hourly_wind_speed_10m_archive = hourly_archive.Variables(4).ValuesAsNumpy()
        hourly_wind_direction_10m_archive = hourly_archive.Variables(5).ValuesAsNumpy()

        hourly_data_archive = {"utc_datetime": pd.date_range(
            start=pd.to_datetime(hourly_archive.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly_archive.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly_archive.Interval()),
            inclusive="left"
        )}

        hourly_data_archive['FCST_LOCATION'] = location
        hourly_data_archive['source'] = 'openmeteo_archive_api'
        hourly_data_archive["temp"] = hourly_temperature_2m_archive
        hourly_data_archive["precip"] = hourly_precipitation_archive
        hourly_data_archive["conditions"] = hourly_weather_code_archive
        hourly_data_archive["cloudcover"] = hourly_cloud_cover_archive
        hourly_data_archive["windspeed"] = hourly_wind_speed_10m_archive
        hourly_data_archive["winddir"] = hourly_wind_direction_10m_archive

        hourly_dataframe_archive = pd.DataFrame(data=hourly_data_archive)

        weather_data = pd.concat((weather_data, hourly_dataframe_archive))

        sleep(15)

    weather_data = weather_data[['FCST_LOCATION',
                                 'utc_datetime',
                                 'temp',
                                 'precip',
                                 'windspeed',
                                 'winddir',
                                 'cloudcover',
                                 'conditions',
                                 'source']]

    return Output(
        value=weather_data,
        metadata={
            'num_records': len(weather_data),
            'markdown': MetadataValue.md(weather_data.head(10).to_markdown())
        }
    )

@asset(partitions_def=daily_partitions)
def get_weather_historic_data(context, get_calender_locations_sql_historic: pd.DataFrame):
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, '%Y-%m-%d').date()

    context.log.info('Getting forecast for: {}'.format(partition_date))

    if (date.today() - timedelta(days=1)) == partition_date:
        api_url = "https://api.open-meteo.com/v1/forecast"
        source = "openmeteo_forecast_api"
    else:
        api_url = "https://archive-api.open-meteo.com/v1/archive"
        source = "openmeteo_archive_api"

    location_df = get_calender_locations_sql_historic

    weather_data = pd.DataFrame()

    for location in location_df.FCST_LOCATION.unique():
        context.log.info('Getting data for {}'.format(location))

        latitude = location_df[location_df['FCST_LOCATION'] == location].LATITUDE.iloc[0]
        longitude = location_df[location_df['FCST_LOCATION'] == location].LONGITUDE.iloc[0]

        if np.isnan(longitude) or np.isnan(latitude):
            context.log.info('Skipping due to longitude or latitude being null')
            continue

        params = {
            "latitude": [latitude],
            "longitude": [longitude],
            "hourly": ["temperature_2m", "precipitation", "weather_code", "cloud_cover", "wind_speed_10m", "wind_direction_10m"],
            "timezone": "UTC",
            "start_date": partition_date.strftime('%Y-%m-%d'),
            "end_date": partition_date.strftime('%Y-%m-%d')
        }

        responses = openmeteo.weather_api(api_url, params=params)

        response = responses[0]

        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
        hourly_weather_code = hourly.Variables(2).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(3).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(4).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(5).ValuesAsNumpy()

        hourly_data = {"utc_datetime": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )}

        hourly_data['FCST_LOCATION'] = location
        hourly_data['source'] = source
        hourly_data["temp"] = hourly_temperature_2m
        hourly_data["precip"] = hourly_precipitation
        hourly_data["conditions"] = hourly_weather_code
        hourly_data["cloudcover"] = hourly_cloud_cover
        hourly_data["windspeed"] = hourly_wind_speed_10m
        hourly_data["winddir"] = hourly_wind_direction_10m

        hourly_dataframe = pd.DataFrame(data=hourly_data)

        weather_data = pd.concat((weather_data, hourly_dataframe))

        sleep(1)

    weather_data = weather_data[['FCST_LOCATION',
                                 'utc_datetime',
                                 'temp',
                                 'precip',
                                 'windspeed',
                                 'winddir',
                                 'cloudcover',
                                 'conditions',
                                 'source']]

    return Output(
        value=weather_data,
        metadata={
            'num_records': len(weather_data),
            'markdown': MetadataValue.md(weather_data.head(10).to_markdown())
        }
    )

@asset(required_resource_keys={"mysql"},
       partitions_def=daily_partitions)
def weather_historical_cleanup(context: AssetExecutionContext, get_weather_historic_data: pd.DataFrame):
    df = get_weather_historic_data
    partition_date_str = context.partition_key
    partition_date = datetime.strptime(partition_date_str, '%Y-%m-%d').date()

    query = f'''
    DELETE FROM WEATHER.WEATHER_HISTORIC 
    WHERE 
        date(FCST_DATETIME) = '{partition_date.strftime("%Y-%m-%d")}'
    '''

    context.log.info(query)

    with context.resources.mysql.get_connection() as conn:
        conn.cursor().execute(query)
        conn.commit()

    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )


@asset(io_manager_key='sql_io_manager',
       key_prefix=['WEATHER', 'WEATHER_HISTORIC', 'replace'])
def full_weather_historic_to_sql(context, get_full_weather_historic_data: pd.DataFrame):
    load_date = datetime.today()
    df = get_full_weather_historic_data
    df.rename(columns={'FCST_LOCATION': 'FCST_LOCATION',
                       'utc_datetime': 'FCST_DATETIME',
                       'temp': 'TEMPERATURE',
                       'precip': 'PRECIPITATION',
                       'windspeed': 'WIND_SPEED',
                       'winddir': 'WIND_DIRECTION',
                       'cloudcover': 'CLOUD_COVER',
                       'conditions': 'WEATHER_TYPE_CD',
                       'source': 'FCST_SOURCE'},
              inplace=True)
    df.loc[:, 'LOAD_TS'] = load_date
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )


@asset(io_manager_key='sql_io_manager',
       key_prefix=['WEATHER', 'WEATHER_HISTORIC', 'append'],
       partitions_def=daily_partitions)
def weather_historic_to_sql(context, weather_historical_cleanup: pd.DataFrame):
    load_date = datetime.today()
    df = weather_historical_cleanup
    df.rename(columns={'FCST_LOCATION': 'FCST_LOCATION',
                       'utc_datetime': 'FCST_DATETIME',
                       'temp': 'TEMPERATURE',
                       'precip': 'PRECIPITATION',
                       'windspeed': 'WIND_SPEED',
                       'winddir': 'WIND_DIRECTION',
                       'cloudcover': 'CLOUD_COVER',
                       'conditions': 'WEATHER_TYPE_CD',
                       'source': 'FCST_SOURCE'},
              inplace=True)
    df.loc[:, 'LOAD_TS'] = load_date
    return Output(
        value=df,
        metadata={
            'Markdown': MetadataValue.md(df.head().to_markdown()),
            'Rows': len(df),
        }
    )

