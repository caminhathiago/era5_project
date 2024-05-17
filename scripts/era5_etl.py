import cdsapi
import findspark
findspark.init('/home/thiagocaminha/spark-3.5.1-bin-hadoop3')
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (StructType, StructField,
                                 StringType, IntegerType,
                                 DoubleType, TimestampType)
import xarray as xr
import pandas as pd
from typing import Union
from datetime import datetime, timedelta

class CDSAPIExtractor:

    variables = ['10m_u_component_of_wind', '10m_v_component_of_wind', 'mean_wave_direction',
            'mean_wave_period', 'sea_surface_temperature', 'significant_height_of_combined_wind_waves_and_swell',
            ]

    @staticmethod
    def create_client() -> cdsapi.api.Client:
        return cdsapi.Client()

    @staticmethod
    def generate_datetimes(end_date:datetime=datetime.utcnow(), past_days:int=7) -> pd.DataFrame:
        date_times = [end_date]
        for days in range(1,past_days+1):
            increment_datetime = end_date - timedelta(days=days)
            date_times.append(increment_datetime)

        date_times.sort()

        return pd.DataFrame(date_times, columns=["date_time"])

    @staticmethod
    def round_now_time(date_times:pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(date_times.date_time.round('1H'))

    @staticmethod
    def get_datetimes_components(date_times:pd.DataFrame) -> dict:

        date_times['year'] = date_times.date_time.dt.year
        date_times['month'] = date_times.date_time.dt.month
        date_times['day'] = date_times.date_time.dt.day
        date_times['time'] = date_times.date_time.dt.hour.astype(str) + ":00"

        years = list(date_times['year'].unique().astype(str))
        months = list(date_times['month'].unique().astype(str))
        days = list(date_times['day'].unique().astype(str))
        times = list(date_times['time'].unique().astype(str))

        return {'year':years,
                'month':months,
                'day':days,
                'time':times,
            }


    @staticmethod
    def request(cdsapi_client: cdsapi.api.Client,
                variable: Union[list,str],
                year: Union[list,str],
                month: Union[list,str],
                day:  Union[list,str],
                time:  Union[list,str],
                area: list,
                output_file: str,
                cds_product: str='reanalysis-era5-single-levels',
                file_format: str='netcdf'
                ) -> None:

        return cdsapi_client.retrieve(cds_product,
                                    {
                                        'product_type': 'reanalysis',
                                        'format': file_format,
                                        'year': year,
                                        'variable': variable,
                                        'month': month,
                                        'day': day,
                                        'time': time,
                                        'area': area,
                                    },
                                    output_file)

    @staticmethod
    def dataset_to_dataframe(nc_file:str) -> pd.DataFrame:
        ds = xr.load_dataset(nc_file)
        return ds.to_dataframe()

    @staticmethod
    def save_as_csv(df:pd.DataFrame, output_file:str) -> None:
        return df.to_csv(output_file)


class ERA5ETL:
    spark: SparkSession

    era5_schema = StructType([
                    StructField(name="latitude", dataType=DoubleType(), nullable=True),
                    StructField(name="longitude", dataType=DoubleType(), nullable=True),
                    StructField(name="time", dataType=TimestampType(), nullable=True),
                    StructField(name="u10", dataType=DoubleType(), nullable=True),
                    StructField(name="v10", dataType=DoubleType(), nullable=True),
                    StructField(name="mwd", dataType=DoubleType(), nullable=True),
                    StructField(name="mwp", dataType=DoubleType(), nullable=True),
                    StructField(name="sst", dataType=DoubleType(), nullable=True),
                    StructField(name="swh", dataType=DoubleType(), nullable=True),
                ])

    rename_dict = {'u10':'u_wind_10_meter',
                'v10':'v_wind_10_meter',
                'mwd':'mean_wave_direction',
                'mwp':'mean_wave_period',
                'sst':'sea_surface_temperature',
                'swh':'significant_wave_height_wind_and_swell'
                }

    @staticmethod
    def extract(spark:SparkSession, csv_file:str) -> DataFrame:
        return spark.read.csv(csv_file)

    @staticmethod
    def transform() -> DataFrame:
        pass

    @staticmethod
    def restrict_area() -> DataFrame:
        pass

    @staticmethod
    def rename_variables(df: DataFrame, rename_dict:dict) -> DataFrame:
        for key in rename_dict:
            df = df.withColumnRenamed(key,rename_dict[key])
        return df

    @staticmethod
    def load(df: DataFrame, parquet_file:str) -> None:
        df.write.parquet(parquet_file, mode='overwrite')


class ETLFactory:

    @staticmethod
    def create_etl(app_name:str) -> SparkSession:
        return SparkSession.builder.appName(app_name).getOrCreate()



def main():

    # cdsapi interaction
    cdsapi_client = CDSAPIExtractor.create_client()

    date_times = CDSAPIExtractor.generate_datetimes(end_date=datetime.utcnow(), past_days=7)
    # date_times = CDSAPIExtractor.round_now_time(date_times=date_times)
    date_times_components = CDSAPIExtractor.get_datetimes_components(date_times=date_times)
    print(type(date_times_components['time']))

    CDSAPIExtractor.request(cdsapi_client=cdsapi_client,
                variable=CDSAPIExtractor.variables,
                year=date_times_components['year'],
                month=date_times_components['month'],
                day=date_times_components['day'],
                time=date_times_components['time'],
                area=[-19, -48, -27, -36],
                output_file="era5_data.nc"
                )

    df = CDSAPIExtractor.dataset_to_dataframe(nc_file="era5_data.nc")
    CDSAPIExtractor.save_as_csv(df=df, output_file="era5_data.csv")


    # ETL
    factory = ETLFactory()
    spark = factory.create_etl(app_name="ERA5ETL")
    df = ERA5ETL.extract(spark, csv_file="era5_data.csv")
    df = ERA5ETL.rename_variables(df=df, rename_dict=ERA5ETL.rename_dict)
    ERA5ETL.load(df=df, parquet_file="era5_data.parquet")

if __name__ == '__main__':
    main()
