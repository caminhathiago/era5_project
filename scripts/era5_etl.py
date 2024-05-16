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


class CDSAPIExtractor:

    variables = ['10m_u_component_of_wind', '10m_v_component_of_wind', 'mean_wave_direction',
            'mean_wave_period', 'sea_surface_temperature', 'significant_height_of_combined_wind_waves_and_swell',
            ]

    @staticmethod
    def create_client() -> cdsapi.api.Client:
        return cdsapi.Client()

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
    CDSAPIExtractor.request(cdsapi_client=cdsapi_client,
                variable=CDSAPIExtractor.variables,
                year='2023',
                month='07',
                day='30',
                time='12:00',
                area=[-19, -48, -27, -36],
                output_file="test_etl.nc"
                )

    df = CDSAPIExtractor.dataset_to_dataframe(nc_file="test_etl.nc")
    CDSAPIExtractor.save_as_csv(df=df, output_file="era5_data.csv")


    # ETL
    factory = ETLFactory()
    spark = factory.create_etl(app_name="ERA5ETL")
    df = ERA5ETL.extract(spark, csv_file="era5_data.csv")
    df = ERA5ETL.rename_variables(df=df, rename_dict=ERA5ETL.rename_dict)
    ERA5ETL.load(df=df, parquet_file="era5_data.parquet")

if __name__ == '__main__':
    main()
