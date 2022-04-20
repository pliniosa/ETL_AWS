"""Programa execultado em uma plataforma windowns"""

import logging
import os
from os import name, system
from time import sleep

from boto import BUCKET_NAME_RE
from numpy import str_

import boto3
from more_itertools import bucket
import pandas as pd
import pyarrow
import pyarrow.parquet as pq
import s3fs
from botocore.exceptions import ClientError


class Extractor:

    def read_from_csv(self, csv_filename: str, sep=';'):
        try:
            df_csv = pd.read_csv(csv_filename, sep)
            shape_df = df_csv.shape
            output_status = f'{csv_filename} foi carregado e tem o shape de: {shape_df}'
            print(output_status)
            return df_csv
        except Exception as e:
            print(e)


class Transformer:

    def pandasdf_to_parquet(self, df, filename, partition_cols: list = None):

        try:
            parquet_path = filename.replace('.csv', '.parquet')

            if partition_cols:
                df.to_parquet(parquet_path, engine='auto', compression='snappy', partition_cols=partition_cols)
            else:
                df.to_parquet(parquet_path, engine='auto', compression='snappy')
            
            return parquet_path
        except Exception as e:
            print(e)


class Loader:

    def write_to_s3(self, df, bucketname, partion_by):
        try:
            if partion_by:
                pq.write_to_dataset(pyarrow.Table.from_pandas(df), bucketname, 
                filesystem=s3fs.S3FileSystem(), partition_cols=partion_by)
            else:
                pq.write_to_dataset(pyarrow.Table.from_pandas(df), bucketname, 
                filesystem=s3fs.S3FileSystem())
        except Exception as e:
            print(e)


class Engine:

    def __init__(self) -> None:
        self.extractor = Extractor()
        self.transformer = Transformer()
        self.loader = Loader()

    def run(self, datasource, partition_by, bucketname):

        # extração
        df_csv = self.extractor.read_from_csv(datasource)

        # transformação dos dados
        if partition_by:
            parquet_path = self.transformer.pandasdf_to_parquet(df_csv, datasource, partition_by)
        else:
            parquet_path = self.transformer.pandasdf_to_parquet(df_csv, datasource)

        # injestão dos dados no AWS S3
        if bucketname:
            address = os.path.basename(parquet_path)
            address = address.replace('.parquet', '')
            bucketname = os.path.join(bucketname, address)
            bucketname = bucketname.replace('\\', '/') # troca para funcionar em systema windowns
            self.loader.write_to_s3(df_csv, bucketname, partition_by)


def clear()->None:
    sleep(1)

    if name == 'nt':
        _ = system('cls')
    else:
        _ = system('clear')


if __name__ == '__main__':
    clear()
    
    datasource = 'C:\\Users\\plini\\Downloads\\handson\\Tabela_PACP.csv'
    partition_by = ['Ano']

    bucketname = 'hotodata-raw-psa'

    engine_app = Engine()
    engine_app.run(datasource, partition_by, bucketname)