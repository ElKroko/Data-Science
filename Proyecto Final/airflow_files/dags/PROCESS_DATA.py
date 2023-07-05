from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
#import statsmodels.api as sm

#from statsmodels.tsa.stattools import adfuller, kpss

# import pmdarima as pm

from datetime import datetime
from pandas import json_normalize


# Copiamos la informacion desde el archivo csv grande, lo procesamos
# y lo guardamos en dos archivos csv mas pequeños, para luego cargarlos
# a postgres y poder acceder desde el modelo a estas nuevas tablas.


def _store_train_data_():
    hook = PostgresHook(postgres_conn_id = 'postgres')
    hook.copy_expert(
        sql = "COPY train FROM stdin WITH (FORMAT CSV, HEADER true, DELIMITER ',')",
        filename = "/tmp/Data/NYPD_ARRESTS_DATA_2019.csv"
    )

def _store_test_data_():
    hook = PostgresHook(postgres_conn_id = 'postgres')
    hook.copy_expert(
        sql = "COPY test FROM stdin WITH (FORMAT CSV, HEADER true, DELIMITER ',')",
        filename = "/tmp/Data/NYPD_ARRESTS_DATA_2020.csv"
    )

def _process_data_():
    ## Leemos la informacion del dataset completo
    df = pd.read_csv('/tmp/Data/NYPD_Arrests_Data__Historic_.csv')
    
    print(df.columns)
    
    df["ARREST_DATE"] = pd.to_datetime(df["ARREST_DATE"])
    df_2019 = df[((df["ARREST_DATE"].dt.year > 2007) & (df["ARREST_DATE"].dt.year <= 2019))]
    df_2020 = df[df["ARREST_DATE"].dt.year == 2020]

    # Data Cleaning
    df_2019 = df_2019.dropna(subset=["LAW_CAT_CD"]).copy()
    df_2020 = df_2020.dropna(subset=["LAW_CAT_CD"]).copy()

    # Convertiremos estos datos en algo legible a continuacion:
    df_2019.loc[:, "LAW_CAT_CD"] = df_2019["LAW_CAT_CD"].replace({"F": "Felony", "M": "Misdemeanor", "V": "Violation"})
    df_2020.loc[:, "LAW_CAT_CD"] = df_2020["LAW_CAT_CD"].replace({"F": "Felony", "M": "Misdemeanor", "V": "Violation"})
    df_2019.loc[:, "ARREST_BORO"] = df_2019["ARREST_BORO"].replace({"B": "Bronx", "K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "S": "Staten Island"})
    df_2020.loc[:, "ARREST_BORO"] = df_2020["ARREST_BORO"].replace({"B": "Bronx", "K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "S": "Staten Island"})

    ## Guardamos los datos en un archivo csv
    df_2019.to_csv("/tmp/Data/NYPD_ARRESTS_DATA_2019.csv", index=None)
    df_2020.to_csv("/tmp/Data/NYPD_ARRESTS_DATA_2020.csv", index=None )

    

    
with DAG('PROCESS_DATA', start_date = datetime(2023, 1, 1),
         schedule = '@daily', catchup = False) as dag:
    
    create_train_table = PostgresOperator(
        task_id='create_train_table',
        postgres_conn_id = 'postgres',
        sql = '''
            CREATE TABLE IF NOT EXISTS train (
                ARREST_KEY SERIAL PRIMARY KEY,
                ARREST_DATE TIMESTAMP,
                PD_CD FLOAT,
                PD_DESC VARCHAR,
                KY_CD FLOAT,
                OFNS_DESC VARCHAR,
                LAW_CODE VARCHAR,
                LAW_CAT_CD VARCHAR,
                ARREST_BORO VARCHAR,
                ARREST_PRECINCT INTEGER,
                JURISDICTION_CODE FLOAT,
                AGE_GROUP VARCHAR,
                PERP_SEX VARCHAR,
                PERP_RACE VARCHAR,
                X_COORD_CD FLOAT,
                Y_COORD_CD FLOAT,
                Latitude FLOAT,
                Longitude FLOAT,
                Lon_Lat VARCHAR
            );
        '''
    )
    
    create_test_table = PostgresOperator(
        task_id='create_test_table',
        postgres_conn_id = 'postgres',
        sql = '''
            CREATE TABLE IF NOT EXISTS test (
                ARREST_KEY SERIAL PRIMARY KEY,
                ARREST_DATE TIMESTAMP,
                PD_CD FLOAT,
                PD_DESC VARCHAR,
                KY_CD FLOAT,
                OFNS_DESC VARCHAR,
                LAW_CODE VARCHAR,
                LAW_CAT_CD VARCHAR,
                ARREST_BORO VARCHAR,
                ARREST_PRECINCT INTEGER,
                JURISDICTION_CODE FLOAT,
                AGE_GROUP VARCHAR,
                PERP_SEX VARCHAR,
                PERP_RACE VARCHAR,
                X_COORD_CD FLOAT,
                Y_COORD_CD FLOAT,
                Latitude FLOAT,
                Longitude FLOAT,
                Lon_Lat VARCHAR
            );
        '''
    )


    process_data = PythonOperator(
        task_id = 'process_data',
        python_callable = _process_data_
    )
    
    store_train_data = PythonOperator(
        task_id = 'store_train_data',
        python_callable = _store_train_data_
    )
    
    store_test_data = PythonOperator(
        task_id = 'store_test_data',
        python_callable = _store_test_data_
    )
    
    
    # Dependencias
    
    create_train_table >> create_test_table >> process_data >> store_train_data >> store_test_data
    