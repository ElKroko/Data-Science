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




# Si lo que queremos ahora es guardar la informacion de los usuarios en postgresy para esto debemos usar algo llamado un Hook
# En realidad los operadores de transferencia como PostgresOperator utilizar hooks por debajo para conectarse directamente
# al servicio. Los Operadores son simplemente abstracciones para simplificar el acceso pero usar hooks permite acceder
# a mayor funcionalidad al momento de trabajar con postgres en este caso



def _store_data_():
    hook = PostgresHook(postgres_conn_id = 'postgres')
    hook.copy_expert(
        sql = "COPY arrests FROM stdin WITH DELIMITER as ','",
        filename = "/tmp/Data/NYPD_Arrests_Data__Historic_.csv"
    )

def _process_data_():
    df = pd.read_csv('/tmp/Data/NYPD_Arrests_Data__Historic_.csv')
    df["ARREST_DATE"] = pd.to_datetime(df["ARREST_DATE"])
    df_2019 = df[((df["ARREST_DATE"].dt.year > 2007) & (df["ARREST_DATE"].dt.year <= 2019))]
    df_2020 = df[df["ARREST_DATE"].dt.year == 2020]
    
    # Data Cleaning
    df_2019.dropna(subset=["LAW_CAT_CD"], inplace=True)
    df_2020.dropna(subset=["LAW_CAT_CD"], inplace=True)
    
    # Convertiremos estos datos en algo legible a continuacion:
    df_2019["LAW_CAT_CD"] = df_2019["LAW_CAT_CD"].replace({"F": "Felony", "M": "Misdemeanor", "V": "Violation"})
    df_2020["LAW_CAT_CD"] = df_2020["LAW_CAT_CD"].replace({"F": "Felony", "M": "Misdemeanor", "V": "Violation"})
    df_2019["ARREST_BORO"] = df_2019["ARREST_BORO"].replace({"B": "Bronx", "K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "S": "Staten Island"})
    df_2020["ARREST_BORO"] = df_2020["ARREST_BORO"].replace({"B": "Bronx", "K": "Brooklyn", "M": "Manhattan", "Q": "Queens", "S": "Staten Island"})
    
    df_2019.to_csv("/tmp/Data/NYPD_ARRESTS_DATA_2019.csv", index=None, header=False)
    df_2020.to_csv("/tmp/Data/NYPD_ARRESTS_DATA_2020.csv", index=None, header=False)


def _model_training_():
    train = pd.read_csv('/tmp/Data/NYPD_ARRESTS_DATA_2019.csv')
    test = pd.read_csv('/tmp/Data/NYPD_ARRESTS_DATA_2019.csv')

    
with DAG('SARIMAX', start_date = datetime(2023, 1, 1),
         schedule = '@daily', catchup = False) as dag:
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id = 'postgres',
        sql = '''
            CREATE TABLE arrests (
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
    
    store_data = PythonOperator(
        task_id = 'store_data',
        python_callable = _store_data_
    )

    process_data = PythonOperator(
        task_id = 'process_data',
        python_callable = _process_data_
    )
    
    

    # Dependencias
    create_table  >> store_data >> process_data