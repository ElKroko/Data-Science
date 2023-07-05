from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd


from datetime import datetime
from pandas import json_normalize



# En caso de que salga mal, botamos la tabla de Predicciones.

    
with DAG('DROP_DATA', start_date = datetime(2023, 1, 1),
         schedule = '@daily', catchup = False) as dag:
    
    drop_table = PostgresOperator(
        task_id='drop_predictions_table',
        postgres_conn_id='postgres',
        sql='DROP TABLE IF EXISTS predictions;'
    )
    
    create_table = PostgresOperator(
        task_id='create_predictions_table',
        postgres_conn_id = 'postgres',
        sql = '''
            CREATE TABLE IF NOT EXISTS predictions (
                ID SERIAL PRIMARY KEY,
                ARREST_DATE TIMESTAMP,
                LAW_CAT_CD VARCHAR,
                ARREST_BORO VARCHAR,
                MODEL_PREDICTIONS INT
            );
        '''
    )
    

    # Dependencias
    drop_table >> create_table