from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import statsmodels.api as sm

from statsmodels.tsa.stattools import adfuller, kpss

import pmdarima as pm

from datetime import datetime
from pandas import json_normalize

import sys
print(sys.path)

# Creamos una tabla para almacenar los resultados segun como queremos que se vean
# en la tabla de Airflow
# luego, entrenamos el modelo con los datos de entrenamiento y hacemos las predicciones
# para luego rellenar la tabla predictions con los resultados de las predicciones



# Funcion para extraer los datos de la base de datos
def extract_data(table):
    hook = PostgresHook(postgres_conn_id='postgres')
    
    query = "SELECT * FROM" + table
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    
    cursor.close()
    connection.close()
    
    return df

# Funcion para entrenar el modelo y hacer las predicciones
def _model_training_():
    train_data = extract_data("train")
    test_data = extract_data("test")
    # Count arrests per week for train_data
    train_arrest_count = train_data.groupby([pd.Grouper(key='ARREST_DATE', freq='W'), 'LAW_CAT_CD', 'ARREST_BORO']).size().reset_index(name='ARREST_COUNT')

    # Count arrests per week for test_data
    test_arrest_count = test_data.groupby([pd.Grouper(key='ARREST_DATE', freq='W'), 'LAW_CAT_CD', 'ARREST_BORO']).size().reset_index(name='ARREST_COUNT')
    
    train_arrest_pivot = train_arrest_count.pivot(index='ARREST_DATE', columns=['LAW_CAT_CD', 'ARREST_BORO'], values='ARREST_COUNT')
    test_arrest_pivot = test_arrest_count.pivot(index='ARREST_DATE', columns=['LAW_CAT_CD', 'ARREST_BORO'], values='ARREST_COUNT')


    train_arrest_pivot = train_arrest_pivot.fillna(0)
    test_arrest_pivot = test_arrest_pivot.fillna(0)
    
    predict_hook = PostgresHook(postgres_conn_id='postgres')
    
    # Connect to the PostgreSQL database for predictions
    predictions_conn = predict_hook.get_conn()
    predictions_cursor = predictions_conn.cursor()
    

    
    # Iterate over each borough and law category in train_arrest_pivot
    for i, borough in enumerate(train_arrest_pivot.columns.get_level_values('ARREST_BORO').unique()):
        for j, law_cat_cd in enumerate(train_arrest_pivot.columns.get_level_values('LAW_CAT_CD').unique()):
            # Prepare the data for the specific borough and law category
            data = train_arrest_pivot.xs((law_cat_cd, borough), level=('LAW_CAT_CD', 'ARREST_BORO'), axis=1)
            test = test_arrest_pivot.xs((law_cat_cd, borough), level=('LAW_CAT_CD', 'ARREST_BORO'), axis=1)
            
            # Fit the SARIMA model with the best order and seasonal order to the data
            model = sm.tsa.SARIMAX(data, order=best_order, seasonal_order=best_seasonal_order)
            model_fit = model.fit()
            
            # Make predictions using index positions
            predictions = model_fit.predict(start="2020-01-06", end="2021-01-05")
            
            # Save the predictions to the PostgreSQL table
            for idx, prediction in enumerate(predictions):
                arrest_date = predictions.index[idx]
                model_predictions = int(prediction)
                
                # Construct the INSERT query
                insert_query = "INSERT INTO predictions (ARREST_DATE, LAW_CAT_CD, ARREST_BORO, MODEL_PREDICTIONS) VALUES (%s, %s, %s, %s)"
                
                # Execute the INSERT query with the prediction values
                predictions_cursor.execute(insert_query, (arrest_date, law_cat_cd, borough, model_predictions))
    
    # Commit the changes and close the cursor and connection
    predictions_conn.commit()
    predictions_cursor.close()
    predictions_conn.close()
    
    

    
    
    
with DAG('MODEL_TRAINING', start_date = datetime(2023, 1, 1),
         schedule = '@daily', catchup = False) as dag:
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id = 'postgres',
        sql = '''
            CREATE TABLE IF NOT EXISTS predictions (
                ARREST_DATE TIMESTAMP PRIMARY KEY,
                LAW_CAT_CD VARCHAR,
                ARREST_BORO VARCHAR,
                MODEL_PREDICTIONS INT,
            );
        '''
    )

    
    model_training = PythonOperator(
        task_id = 'model_training',
        python_callable = _model_training_
    )

    # Dependencias
    create_table >> model_training