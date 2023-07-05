from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from datetime import datetime
from pandas import json_normalize

def extract_data(table):
    hook = PostgresHook(postgres_conn_id='postgres')
    
    query = "SELECT * FROM " + table
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    df.columns = df.columns.str.upper()
    
    cursor.close()
    connection.close()

def visualize_dataset():
    
    # Connect to the PostgreSQL database
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Execute the SQL query to fetch the data from the predictions table
    cursor.execute("SELECT ARREST_DATE, ARREST_BORO, LAW_CAT_CD, MODEL_PREDICTIONS FROM predictions")

    # Fetch all the rows from the query result
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=["ARREST_DATE",'ARREST_BORO', 'LAW_CAT_CD', 'MODEL_PREDICTIONS'])

    # Close the cursor and connection
    cursor.close()
    conn.close()
    
    hook = PostgresHook(postgres_conn_id='postgres')
    
    query = "SELECT * FROM test"
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    
    test = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    test.columns = test.columns.str.upper()
    print(test.columns)
    
    cursor.close()
    connection.close()
    
    test_arrest_count = test.groupby([pd.Grouper(key='ARREST_DATE', freq='W'), 'LAW_CAT_CD', 'ARREST_BORO']).size().reset_index(name='ARREST_COUNT')
    test_arrest_pivot = test_arrest_count.pivot(index='ARREST_DATE', columns=['LAW_CAT_CD', 'ARREST_BORO'], values='ARREST_COUNT')
    test_arrest_pivot = test_arrest_pivot.fillna(0)


    for i, arrest_boro in enumerate(df['ARREST_BORO'].unique()):
        for j, law_cat_cd in enumerate(df['LAW_CAT_CD'].unique()):
            
            print(arrest_boro)
            print(law_cat_cd)
            
            data = df[(df['ARREST_BORO'] == arrest_boro) & (df['LAW_CAT_CD'] == law_cat_cd)]
            data.loc[:, 'MODEL_PREDICTIONS'] = data['MODEL_PREDICTIONS'].apply(lambda x: np.asscalar(x) if isinstance(x, np.ndarray) else x).astype(float)
            data['MODEL_PREDICTIONS'] = data['MODEL_PREDICTIONS'].astype(float)  # Convert to numeric type
            
            print(data.columns)

            fig, ax = plt.subplots(figsize=(10, 6))
            test = test_arrest_pivot.xs((law_cat_cd, arrest_boro), level=('LAW_CAT_CD', 'ARREST_BORO'), axis=1)
            ax.plot(data["ARREST_DATE"], data["MODEL_PREDICTIONS"], label=f'ARREST_BORO={arrest_boro}, LAW_CAT_CD={law_cat_cd}')
            ax.plot(test, label="Actual")
            ax.set_title('Dataset Visualization for ' + arrest_boro +" and "+ law_cat_cd)
            ax.set_xlabel('Date')
            ax.set_ylabel('Arrest Count')
            ax.legend()
        
            plt.savefig("./data/visualization_"+arrest_boro+"_"+law_cat_cd+".png")
    


    
with DAG('VISUALIZATION_DAG', start_date = datetime(2023, 1, 1),
         schedule = '@daily', catchup = False) as dag:
    
    visualize_task = PythonOperator(
        task_id='visualize_dataset',
        python_callable=visualize_dataset
    )
    

    # Dependencias
    visualize_task