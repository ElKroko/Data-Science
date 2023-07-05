from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import matplotlib.pyplot as plt

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
    cursor.execute("SELECT ARREST_BORO, LAW_CAT_CD, MODEL_PREDICTIONS FROM predictions")

    # Fetch all the rows from the query result
    rows = cursor.fetchall()

    # Close the cursor and connection
    cursor.close()
    conn.close()
    
    # Create a DataFrame from the fetched rows
    df = pd.DataFrame(rows, columns=['ARREST_BORO', 'LAW_CAT_CD', 'MODEL_PREDICTIONS'])

    if df.empty:
        raise ValueError("DataFrame is empty. No data to visualize")
    
    # test_data = extract_data("test")
    
    # test_arrest_count = test_data.groupby([pd.Grouper(key='ARREST_DATE', freq='W'), 'LAW_CAT_CD', 'ARREST_BORO']).size().reset_index(name='ARREST_COUNT')
    # test_arrest_pivot = test_arrest_count.pivot(index='ARREST_DATE', columns=['LAW_CAT_CD', 'ARREST_BORO'], values='ARREST_COUNT')
    # test_arrest_pivot = test_arrest_pivot.fillna(0)


    # Perform any necessary data processing or transformation

    # Visualize the dataset per ARREST_BORO and LAW_CAT_CD
    fig, ax = plt.subplots(figsize=(10, 6))
    # Add your visualization code here, using matplotlib or other libraries
    
    for (arrest_boro, law_cat_cd), group in df.groupby(['ARREST_BORO', 'LAW_CAT_CD']):
        
        # test = test_arrest_pivot.xs((law_cat_cd, arrest_boro), level=('LAW_CAT_CD', 'ARREST_BORO'), axis=1)
        ax.plot(df["MODEL_PREDICTIONS"], label=f'ARREST_BORO={arrest_boro}, LAW_CAT_CD={law_cat_cd}')
        # ax.plot(test, label="Actual")
        ax.set_title('Dataset Visualization by ARREST_BORO and LAW_CAT_CD')
        ax.set_xlabel('Date')
        ax.set_ylabel('Arrest Count')
        
        plt.savefig("./data/visualization"+arrest_boro+law_cat_cd+".png")
    # Display the visualization (optional)
    plt.show()



# En caso de que salga mal, botamos la tabla de Predicciones.

    
with DAG('VISUALIZATION_DAG', start_date = datetime(2023, 1, 1),
         schedule = '@daily', catchup = False) as dag:
    
    visualize_task = PythonOperator(
        task_id='visualize_dataset',
        python_callable=visualize_dataset
    )
    

    # Dependencias
    visualize_task