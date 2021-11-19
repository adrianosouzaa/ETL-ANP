from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

import logging
import urllib
import os 
import pandas as pd 
from subprocess import  Popen
import psycopg2 as pg
#from fastparquet import *
#import pyarrow.parquet

## Download XLS file from ANP

def download_xls():
        import urllib
        url = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
        response = urllib.request.urlretrieve(url, 'vendas-combustiveis-m3.xls')        

# Use LibreOffice to convert file to XLS to view pivot table cached

def converter_xls():
    p = Popen(['libreoffice', '--headless', '--convert-to', 'xls', '--outdir',
               'raw_data', 'vendas-combustiveis-m3.xls'])
    print(['libreoffice', '--convert-to', 'ods', './raw_data/vendas-combustiveis-m3.xls'])
    p.communicate()
    
    
def transform(sheetName, tableName):
    os.popen('mkdir staging')    
    df = pd.read_excel('./raw_data/vendas-combustiveis-m3.xls', sheet_name=sheetName)
    df.columns = ['Combustível', 'Ano', 'Região', 'UF', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'Total']
    df = df.melt(id_vars=['Combustível', 'Ano', 'Região', 'UF'])
    df = df.loc[df['variable'] != 'Total']
    df['year_month'] = df['Ano'].astype(str) + '-' + df['variable']
    df['year_month'] = pd.to_datetime(df['year_month'])
    df = df.drop(labels=['variable', 'Região', 'Ano'], axis=1)
    df.columns = ['product', 'uf', 'volume', 'year_month']
    df['volume'] = pd.to_numeric(df['volume'])
    df = df.fillna(0)
    df['unit'] = 'm3'
    diferenca = timedelta(hours=-3)
    fuso_horario = timezone(diferenca)
    data_hora_atual = datetime.now()
    data_hora_texto = data_hora_atual.strftime('%Y-%m-%d %H:%M:%S') # PADRONIZAÇÃO
    data_hora_sp = data_hora_atual.astimezone(fuso_horario)
    data_hora_sp = data_hora_sp.strftime('%Y-%m-%d %H:%M:%S')
    df['created_at'] = data_hora_sp
    df['created_at'] = pd.to_datetime(df['created_at'])
    df.to_csv('./staging/' + tableName + '.csv')
    
dag = DAG('ETL-ANP', description='ETL process',
          start_date=datetime(2020, 7, 1), catchup=False)




download_anp = PythonOperator(
        task_id='download_anp', 
        python_callable=download_xls,
        dag=dag)

converter_xls  = PythonOperator(
        task_id='converter_xls', 
        python_callable=converter_xls,
        dag=dag)

extract_derivated_fuels  = PythonOperator(
        task_id='extract_derivated_fuels', 
        python_callable=transform,
        op_kwargs={'sheetName': 1, 'tableName': 'derivated_fuels'},
        dag=dag)


extract_diesel  = PythonOperator(
        task_id='extract_diesel', 
        python_callable=transform,
        op_kwargs={'sheetName': 2, 'tableName': 'diesel'},
        dag=dag)

    
    
    
download_anp >> converter_xls >> [extract_derivated_fuels, extract_diesel]