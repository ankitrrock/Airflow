import re
import glob
import snowflake.connector
from airflow.models import Variable
import pandas as pd
import psycopg2 as pg2
import shutil
import setuptools
from time import sleep

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
from datetime import datetime
import gzip
import csv

WORKFLOW_DEFAULT_ARGS = {
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('NCS_DW_CONFIG_PE'
          ,description='PE Tables Full Configuration Extraction'
          ,default_args=WORKFLOW_DEFAULT_ARGS
          #,schedule='10 14 * * *'
          ,schedule=None
          ,start_date=datetime(2023, 6, 11)
          ,catchup=False
          ,max_active_runs=1
          ,max_active_tasks=10)


def excel_files():
    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))
    cur = con.cursor()

    KCA_CFG_BATCH_PARAM = pd.read_sql("select * from NCS_MDADM.KCA_CFG_BATCH_PARAM_HIST", con)
    print(KCA_CFG_BATCH_PARAM)
    KCA_CFG_BATCH_PARAM.to_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/excel_files/KCA_CFG_BATCH_PARAM.xlsx", index=False)

    KCA_CTRL_BATCH = pd.read_sql("select * from NCS_MDADM.KCA_CTRL_BATCH where BATCH_NAME='NCS_DW_CONFIG_PE' ORDER BY BATCH_START_DATE DESC LIMIT 1",con)
    KCA_CTRL_BATCH.to_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/excel_files/KCA_CTRL_BATCH_PE.xlsx",index=False)

    query1 = "select * from NCS_MDADM.KCA_CFG_BATCH_DEP_PE"
    data = pd.read_sql(query1, con)
    data.to_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/excel_files/data.xlsx", index=False)

    query2 = "select * from NCS_MDADM.KCA_ERP_TABLE_ROW_CNT_PE order by ROW_COUNT DESC"
    data1 = pd.read_sql(query2, con)
    data1.to_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/excel_files/KCA_ERP_TABLE_ROW_CNT.xlsx",index=False)

    data = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/excel_files/PE_data.xlsx")


def bip_ucm_start():
    # BATCH_NAME="KCA_BIP_ON_DEMAND"

    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))
    cur = con.cursor()

    full_or_inc = cur.execute("select VALUE from NCS_MDADM.KCA_CFG_BATCH_PARAM_HIST where PARAMETER_NAME='LOAD_TYPE'").fetchall()[0][0]
    df = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/excel_files/KCA_CFG_BATCH_PARAM.xlsx")

    # full_or_inc="".join(df[df["PARAM1"]=="LOAD_TYPE"]["PARAM2"].tolist())
    print("load type", full_or_inc)

    df1 = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/excel_files/KCA_CTRL_BATCH_PE.xlsx")

    # full_or_inc="FULL"
    if full_or_inc == "FULL":
																																				   
        start_date = "".join(df[df["PARAMETER_NAME"] == "SRC_EXTRACT_START_DATE"]["VALUE"].tolist())
										  
        print("******", full_or_inc, start_date)
    else:
																																															   
        start_date = "".join(df[df["PARAMETER_NAME"] == "SRC_EXTRACT_START_DATE"]["VALUE"].tolist())
        

        print("******", full_or_inc, start_date)
    
    start_date = datetime.strptime(str(start_date).split()[0], "%Y-%m-%d").strftime('%m-%d-%Y')

			 
																							  
    dt = len(df1)
    if dt == 0:
        end_date = datetime.now()+ timedelta(days=1)
    else:
																																												
        end_date = datetime.now()+ timedelta(days=1)
        

    end_date = datetime.strptime(str(end_date).split()[0], "%Y-%m-%d").strftime('%m-%d-%Y')

    en_path = str('/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/encryptedfiles/')
    de_path = str('/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/decrypted_files/')
														
    print(start_date)
    print(end_date)
    global bip_row
    bip_row = BashOperator(task_id='bulk_stats_fetch',
                           bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/bip_ondemand.sh '{{params.start_date}}' '{{params.end_date}}' '{{params.table_name}}' '{{params.min_records}}' '{{params.max_records}}' '{{params.en_path}}' '{{params.de_path}}'",
                           params={"start_date": start_date, "end_date": end_date,
                                   "table_name": "KCA_ERP_TABLE_ROW_CNT", "min_records": str(1),
                                   "max_records": str(100000), "en_path": en_path, "de_path": de_path}, dag=dag)
    global ucm_row
    ucm_row = BashOperator(task_id="bulk_stats_download",
                           bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/ucm_ondemand.sh '{{params.table_name}}' '{{params.de_path}}' '{{params.en_path}}'",
                           params={"table_name": "KCA_ERP_TABLE_ROW_CNT", "de_path": de_path, "en_path": en_path},
                           dag=dag)


def rowcount():
    data = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/excel_files/data.xlsx")
    data = data[(data["BATCH_NAME"] == "KCA_DWH_LOAD") & (data["DEPTH"] == 0)]
    data_list = []
    for x in list(data["JOB_NAME"]):
        data_list.append(x)

    file = glob.glob(Variable.get('PE_FILES') + "*.csv")
    for fi in file:
        print("fi is", fi)
        actual_filename = os.path.basename(fi)
        print(re.sub('_UCM\w+.csv$', "", actual_filename))
        print(re.sub('_UCM\w+.csv$', "", actual_filename) == "KCA_ERP_TABLE_ROW_CNT")
        if re.sub('_UCM\w+.csv$', "", actual_filename) == "KCA_ERP_TABLE_ROW_CNT":
            data1 = pd.read_csv(fi)
            print("dataframe", data1)
            data1 = data1[data1["TABLE_NAME"].isin(data_list)]
            print("data list is", data_list)
            import snowflake.connector
            from snowflake.sqlalchemy import URL
            from sqlalchemy import create_engine

            con = snowflake.connector.connect(
                account=Variable.get('SNOW_ACCOUNT'),
                user=Variable.get('SNOW_USER'),
                password=Variable.get('SNOW_PASSWORD'),
                role=Variable.get('SNOW_ROLE'),
                warehouse=Variable.get('SNOW_WAREHOUSE'),
                database=Variable.get('SNOW_DATABASE'),
                schema='NCS_MDADM')
            cur = con.cursor()
            cur.execute("TRUNCATE TABLE NCS_MDADM.KCA_ERP_TABLE_ROW_CNT_PE")

            engine = create_engine(URL(
                account=Variable.get('SNOW_ACCOUNT'),
                user=Variable.get('SNOW_USER'),
                password=Variable.get('SNOW_PASSWORD'),
                role=Variable.get('SNOW_ROLE'),
                warehouse=Variable.get('SNOW_WAREHOUSE'),
                database=Variable.get('SNOW_DATABASE'),
                schema='NCS_MDADM'))
            connection = engine.connect()
            print("data1 is", data1)
            data1.to_sql('KCA_ERP_TABLE_ROW_CNT_PE', con=engine, index=False, if_exists="append")

            connection.close()
            engine.dispose()
            con.close()
            #destination = "/home/oodles/ncs_multistage/apps/dwh_data/extracts/pe_extracts/archieve_files/"
            #shutil.move(fi, destination + actual_filename)
            sleep(5)


def load_stats():
    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))
    cur = con.cursor()

    query2 = "select * from NCS_MDADM.KCA_ERP_TABLE_ROW_CNT_PE order by ROW_COUNT DESC"
    data1 = pd.read_sql(query2, con)
    data1.to_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/pe_files/excel_files/KCA_ERP_TABLE_ROW_CNT.xlsx",
                   index=False)


bip_ucm_start()

excel = PythonOperator(task_id='excel_files', python_callable=excel_files, dag=dag)
# row_calc = PythonOperator(task_id='row_calc', python_callable=bip_ucm_start, dag=dag)
load_stat = PythonOperator(task_id='load_stats', python_callable=load_stats, dag=dag)
row_count = PythonOperator(task_id='row_counts', python_callable=rowcount, dag=dag)
trigger__dag_ncs_dw_extract_pe=TriggerDagRunOperator(task_id="Trigger_Dag_NCS_DW_EXTRACT_PE",trigger_dag_id="NCS_DW_EXTRACT_PE",dag=dag)
config_end = DummyOperator(task_id='config_end', dag=dag)

excel >> bip_row >> ucm_row >> row_count >> load_stat >> trigger__dag_ncs_dw_extract_pe >> config_end
#excel >> bip_row >> ucm_row >> row_count >> load_stat >> config_end