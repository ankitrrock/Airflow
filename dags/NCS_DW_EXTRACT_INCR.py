# importing python libraries
import airflow
import re
import glob
import snowflake.connector
from airflow.models import Variable
import pandas as pd
import psycopg2 as pg2
import shutil

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
from datetime import datetime
from time import sleep
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email

BATCH_NAME = "NCS_DW_EXTRACT_INCR"

WORKFLOW_DEFAULT_ARGS = {
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email':['vikas.sanwal@oodles.io',"ankit.anand@oodles.io"],
    'email_on_failure':True,
    'email_on_retry':False,
}

dag = DAG('NCS_DW_EXTRACT_INCR'
          ,description='NCS_DW_EXTRACT_INCR'
          ,default_args=WORKFLOW_DEFAULT_ARGS
          ,schedule=None
          #,schedule='0 10 * * *'
          ,start_date=datetime(2022, 11, 8)
          ,catchup=False
          ,max_active_runs=1
          ,max_active_tasks=10)


def extraction_start():
    abc = glob.glob(Variable.get("UCM_DECRYPT_PATH_INCR") + "*")
    if len(abc) > 0:
        time = str(datetime.now()).split(".")[0].replace("-", "").replace(" ", "").replace(":", "")
        print(time)
        zip_path = '/home/oodles/ncs_multistage/apps/dwh_data/extracts/erp_extracts/archieve_files/incr/' + time + '/'
        print(zip_path)
        shutil.make_archive(zip_path, 'zip', Variable.get("UCM_DECRYPT_PATH_INCR"))
        for x in abc:
            os.remove(x)

    BATCH_NAME = "NCS_DW_EXTRACT_INCR"

    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))
    cur = con.cursor()

    # Resuming the warehouse if suspended
    cur.execute("alter warehouse resume if suspended")
    # Inserting the running record into batch table to log the batch details
    cur.execute(
        "INSERT INTO NCS_MDADM.KCA_CTRL_BATCH(BATCH_ID,BATCH_NAME,BATCH_LOAD_TYPE,BATCH_STATUS,BATCH_START_DATE,SRC_EXTR_START_DATE,SRC_EXTR_END_DATE,PRUNE_DAYS,SOURCE_APP_ID) SELECT   NCS_MDADM.SEQ_JOB_ID.NEXTVAL BATCH_ID,'" + BATCH_NAME + "' BATCH_NAME ,case when (select VALUE from NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR where PARAMETER_NAME='LOAD_TYPE') ='FULL' THEN 'FULL' ELSE 'INCR' END BATCH_LOAD_TYPE,'RUNNING' BATCH_STATUS,current_timestamp::timestamp_ntz BATCH_START_DATE ,case when (select VALUE from NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR where PARAMETER_NAME='LOAD_TYPE') ='FULL' THEN (select to_timestamp_ntz(VALUE,'YYYY-MM-DD HH:MI:SS') FROM NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR WHERE PARAMETER_NAME='SRC_EXTRACT_START_DATE') ELSE  NVL((SELECT to_timestamp(to_date(SRC_EXTR_END_DATE )-(SELECT  to_number(VALUE) FROM NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR  WHERE PARAMETER_NAME='PRUNE_DAYS')|| ' '||to_time(SRC_EXTR_END_DATE)) FROM (SELECT SRC_EXTR_END_DATE FROM (SELECT SRC_EXTR_END_DATE,                    ROW_NUMBER() OVER (ORDER BY SRC_EXTR_END_DATE DESC) AS RN FROM  NCS_MDADM.KCA_CTRL_BATCH WHERE BATCH_NAME='" + BATCH_NAME + "' AND  BATCH_STATUS = 'COMPLETED' AND SRC_EXTR_END_DATE IS NOT NULL ) WHERE RN = 1)),(select to_timestamp_ntz(VALUE,'YYYY-MM-DD HH:MI:SS') FROM NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR WHERE PARAMETER_NAME='SRC_EXTRACT_START_DATE')) END SRC_EXTR_START_DATE,to_timestamp(to_char(current_date+1) ||' ' ||to_char(current_time)),(SELECT  to_number(VALUE) FROM NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR  WHERE PARAMETER_NAME='PRUNE_DAYS') PRUNE_DAYS,(select APP_ID FROM NCS_MDADM.KCA_CFG_APP_PARAM WHERE APPLICATION_NAME ='ERP' ) SOURCE_APP_ID")

    KCA_CFG_BATCH_PARAM = pd.read_sql("select * from NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR", con)
    KCA_CFG_BATCH_PARAM.to_excel(
        "/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_CFG_BATCH_PARAM.xlsx", index=False)

    KCA_CTRL_BATCH = pd.read_sql(
        "select * from NCS_MDADM.KCA_CTRL_BATCH where BATCH_NAME='NCS_DW_EXTRACT_INCR' ORDER BY BATCH_START_DATE DESC LIMIT 1",
        con)
    KCA_CTRL_BATCH.to_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_CTRL_BATCH.xlsx",
                            index=False)

    query1 = "select * from NCS_MDADM.KCA_CFG_BATCH_DEP_INCR"
    data = pd.read_sql(query1, con)
    data.to_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/data.xlsx", index=False)

    PE_files = pd.read_sql(
        "select JOB_NAME FROM NCS_MDADM.KCA_CFG_BATCH_DEP_INCR WHERE JOB_NAME LIKE ('%_PE') AND DEPTH=0", con)
    PE_files.to_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/PE_files.xlsx", index=False)

    query2 = "select * from NCS_MDADM.KCA_ERP_TABLE_ROW_CNT_INCR order by ROW_COUNT DESC"
    data1 = pd.read_sql(query2, con)
    data1.to_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_ERP_TABLE_ROW_CNT.xlsx",
                   index=False)

    
    full_or_inc = cur.execute("select VALUE from NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR where PARAMETER_NAME='LOAD_TYPE'").fetchall()[0][0]

    data1 = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/data.xlsx")
    data1 = data1[(data1["BATCH_NAME"] == "KCA_DWH_LOAD") & (data1["DEPTH"] == 0)]
    data_list1=[]
    for x1 in list(data1["JOB_NAME"]):
        data_list1.append(x1)
    print("data list complete")                          

    if full_or_inc=='FULL':
        for JOB_NAME in data_list1:
            trunc= "TRUNCATE TABLE NP_ODS.{};".format(JOB_NAME)
            print("trunc is",trunc)
            cur.execute(trunc)
            print("truncated",JOB_NAME)


def bip_start():
    # BATCH_NAME="KCA_BIP_ON_DEMAND"

    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))

    cur = con.cursor()
    # Resuming the warehouse if suspended
    cur.execute("alter warehouse resume if suspended")
    print("starting query")
    q11 = "insert into NCS_MDADM.KCA_CTRL_JOB (JOB_ID,BATCH_ID,JOB_SOURCE,JOB_TARGET,JOB_STATUS,JOB_TYPE,JOB_START_DATE,JOB_END_DATE) select NCS_MDADM.SEQ_JOB_CTRL_ID.NEXTVAL job_id,(select BATCH_ID from (SELECT BATCH_ID,ROW_NUMBER() OVER (ORDER BY BATCH_START_DATE DESC) AS RN FROM NCS_MDADM.KCA_CTRL_BATCH WHERE BATCH_STATUS IN ('RUNNING','FAILED') AND BATCH_NAME ='NCS_DW_EXTRACT_INCR')WHERE RN=1) BATCH_ID,'BIP_ON_DEMAND' JOB_SOURCE ,'UCM' JOB_TARGET,'RUNNING', 'BASH_SCRIPTS' JOB_TYPE,current_timestamp::timestamp_ntz JOB_START_DATE,NULL JOB_END_DATE;"
    cur.execute(q11)


# Connecting to SnowFlake and running sql query to update data
def bip_success():
    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))

    cur = con.cursor()
    # Post updating completed status to batch
    cur.execute(
        "update NCS_MDADM.KCA_CTRL_JOB set JOB_END_DATE=current_timestamp::timestamp_ntz,JOB_STATUS='COMPLETED' where JOB_SOURCE = 'BIP_ON_DEMAND' and BATCH_ID = (select MAX(BATCH_ID) from NCS_MDADM.KCA_CTRL_JOB order by BATCH_ID desc limit 1);")


def ucm_start():
    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))

    cur = con.cursor()
    cur.execute("alter warehouse resume if suspended")

    q12 = "insert into NCS_MDADM.KCA_CTRL_JOB (JOB_ID,BATCH_ID,JOB_SOURCE,JOB_TARGET,JOB_STATUS,JOB_TYPE,JOB_START_DATE,JOB_END_DATE) select NCS_MDADM.SEQ_JOB_CTRL_ID.NEXTVAL job_id,(select BATCH_ID from (SELECT BATCH_ID,ROW_NUMBER() OVER (ORDER BY BATCH_START_DATE DESC) AS RN FROM NCS_MDADM.KCA_CTRL_BATCH WHERE BATCH_STATUS IN ('RUNNING','FAILED') AND BATCH_NAME ='NCS_DW_EXTRACT_INCR')WHERE RN=1) BATCH_ID,'UCM_EXTRACT' JOB_SOURCE ,'VM' JOB_TARGET,'RUNNING', 'BASH_SCRIPTS' JOB_TYPE,current_timestamp::timestamp_ntz JOB_START_DATE,NULL JOB_END_DATE;"
    cur.execute(q12)


# Connecting to SnowFlake and running sql query to update data
def ucm_success():
    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))

    cur = con.cursor()
    # Post updating completed status to batch
    cur.execute(
        "update NCS_MDADM.KCA_CTRL_JOB set JOB_END_DATE=current_timestamp::timestamp_ntz,JOB_STATUS='COMPLETED' where JOB_SOURCE = 'UCM_EXTRACT' and BATCH_ID = (select MAX(BATCH_ID) from NCS_MDADM.KCA_CTRL_JOB order by BATCH_ID desc limit 1);")


def extraction_complete():
    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))

    cur = con.cursor()

    # Resuming the warehouse if suspended
    cur.execute("alter warehouse resume if suspended")
    # Inserting the running record into batch table to log the batch details
    cur.execute(
        "update NCS_MDADM.KCA_CTRL_BATCH set BATCH_STATUS='COMPLETED',BATCH_END_DATE=current_timestamp::timestamp_ntz WHERE BATCH_NAME='NCS_DW_EXTRACT_INCR' AND BATCH_ID=(select MAX(BATCH_ID) from NCS_MDADM.KCA_CTRL_BATCH);")


# Connecting to Postgres, and running sql command. If error is present, it will insert the error message into log table.
def batch_error():
    BATCH_NAME = "NCS_DW_EXTRACT_INCR"
    connpg2 = pg2.connect(
        user=Variable.get('POSTGRES_USER'),
        password=Variable.get('POSTGRES_PASSWORD'),
        database=Variable.get('POSTGRES_DATABASE'),
        host=Variable.get('POSTGRES_HOST'),
        port=Variable.get('POSTGRES_PORT'))
    curpg2 = connpg2.cursor()
    curpg2.execute(
        "select task_id, dag_id, execution_date, state , job_id from task_instance where job_id=(select max(job_id) from task_instance where state='failed')")

    datapg2 = curpg2.fetchall()
    # creating a dataframe
    datapg2 = pd.DataFrame(datapg2, columns=['task_id', 'dag_id', 'execution_date', 'state', 'job_id'])

    if len(datapg2) == 0:
        print("No errors in the current batch")

    # this will take the latest log file in the folder
    else:
        LOG_PATH = str(Variable.get('LOG_PATH') + "/" + datapg2['dag_id'][0] + "/" + datapg2['task_id'][0])
        FULL_PATH = str(max([os.path.join(LOG_PATH, d) for d in os.listdir(LOG_PATH)], key=os.path.getmtime))

    list_of_files = glob.glob(FULL_PATH + '/*.log')
    latest_log = max(list_of_files, key=os.path.getctime)

    print(latest_log)

    # Reading the log file
    f = open(latest_log, 'r')
    f.seek(0)
    liss = f.readlines()
    count = 0
    b = []

    # This will read the error message from the postgres log file
    for x in liss:
        if "ERROR" in x:
            b.append(liss[count:count + 3])
            count = count + 1
        else:
            count = count + 1
    errm = "".join(str(b)).replace("\\n", "").replace("'", "").replace(":,", ".")
    print(errm)
    f.close()

    # Connecting to SnowFlake to enter the error details in the log table
    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))
    cur = con.cursor()

    # inserting the data into error log table
    query = "INSERT INTO NCS_MDADM.KCA_CTRL_JOB_LOG (JOB_ID,BATCH_ID,JOB_NAME,JOB_LOG_SUMMARY) Select NVL((select max(job_id) from NCS_MDADM.KCA_CTRL_JOB where JOB_STATUS='RUNNING'),NCS_MDADM.SEQ_JOB_CTRL_ID.NEXTVAL) JOB_ID,(select max(BATCH_ID) from NCS_MDADM.KCA_CTRL_BATCH where BATCH_STATUS='RUNNING'),'" + \
            datapg2['task_id'][0] + "','" + errm + "'"
    print(query)

    query2 = "update NCS_MDADM.KCA_CTRL_JOB set JOB_STATUS='FAILED',JOB_END_DATE=current_timestamp::timestamp_ntz WHERE JOB_ID IN (select MAX(JOB_ID) from NCS_MDADM.KCA_CTRL_JOB);"

    query3 = "update NCS_MDADM.KCA_CTRL_BATCH set BATCH_STATUS='FAILED',BATCH_END_DATE=current_timestamp::timestamp_ntz WHERE BATCH_ID IN (select MAX(BATCH_ID) from NCS_MDADM.KCA_CTRL_JOB ORDER BY BATCH_START_DATE DESC  LIMIT 1);"

    cur.execute(query)
    cur.execute(query2)
    cur.execute(query3)
    raise ValueError('Error ' + errm)



#main code
con = snowflake.connector.connect(
    account=Variable.get('SNOW_ACCOUNT'),
    user=Variable.get('SNOW_USER'),
    password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
    warehouse=Variable.get('SNOW_WAREHOUSE'),
    database=Variable.get('SNOW_DATABASE'))
cur = con.cursor()

full_or_inc = cur.execute("select VALUE from NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR where PARAMETER_NAME='LOAD_TYPE'").fetchall()[0][0]
df = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_CFG_BATCH_PARAM.xlsx")

# full_or_inc="".join(df[df["PARAMETER_NAME"]=="LOAD_TYPE"]["VALUE"].tolist())
print("load type", full_or_inc)

df1 = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_CTRL_BATCH.xlsx")

# full_or_inc="FULL"
if full_or_inc == "FULL":
    # start_date=cur.execute("select VALUE from EDWUAT.NCS_MDADM.KCA_CFG_BATCH_PARAM where PARAMETER_NAME='SRC_EXTRACT_START_DATE'").fetchall()[0][0]
    start_date = "".join(df[df["PARAMETER_NAME"] == "SRC_EXTRACT_START_DATE"]["VALUE"].tolist())
    # start_date=datetime(2022, 8, 10)
    print("date is", start_date)
    print("******", full_or_inc, start_date)
else:
    # start_date=cur.execute("select SRC_EXTR_START_DATE from EDWUAT.NCS_MDADM.KCA_CTRL_BATCH where BATCH_NAME='KCA_DWH_EXTRACT' ORDER BY BATCH_START_DATE DESC LIMIT 1").fetchall()[0][0]
    start_date = "".join(df[df["PARAMETER_NAME"] == "SRC_EXTRACT_START_DATE"]["VALUE"].tolist())
    #start_date = "".join(df1["SRC_EXTR_START_DATE"].astype('str').tolist())

    print("******", full_or_inc, start_date)
    # start_date=datetime(2010, 8, 10)
start_date = datetime.strptime(str(start_date).split()[0], "%Y-%m-%d").strftime('%m-%d-%Y')
print(start_date)

# enddate
# dt=cur.execute("select count(*) from EDWUAT.NCS_MDADM.KCA_CTRL_BATCH").fetchall()[0][0]
dt = len(df1)
if dt == 0:
    end_date = datetime.now()
else:
    # end_date=cur.execute("select MAX(SRC_EXTR_END_DATE) FROM EDWUAT.NCS_MDADM.KCA_CTRL_BATCH where BATCH_NAME='KCA_DWH_EXTRACT' ORDER BY BATCH_ID DESC").fetchall()[0][0]
                                                                         
    end_date = datetime.now()
    #end_date = "".join(df1["SRC_EXTR_END_DATE"].astype('str').tolist())

end_date = datetime.strptime(str(end_date).split()[0], "%Y-%m-%d").strftime('%m-%d-%Y')

# start_date="08-20-2020"
# end_date="09-08-2020"

en_path = str(Variable.get('UCM_ENCRYPT_PATH_INCR'))
de_path = str(Variable.get('UCM_DECRYPT_PATH_INCR'))

# query1="select JOB_NAME from EDWUAT.NCS_MDADM.KCA_CFG_BATCH_DEP_TEST where BATCH_NAME='KCA_DWH_LOAD' and DEPTH=0"
# data=pd.read_sql(query1,con)

data = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/data.xlsx")
data = data[(data["BATCH_NAME"] == "KCA_DWH_LOAD") & (data["DEPTH"] == 0)]
print(data)
data_list=[]
for x in list(data["JOB_NAME"]):
    data_list.append(x)

max_records=int(df[df["PARAMETER_NAME"]=="NO_OF_ROWS"]["VALUE"])
row_add=int(df[df["PARAMETER_NAME"]=="NO_OF_ROWS"]["VALUE"])
#max_records = 100000
min_records = 1

ucm1 = []
z = []
#bulk_file=["PER_ALL_ASSIGNMENTS_M","PER_MANAGER_HRCHY_DN"]
#bulk_file = ["REQUEST_PROPERTY", "CMP_SALARY"]
x="/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_ERP_TABLE_ROW_CNT.xlsx"
bulk_file=[]
data2=pd.read_excel(x)
sorted_column = data2.sort_values(['ROW_COUNT'], ascending = False)
a=data2[data2['ROW_COUNT'] > max_records]
b=a.values.tolist()
for i in b:
    if i not in bulk_file:
        bulk_file.append(i[0])
print("final list is",bulk_file)

# PE_files=pd.read_sql("select JOB_NAME FROM EDWUAT.NCS_MDADM.KCA_CFG_BATCH_DEP_TEST WHERE JOB_NAME LIKE ('%_PE') AND DEPTH=0",con)
PE_files = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/PE_files.xlsx")
PE_files = list(PE_files["JOB_NAME"])
bulk_pe_files = bulk_file + PE_files



if full_or_inc == 'FULL':
    for x in list(data["JOB_NAME"]):
    #for x in list(data["JOB_NAME","TABLE_RECORDS"]):
        if x not in bulk_pe_files:
            table_name = x
            # min_records=1
            # max_records=1000000
            # max_records=cur.execute("select VALUE FROM EDWUAT.NCS_MDADM.KCA_CFG_BATCH_PARAM WHERE PARAMETER_NAME='NO_OF_ROWS'").fetchall()[0][0]

            task1 = BashOperator(task_id=table_name + "_BIP",
                                 bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/bip_ondemand.sh '{{params.start_date}}' '{{params.end_date}}' '{{params.table_name}}' '{{params.min_records}}' '{{params.max_records}}'",
                                 params={"start_date": start_date, "end_date": end_date, "table_name": table_name,
                                         "min_records": str(min_records), "max_records": str(max_records)}, dag=dag)
            z.append(task1)

            task2 = BashOperator(task_id=table_name + "_UCM",
                                 bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/ucm_ondemand.sh '{{params.table_name}}' '{{params.de_path}}' '{{params.en_path}}'",
                                 params={"table_name": table_name, "de_path": de_path, "en_path": en_path}, dag=dag)

            ucm1.append(task2)

elif full_or_inc == 'INCR':
    for x in list(data["JOB_NAME"]):
        if x not in bulk_pe_files:
            table_name = x
            # min_records=1
            # max_records=1000000
            # max_records=cur.execute("select VALUE FROM EDWUAT.NCS_MDADM.KCA_CFG_BATCH_PARAM WHERE PARAMETER_NAME='NO_OF_ROWS'").fetchall()[0][0]

            task1 = BashOperator(task_id=table_name+ "_BIP",
                                 bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/bip_ondemand.sh '{{params.start_date}}' '{{params.end_date}}' '{{params.table_name}}' '{{params.min_records}}' '{{params.max_records}}'",
                                 params={"start_date": start_date, "end_date": end_date, "table_name": table_name,
                                     "min_records": str(min_records), "max_records": str(max_records)}, dag=dag)
            z.append(task1)

            task2 = BashOperator(task_id=table_name+ "_UCM",
                                 bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/ucm_ondemand.sh '{{params.table_name}}' '{{params.de_path}}' '{{params.en_path}}'",
                                 params={"table_name": table_name, "de_path": de_path, "en_path": en_path}, dag=dag)

            ucm1.append(task2)

# nof=int(cur.execute("SELECT VALUE FROM EDWUAT.NCS_MDADM.KCA_CFG_BATCH_PARAM WHERE PARAMETER_NAME='NO_OF_FILES'").fetchall()[0][0])
nof = int(df[df["PARAMETER_NAME"] == "NO_OF_FILES"]["VALUE"])
# nof=10
zz = [z[i:i + nof] for i in range(0, len(z), nof)]
n = len(zz)
print(zz, n)

# Dummy_Operator
dummy = []
for r in range(n + 1):
    dummy.append(EmptyOperator(task_id="dummy" + str(r), dag=dag))

Cur_DateTime = datetime.now()
ENV="NCS Production Instance"
MAIL_ID = ['vikas.sanwal@oodles.io',"ankit.anand@oodles.io"]
mail_body = f"""
<html>
<body>
    <h1 style="color: green;; font-size: 12px">&#128994; {BATCH_NAME} DAG Production Load Completed Successfully</h1>
    <p>Load Name: {BATCH_NAME}</p>
    <p>Env: {ENV}</p>
    <p>Time: {Cur_DateTime}</p>
</body>
</html>
"""
success_mail = EmailOperator(task_id='Success_mail',to=MAIL_ID,subject='Production Load Completed Successfully',html_content=mail_body,dag=dag)

# operator
extraction_start = PythonOperator(task_id='extraction_start', python_callable=extraction_start, dag=dag)

bip_start = PythonOperator(task_id='bip_start', python_callable=bip_start, dag=dag)

bip_success = PythonOperator(task_id='bip_success', python_callable=bip_success, dag=dag)

ucm_start = PythonOperator(task_id='ucm_start', python_callable=ucm_start, dag=dag)

ucm_success = PythonOperator(task_id='ucm_success', python_callable=ucm_success, dag=dag)

extraction_complete = PythonOperator(task_id='extraction_complete', python_callable=extraction_complete, dag=dag)

trigger_dag_ncs_dw_load_incr =TriggerDagRunOperator(task_id="trigger_ncs_dw_load_incr",trigger_dag_id="NCS_DW_LOAD_INCR",dag=dag)

batch_error = PythonOperator(task_id='batch_error', python_callable=batch_error, trigger_rule='one_failed', dag=dag)

# workflow
extraction_start >> bip_start >> dummy[0]

for i in range(n):
    dummy[i] >> zz[i] >> dummy[i + 1]


dummy[n] >> bip_success >> ucm_start >> ucm1 >> ucm_success >> extraction_complete >> success_mail >> trigger_dag_ncs_dw_load_incr >> batch_error
#dummy[n] >> bip_success >> ucm_start >> ucm1 >> ucm_success >> extraction_complete >> batch_error




# bulkcode


df2 = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_ERP_TABLE_ROW_CNT.xlsx")

if full_or_inc == "FULL" or "INCR":
    new_dict = {new_list: [] for new_list in bulk_file}
    # biprow


    for x in bulk_file:
        table_name = x
        # no_of_rows= cur.execute("select ROW_COUNT from EDWUAT.NCS_MDADM.KCA_ERP_TABLE_ROW_CNT where TABLE_NAME='"+x+"' order by ROW_COUNT DESC").fetchall()[0][0]
        no_of_rows = df2[df2["TABLE_NAME"] == x]["ROW_COUNT"]
        print(x, no_of_rows)
        no_of_rows = int(df2[df2["TABLE_NAME"] == x]["ROW_COUNT"])
        no_of_rows = int(no_of_rows)
        #max_records=int(df[df["PARAMETER_NAME"]=="NO_OF_ROWS"]["VALUE"])
        tbl_max_records=int(data[data["JOB_NAME"]==x]["TBL_MAX_RECORDS"])
        tbl_row_add=int(data[data["JOB_NAME"]==x]["TBL_MAX_RECORDS"])
        min_records = 1

        import math

        iterations = math.ceil(no_of_rows / tbl_max_records)
        print("no of rows is",no_of_rows)
        print("max records is",tbl_max_records)
        print("table is",table_name)
        print("iterations is",iterations)
        if iterations == 1 or iterations == 0:

            task1 = BashOperator(task_id=table_name+"_BIP",
                                 bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/bip_ondemand.sh '{{params.start_date}}' '{{params.end_date}}' '{{params.table_name}}' '{{params.min_records}}' '{{params.max_records}}'",
                                 params={"start_date": start_date, "end_date": end_date, "table_name": table_name,
                                         "min_records": str(min_records), "max_records": str(tbl_max_records)}, dag=dag)
            new_dict[x].append(task1)

            task2 = BashOperator(task_id=table_name + "_UCM",
                                 bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/ucm_ondemand.sh '{{params.table_name}}' '{{params.de_path}}' '{{params.en_path}}'",
                                 params={"table_name": table_name, "de_path": de_path, "en_path": en_path}, dag=dag)
            new_dict[x].append(task2)

        elif iterations > 1:
            for num in range(1, iterations + 1):
                table_name = x
                if num == 1:
                    table_namee = table_name
                else:
                    table_namee = table_name + str(num)

                task1 = BashOperator(task_id=table_namee + "_BIP",
                                     bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/bip_ondemand.sh '{{params.start_date}}' '{{params.end_date}}' '{{params.table_name}}' '{{params.min_records}}' '{{params.max_records}}'",
                                     params={"start_date": start_date, "end_date": end_date, "table_name": table_name,
                                             "min_records": str(min_records), "max_records": str(tbl_max_records)}, dag=dag)
                print(table_name)
                print("tbl max for iteration greater 1",tbl_max_records)
                new_dict[x].append(task1)
                row_addition = tbl_row_add
                #min_records = min_records + row_addition
                min_records = tbl_max_records
                tbl_max_records = tbl_max_records + row_addition
                print("min is",min_records)
                print("max is",tbl_max_records)

                task2 = BashOperator(task_id=table_namee  + "_UCM",
                                     bash_command="/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/scripts/ucm_ondemand.sh '{{params.table_name}}' '{{params.de_path}}' '{{params.en_path}}'",
                                     params={"table_name": table_name, "de_path": de_path, "en_path": en_path}, dag=dag)

                new_dict[x].append(task2)
                print("new dict is", new_dict)

    # workflow of bulkfile

    for x1 in bulk_file:
        bip_start >> new_dict[x1][0]

    for x1 in bulk_file:
        for y1 in range(len(new_dict[x1]) - 1):
            new_dict[x1][y1] >> new_dict[x1][y1 + 1]

    for x1 in bulk_file:
        le = len(new_dict[x1]) - 1
        new_dict[x1][le] >> ucm_success

# end of bulkfile code
