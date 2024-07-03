import os
from datetime import datetime, timedelta
from airflow import DAG
import re
import glob
import snowflake.connector
from airflow.models import Variable
import pandas as pd
import math
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from os import environ
import psycopg2 as pg2
from dateutil.relativedelta import relativedelta
import time
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email

# Assigning DAG Name to a variable so that we can use where needed
BATCH_NAME = "NCS_DW_LOAD_INCR"

WORKFLOW_DEFAULT_ARGS = {
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email':['vikas.sanwal@oodles.io',"ankit.anand@oodles.io"],
    'email_on_failure':True,
    'email_on_retry':False,
}

# Dag name and desciption
dag = DAG('NCS_DW_LOAD_INCR'
          , description='NCS_DW_LOAD_INCR'
          , default_args=WORKFLOW_DEFAULT_ARGS
          , schedule_interval=None
          #, schedule_interval='0 10 * * *'
          , start_date=datetime(2022, 11, 8)
          , catchup=False
          , max_active_runs=1
          , max_active_tasks=8)

"""
def snowflake_con(*op_args):
    con = snowflake.connector.connect (
    account  = Variable.get('SNOW_ACCOUNT'),
    user     = Variable.get('SNOW_USER'),
    password = Variable.get('SNOW_PASSWORD'),
    warehouse = Variable.get('SNOW_WAREHOUSE'),
    database =Variable.get('SNOW_DATABASE'),
    schema   = Variable.get('SNOW_SCHEMA'))
    cur = con.cursor()
    return cur
"""


def sql_runner(*op_args):
    global BATCH_NAME

    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'),
        schema=Variable.get('SNOW_SCHEMA'))

    # Creating a cursor
    cur = con.cursor()

    cur.execute(
        "UPDATE NCS_MDADM.KCA_CTRL_BATCH SET BATCH_STATUS='RUNNING' WHERE BATCH_START_DATE IN (SELECT MAX(BATCH_START_DATE) FROM NCS_MDADM.KCA_CTRL_BATCH) AND BATCH_STATUS IN('FAILED','COMPLETED') AND BATCH_ID IN (select MAX(BATCH_ID) from NCS_MDADM.KCA_CTRL_BATCH)")
    # cur.execute("USE ROLE " + Variable.get('SNOW_ROLE'))
    # For full load the file and SQL path being set
    # full_or_inc = cur.execute("select VALUE from NCS_MDADM.KCA_CFG_BATCH_PARAM where PARAMETER_NAME='LOAD_TYPE'").fetchall()[0][0]

    df = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_CFG_BATCH_PARAM.xlsx")
    full_or_inc = "".join(df[df["PARAMETER_NAME"] == "LOAD_TYPE"]["VALUE"].tolist())

    full = cur.execute("select 'FULL'").fetchall()[0][0]
    if full_or_inc == full:
        print("Set Path for FULL Load")
        file_path = Variable.get("UCM_DECRYPT_PATH_INCR")
        sql_path = Variable.get("KCA_SQL_PATH_FULL")
    else:
        print("Set path for INCREMENTAL Load")
        file_path = Variable.get("UCM_DECRYPT_PATH_INCR")
        sql_path = Variable.get("KCA_SQL_PATH_INC")

    # Create a list containing all the files that need to be loaded
    actual_filenames_list = []
    print("Loading filelist")
    print(os.scandir(file_path))

    # Stores all the files present in that path
    listOfEntries = os.scandir(file_path)
    print("entering depth condition")
    # depth 0 will be considered for the files. If depth is equal to 0,files will be loaded.
    print(type(op_args[1]['depth'].values.tolist()[0]))
    print(op_args[1]['depth'].values.tolist()[0] == 0)
    if op_args[1]['depth'].values.tolist()[0] == 0:
        for entry in listOfEntries:
            if entry.is_file():
                # Removing extension from filename
                actual_filenames_list.append(os.path.splitext(entry.name)[0])
                print("actual_filename_list")

        get_param = ''
        for actual_filename in actual_filenames_list:
            # Using Regular expression to remove timestamp from filename
            # fname_without_tstamp=re.sub('[0-9]', '', actual_filename)
            fname_without_tstamp = re.sub('UCM\w+', "", actual_filename)
            print("check", op_args[0])
            print("fname", fname_without_tstamp)
            print("check", op_args[0] == fname_without_tstamp[:-1])
            if op_args[0] == fname_without_tstamp[:-1]:
                values = cur.execute(
                    "select TGT_TABLE, STG_TABLE,MERGE_CONDITION from NCS_MDADM.KCA_CFG_JOB_PARAM where JOB_NAME='" + fname_without_tstamp[
                                                                                                       :-1] + "'").fetchall()
                print(values)

                # Calling blob encryption method
                # file_cse_to_blob(filename=file_path + actual_filename)
                # this will trigger the sql file for each table
                for val in range(len(values)):
                    # print('{0}, {1}, {2}'.format(TGT_TAB_NAME, SRC_TAB_NAME, JOIN_CND))
                    get_param = (values[val])
                    print(get_param)
                    # get_param= values[val]
                file_nm = str(sql_path + fname_without_tstamp[:-1] + ".sql")
                # file = open(str(sql_path+fname_without_tstamp[:-1]+".sql"), 'r')
                # sql = " ".join(file.readlines())
                # sql=filter(None,sql.split(';'))

                file_nm = str(sql_path + fname_without_tstamp[:-1] + ".sql")
                with open(file_nm, 'r', encoding='utf-8') as file1:
                    sql = " ".join(file1.readlines())
                    sql = filter(None, sql.split(';'))

                    # Depending on the query inside the sql script, it will pass the parameters accordingly
                    for qrys in sql:
                        print("query is", qrys)
                        if 'CALL KCA_ODS_MERGE' in qrys or 'call kca_ods_merge' in qrys:
                            cur.execute(qrys % get_param)
                        elif "copy into" in qrys:
                            cur.execute(qrys % str(actual_filename))
                        elif 'PUT ' in qrys:
                            cur.execute(qrys % str(actual_filename))
                        elif 'truncate' in qrys or 'TRUNCATE' in qrys:
                            cur.execute(qrys)
                        elif 'rm' in qrys or 'RM' in qrys:
                            cur.execute(qrys)
                        elif 'insert' in qrys:
                            print(qrys)
                            cur.execute(qrys % str(actual_filename))
                        elif 'update' in qrys:
                            cur.execute(qrys % str(actual_filename))
                        elif 'INSERT' in qrys:
                            print(qrys)
                            cur.execute(qrys)
                        elif 'MERGE' in qrys:
                            cur.execute(qrys)
                        elif 'UPDATE' in qrys:
                            print(qrys)
                            cur.execute(qrys)


    # If depth is > 0, below code will trigger the sql file
    elif op_args[1]['depth'].values.tolist()[0] == 5 and op_args[0] == "TABLE_NAME":
        import datetime
        if full_or_inc == "FULL":
            # start_date=cur.execute("select VALUE from KCA_ERP.NCS_MDADM.KCA_CFG_BATCH_PARAM where PARAMETER_NAME='SRC_EXTRACT_START_DATE'").fetchall()[0][0]
            from dateutil.relativedelta import relativedelta

            start_date = (datetime.datetime.now() - relativedelta(years=3)).strftime("%Y-%m-%d")
        else:
            start_date = cur.execute(
                "select SRC_EXTR_START_DATE from NCS_MDADM.KCA_CTRL_BATCH where BATCH_NAME='NCS_DW_EXTRACT_INCR' ORDER BY BATCH_START_DATE DESC LIMIT 1").fetchall()[
                0][0]
            # start_date=str(start_date).split()[0]
            # print(start_date)

            start_date = datetime.datetime.strptime(str(start_date).split()[0], "%Y-%m-%d").strftime('%Y-%m-%d')
            print(start_date)
        # start_date=datetime.strptime(str(start_date).split()[0], "%Y-%m-%d").strftime('%Y-%m-%d')
        import time
        end_date = time.strftime("%Y-%m-%d")
        print(end_date)
        # end_date=time.strftime("%Y-%m-%d")

        # etl_job_id=cur.execute("select MAX(JOB_ID) FROM KCA_ERP.NCS_MDADM.KCA_CTRL_JOB WHERE JOB_TARGET='WRKFC_HD_CNT_INCR_FACT_STG' AND BATCH_ID IN (SELECT MAX(BATCH_ID) FROM KCA_ERP.NCS_MDADM.KCA_CTRL_BATCH)").fetchall()[0][0]
        # start_date="2020-09-01"
        etl_job_id = 12345
        get_param = (start_date, end_date, "N", "100", str(etl_job_id))
        print(get_param)

        sql_file = sql_path + op_args[0] + ".sql"
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql = " ".join(f.readlines())
            sql = filter(None, sql.split(';'))
            for qrys in sql:
                if 'CALL NP_DW.KCA_WRKFC_HD_CNT_INCR_FACT_STG' in qrys:
                    cur.execute(qrys % get_param)
                else:
                    cur.execute(qrys)


    else:
        sql_file = sql_path + op_args[0] + ".sql"
        with open(sql_file, 'r', encoding='utf-8') as f:
            for cur in con.execute_stream(f):
                print(cur)
                # for ret in cur:
                # print(ret)

    con.close()


# depth[0]bulkfunc
def sql_run(*op_args):
    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'),
        schema=Variable.get('SNOW_SCHEMA'))

    # Creating a cursor
    cur = con.cursor()
    cur.execute(
        "UPDATE NCS_MDADM.KCA_CTRL_BATCH SET BATCH_STATUS='RUNNING' WHERE BATCH_START_DATE IN (SELECT MAX(BATCH_START_DATE) FROM NCS_MDADM.KCA_CTRL_BATCH) AND BATCH_STATUS IN('FAILED','COMPLETED') AND BATCH_ID IN (select MAX(BATCH_ID) from NCS_MDADM.KCA_CTRL_BATCH)")

    sql_path = op_args[0]
    filename = op_args[1]
    actual_filename = op_args[2]
    # get_param=op_args[3]
    # print(get_param)
    print(sql_path)
    print(filename)
    print(actual_filename)

    get_param = cur.execute(
        "select TGT_TABLE, STG_TABLE,MERGE_CONDITION from NCS_MDADM.KCA_CFG_JOB_PARAM where JOB_NAME='" + filename + "'").fetchall()[0]
    # blob
    # file_path="/home/ubuntu/pallav_files/files_full/test_full/"+actual_filename

    file = open(sql_path + filename + ".sql", 'r')
    sql = " ".join(file.readlines())
    sql = filter(None, sql.split(';'))

    # Depending on the query inside the sql script, it will pass the parameters accordingly
    for qrys in sql:
        print(qrys)
        if 'CALL KCA_ODS_MERGE' in qrys:
            cur.execute(qrys % get_param)
        elif "copy into" in qrys:
            cur.execute(qrys % str(actual_filename))
        elif 'PUT ' in qrys:
            cur.execute(qrys % str(actual_filename))
        elif 'truncate' in qrys:
            cur.execute(qrys)
        elif 'rm' in qrys:
            cur.execute(qrys)
        elif 'insert' in qrys:
            cur.execute(qrys % str(actual_filename))
        elif 'update' in qrys:
            cur.execute(qrys % str(actual_filename))
        elif 'INSERT' in qrys:
            cur.execute(qrys)
        elif 'MERGE' in qrys:
            cur.execute(qrys)
        elif 'TRUNCATE' in qrys:
            cur.execute(qrys)

    con.close()


# end of bulk func
"""  
#Connecting to snowflake
#connection = snowflake.connector.connect (
    account  = Variable.get('SNOW_ACCOUNT'),
    user     = Variable.get('SNOW_USER'),
    password = Variable.get('SNOW_PASSWORD'),
    warehouse = Variable.get('SNOW_WAREHOUSE'),
    database =Variable.get('SNOW_DATABASE'))
curp = connection.cursor()
"""
# full_or_inc = curp.execute("select VALUE from NCS_MDADM.KCA_CFG_BATCH_PARAM where PARAMETER_NAME='LOAD_TYPE'").fetchall()[0][0]
df = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_CFG_BATCH_PARAM.xlsx")
full_or_inc = "".join(df[df["PARAMETER_NAME"] == "LOAD_TYPE"]["VALUE"].tolist())

# curp.execute("select DEPTH,JOB_NAME from NCS_MDADM.KCA_CFG_BATCH_DEP where BATCH_NAME='"+BATCH_NAME+"'")
# data=curp.fetchall()
data = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/data.xlsx")
data = data[data["BATCH_NAME"] == "KCA_DWH_LOAD"][["DEPTH", "JOB_NAME"]]
data.columns = ["depth", "job_name"]

# Created a dataframe after running sql query
# data=pd.DataFrame(data,columns=['depth','job_name'])

# Finding the max of depth
depth = data['depth'].max()
depth = depth + 1

max_records=int(df[df["PARAMETER_NAME"]=="NO_OF_ROWS"]["VALUE"])
x="/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_ERP_TABLE_ROW_CNT.xlsx"
bulk_files=[]
data2=pd.read_excel(x)
sorted_column = data2.sort_values(['ROW_COUNT'], ascending = False)
a=data2[data2['ROW_COUNT'] > max_records]
b=a.values.tolist()
for i in b:
    if i not in bulk_files:
        bulk_files.append(i[0])


#bulk_files = ["PER_ALL_ASSIGNMENTS_M", "CMP_SALARY"]
# PE_files=pd.read_sql("select JOB_NAME FROM KCA_ERP.NCS_MDADM.KCA_CFG_BATCH_DEP WHERE JOB_NAME LIKE ('%_PE') AND DEPTH=0",connection)
PE_files = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/PE_files.xlsx")

PE_files = list(PE_files["JOB_NAME"])
del_rec_pe = ["DELETE_REC_UPD"]
bulk_pe_files = bulk_files + PE_files + del_rec_pe



# created a list of list and appended Tasks. (depth0 tasks in 1st element of list, depth2 tasks in 2nd element of list,..)
depth_list = [[] for li in range(depth)]
for i in range(depth):
    stage = data[data['depth'] == i]
    print('stage:', stage)

    for index, row in stage.iterrows():
        tableName = row["job_name"]
        print(tableName)
        if full_or_inc == "FULL":
            if tableName not in bulk_pe_files:
                depth_list[i].append(
                    PythonOperator(task_id=tableName, python_callable=sql_runner, op_args=[tableName, stage], dag=dag))
        elif full_or_inc == "INCR":
            depth_list[i].append(
                PythonOperator(task_id=tableName, python_callable=sql_runner, op_args=[tableName, stage], dag=dag))

n = len(depth_list)
# Dummy_Operator
dummy = []
for r in range(n + 1):
    dummy.append(DummyOperator(task_id="dummy" + str(r), dag=dag))

for i in range(n):
    dummy[i] >> depth_list[i] >> dummy[i + 1]
# end of all depths

# bulkfiles
# full_or_inc = curp.execute("select VALUE from NCS_MDADM.KCA_CFG_BATCH_PARAM where PARAMETER_NAME='LOAD_TYPE'").fetchall()[0][0]
data1 = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/data.xlsx")
data1 = data1[(data1["BATCH_NAME"] == "KCA_DWH_LOAD") & (data1["DEPTH"] == 0)]
data1['TBL_MAX_RECORDS']=data1["TBL_MAX_RECORDS"].astype(int)
min_iter_value=data1["TBL_MAX_RECORDS"].min()
print("minimum iteration value is",min_iter_value)


#max_records=int(df[df["PARAMETER_NAME"]=="NO_OF_ROWS"]["VALUE"])
#no_of_rows = max_records
no_of_rows = min_iter_value           
# query2="select TABLE_NAME,ROW_COUNT from KCA_ERP.NCS_MDADM.KCA_ERP_TABLE_ROW_CNT"
# data_rows=pd.read_sql(query2,connection)
data_rows = pd.read_excel("/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_ERP_TABLE_ROW_CNT.xlsx")

data_rows["ROW_COUNT"] = data_rows["ROW_COUNT"].astype('int')
data_rows = data_rows.sort_values(by=['ROW_COUNT'], ascending=True)
data_rows["ITERATIONS"] = data_rows["ROW_COUNT"] / no_of_rows
data_rows["ITERATIONS"] = data_rows["ITERATIONS"].apply(lambda x: math.ceil(x))
maximum = data_rows["ITERATIONS"].max()
print(maximum)              

filepath = Variable.get('UCM_DECRYPT_PATH_INCR')
abc = glob.glob(filepath + "*.csv")
csv_path = []
for x in abc:
    filename = os.path.basename(x)
    filename = re.sub('_UCM\w+.csv$', "", filename)
    if filename in bulk_files:
        print(filename)
        csv_path.append(x)

# if bulk files is present
if len(csv_path) > 0 and full_or_inc == 'FULL':
    print("load type",full_or_inc)                              
    new_dict = {new_list: [] for new_list in bulk_files}
    bulk_file_name = []
    for x in csv_path:
        filename = os.path.basename(x)
        actual_filename = filename.split(".")[0]

        filename = re.sub('_UCM\w+.csv$', "", filename)
        # filename= re.sub('[0-9]+.csv$',"",filename)[:-1]
        # get_param= curp.execute("select TGT_TABLE, STG_TABLE,MERGE_CONDITION from NCS_MDADM.KCA_CFG_JOB_PARAM where JOB_NAME='"+filename+"'").fetchall()[0]

        if full_or_inc == "FULL":
            if filename not in bulk_files:
                # sql_path="/home/ubuntu/KCA_SQL_REPO/FULL/"
                sql_path = Variable.get('KCA_SQL_PATH_FULL')
                tablename = filename
                depth_list[0].append(PythonOperator(task_id=tablename, python_callable=sql_run,
                                                    op_args=[sql_path, filename, actual_filename], dag=dag))

            elif filename in bulk_files:
                filenamee = filename + str(1)
                if filenamee in bulk_file_name:
                    for i in range(2, maximum + 1):
                        filenamep = filename + str(i)
                        if filenamep not in bulk_file_name:
                            # sql_path="/home/ubuntu/KCA_SQL_REPO/INC/"
                            sql_path = Variable.get('KCA_SQL_PATH_FULL')
                            tablename = filenamep
                            bulk_file_name.append(filenamep)
                            new_dict[filename].append(PythonOperator(task_id=tablename, python_callable=sql_run,
                                                                     op_args=[sql_path, filename, actual_filename],
                                                                     dag=dag))
                            break
                else:
                    # sql_path="/home/ubuntu/KCA_SQL_REPO/FULL/"
                    sql_path = Variable.get('KCA_SQL_PATH_FULL')
                    tablename = filenamee
                    bulk_file_name.append(filenamee)
                    new_dict[filename].append(PythonOperator(task_id=tablename[:-1], python_callable=sql_run,
                                                             op_args=[sql_path, filename, actual_filename], dag=dag))


        elif full_or_inc == "INCR":
            if filename not in bulk_files:
                # sql_path="/home/ubuntu/KCA_SQL_REPO/INC/"
                sql_path = Variable.get('KCA_SQL_PATH_INC')
                tablename = filename
                depth_list[0].append(PythonOperator(task_id=tablename, python_callable=sql_run,
                                                    op_args=[sql_path, filename, actual_filename], dag=dag))

            elif filename in bulk_files:
                for i in range(1, maximum + 1):
                    filenamep = filename + str(i)
                    if filenamep not in bulk_file_name:
                        sql_path = Variable.get('KCA_SQL_PATH_INC')
                        # sql_path="/home/ubuntu/pallav_files/sql_inc/sql_test_bulk_inc/"
                        tablename = filenamep
                        bulk_file_name.append(filenamep)
                        new_dict[filename].append(PythonOperator(task_id=tablename, python_callable=sql_run,
                                                                 op_args=[sql_path, filename, actual_filename],
                                                                 dag=dag))
                        break

    # workflow of bulkfile
    for x1 in bulk_files:
        dummy[0] >> new_dict[x1][0]

    for x1 in bulk_files:
        for y1 in range(len(new_dict[x1]) - 1):
            new_dict[x1][y1] >> new_dict[x1][y1 + 1]

    for x1 in bulk_files:
        le = len(new_dict[x1]) - 1
        new_dict[x1][le] >> dummy[1]


# end of bulkfile code

# start,end,batch_error
def load_config():
    BATCH_NAME = "NCS_DW_LOAD_INCR"

    con = snowflake.connector.connect(
        account=Variable.get('SNOW_ACCOUNT'),
        user=Variable.get('SNOW_USER'),
        password=Variable.get('SNOW_PASSWORD'),
        role=Variable.get('SNOW_ROLE'),
        warehouse=Variable.get('SNOW_WAREHOUSE'),
        database=Variable.get('SNOW_DATABASE'))

    cur = con.cursor()

    cur.execute(
        "INSERT INTO NCS_MDADM.KCA_CTRL_BATCH(BATCH_ID,BATCH_NAME,BATCH_LOAD_TYPE,BATCH_STATUS,BATCH_START_DATE,SRC_EXTR_START_DATE,SRC_EXTR_END_DATE,PRUNE_DAYS,SOURCE_APP_ID)SELECT  (select MAX(BATCH_ID) from NCS_MDADM.KCA_CTRL_BATCH where BATCH_NAME='NCS_DW_EXTRACT_INCR') BATCH_ID,'" + BATCH_NAME + "' BATCH_NAME ,case when (select VALUE from NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR where PARAMETER_NAME='LOAD_TYPE') ='FULL' THEN 'FULL' ELSE 'INCR' END BATCH_LOAD_TYPE,'RUNNING' BATCH_STATUS,current_timestamp::timestamp_ntz BATCH_START_DATE ,(select SRC_EXTR_START_DATE from NCS_MDADM.KCA_CTRL_BATCH WHERE BATCH_NAME = 'NCS_DW_EXTRACT_INCR' and BATCH_ID  IN (SELECT MAX(BATCH_ID) FROM NCS_MDADM.KCA_CTRL_BATCH)) SRC_EXTR_START_DATE,(select SRC_EXTR_END_DATE from NCS_MDADM.KCA_CTRL_BATCH WHERE BATCH_NAME = 'NCS_DW_EXTRACT_INCR' and  BATCH_ID  IN (SELECT MAX(BATCH_ID) FROM NCS_MDADM.KCA_CTRL_BATCH where BATCH_NAME='NCS_DW_EXTRACT_INCR' and BATCH_STATUS='COMPLETED')) SRC_EXTR_END_DATE,(SELECT  to_number(VALUE) FROM NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR WHERE PARAMETER_NAME='PRUNE_DAYS') PRUNE_DAYS,(select APP_ID FROM NCS_MDADM.KCA_CFG_APP_PARAM WHERE APPLICATION_NAME ='ERP' ) SOURCE_APP_ID")


def load_success():
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
    cur.execute("update NCS_MDADM.KCA_CTRL_BATCH set BATCH_STATUS='COMPLETED',BATCH_END_DATE=current_timestamp::timestamp_ntz WHERE BATCH_START_DATE IN(SELECT MAX(BATCH_START_DATE) FROM NCS_MDADM.KCA_CTRL_BATCH) AND BATCH_NAME='NCS_DW_LOAD_INCR' AND BATCH_ID=(select MAX(BATCH_ID) from NCS_MDADM.KCA_CTRL_BATCH);")
    #cur.execute("UPDATE NCS_MDADM.KCA_CFG_BATCH_PARAM SET VALUE='INCR' WHERE PARAMETER_NAME='LOAD_TYPE' AND  VALUE='FULL'")
    cur.execute("UPDATE NCS_MDADM.KCA_CFG_BATCH_PARAM_INCR SET VALUE=(SELECT TO_VARCHAR(DATEADD(day,-3,to_timestamp(MAX(SRC_EXTR_END_DATE))),'YYYY-MM-DD HH24:MI:SS') SRC_EXTR_END_DATE FROM NCS_MDADM.KCA_CTRL_BATCH WHERE BATCH_STATUS='COMPLETED' AND BATCH_NAME='NCS_DW_EXTRACT_INCR' ORDER BY SRC_EXTR_END_DATE DESC) WHERE PARAMETER_NAME='SRC_EXTRACT_START_DATE';")
    
    #cur.execute("MERGE INTO NCS_MDADM.KCA_CFG_BATCH_DEP TGT USING (SELECT DEPTH,BATCH_NAME,JOB_NAME,TBL_MAX_RECORDS FROM NCS_MDADM.KCA_CFG_BATCH_DEP_INCR) SRC ON TGT.JOB_NAME=SRC.JOB_NAME WHEN NOT MATCHED THEN INSERT (DEPTH,BATCH_NAME,JOB_NAME,TBL_MAX_RECORDS) VALUES (SRC.DEPTH,SRC.BATCH_NAME,SRC.JOB_NAME,SRC.TBL_MAX_RECORDS);")
    
    #cur.execute("TRUNCATE NCS_MDADM.KCA_CFG_BATCH_DEP_INCR")


def batch_error():
    global BATCH_NAME
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
    query = "INSERT INTO NCS_MDADM.KCA_CTRL_JOB_LOG (JOB_ID,BATCH_ID,JOB_NAME,JOB_LOG_SUMMARY) Select NVL((select max(job_id) from NCS_MDADM.KCA_CTRL_JOB where job_target='" + \
            datapg2['task_id'][
                0] + "' and job_status='RUNNING'),NCS_MDADM.SEQ_JOB_CTRL_ID.NEXTVAL) JOB_ID,(select max(BATCH_ID) from NCS_MDADM.KCA_CTRL_BATCH where BATCH_STATUS='RUNNING' AND BATCH_NAME='" + BATCH_NAME + "'),'" + \
            datapg2['task_id'][0] + "','" + errm + "'"
    print(query)

    # updating batch table to failed status
    query2 = "UPDATE NCS_MDADM.KCA_CTRL_BATCH SET BATCH_STATUS='FAILED' WHERE BATCH_STATUS='RUNNING' AND BATCH_ID=(SELECT MAX(BATCH_ID) FROM NCS_MDADM.KCA_CTRL_BATCH WHERE BATCH_STATUS='RUNNING' AND BATCH_NAME='" + BATCH_NAME + "')"

    # updating the job table to failed status
    query3 = "update NCS_MDADM.KCA_CTRL_JOB set JOB_END_DATE=current_timestamp::timestamp_ntz, JOB_STATUS='FAILED' where JOB_END_DATE is null and JOB_TARGET ='" + \
             datapg2['task_id'][
                 0] + "'  and JOB_ID =(select max(JOB_ID) from NCS_MDADM.KCA_CTRL_JOB where JOB_STATUS='RUNNING') "
    cur.execute(query)
    cur.execute(query2)
    cur.execute(query3)
    raise ValueError('Error ' + errm)

def delete_old_files():
    import time
    file_path = '/home/oodles/ncs_multistage/airflow/airflow-scheduler.log'
    with open(file_path, 'w') as file:
        file.seek(0)
        file.truncate()
    
    encrypt_path='/home/oodles/ncs_multistage/export/home/dwhadminuser/bip_ucm_home/ucm/encryptedfiles/incr'
    current_time = time.time()
    extension = '.txt'
    two_days_ago = current_time - 2 * 24 * 60 * 60
    for file in os.listdir(encrypt_path):
        file_path = os.path.join(encrypt_path, file)
        if os.path.isfile(file_path) and not file_path.endswith(extension) and os.path.getctime(file_path) < two_days_ago:
            os.remove(file_path)

    archieve_path='/home/oodles/ncs_multistage/apps/dwh_data/extracts/erp_extracts/archieve_files/incr/'
    for file in os.listdir(archieve_path):
        file_path = os.path.join(archieve_path, file)
        if os.path.isfile(file_path) and os.path.getctime(file_path) < two_days_ago:
            os.remove(file_path)

    path = '/home/oodles/ncs_multistage/airflow/logs/'
    current_time = time.time()
    seven_days_ago = current_time - 7 * 24 * 60 * 60
    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.getctime(file_path) < seven_days_ago:
                os.remove(file_path)

# Load the Excel file
excel_file_path = "/home/oodles/ncs_multistage/export/home/dwhadminuser/excel_files/extract_dag/INCR/KCA_ERP_TABLE_ROW_CNT.xlsx"
df = pd.read_excel(excel_file_path)

# Sort the DataFrame by ROW_COUNT in descending order
df_sorted = df.sort_values(by='ROW_COUNT', ascending=False)

# Extract table names and row counts
table_names = df_sorted['TABLE_NAME'].tolist()
row_counts = df_sorted['ROW_COUNT'].tolist()

# Count tables with zero row counts
zero_count_tables = [table for table, count in zip(table_names, row_counts) if count == 0]

# Construct the email body
Cur_DateTime = datetime.now()
ENV="NCS Production Instance"
MAIL_ID = ['vikas.sanwal@oodles.io',"ankit.anand@oodles.io"]
#MAIL_ID = 'mallikarjuna.dhanireddy@kpipartners.com'

# Count the number of tables with row counts greater than 0
tables_with_counts = [(table, count) for table, count in zip(table_names, row_counts) if count > 0]
num_tables_with_counts = len(tables_with_counts)

# Construct the email body
mail_body = f"""
<html>
<body>
    <h1 style="color: green; font-size: 12px">&#128994; {BATCH_NAME} DAG Production Load Completed Successfully</h1>
    <p>Load Name: {BATCH_NAME}</p>
    <p>Env: {ENV}</p>
    <p>Time: {Cur_DateTime}</p>
    <p>PRODUCTION INCREMENTAL load ran for {num_tables_with_counts} table(s) today<p>
    <br>We got row counts for {num_tables_with_counts} table(s) only, rest of {len(zero_count_tables)} table(s) have ZERO records for today INCR load."
    <p>List of table(s) with Row Counts > 0:</p>
    <table border="1">
        <tr>
            <th>Table Name</th>
            <th>Row Count</th>
        </tr>
"""

for table, count in tables_with_counts:
    mail_body += f"<tr><td>{table}</td><td>{count}</td></tr>"

mail_body += """
    </table>
"""

if zero_count_tables:
    mail_body += "<p>Tables with ZERO row count:</p><ul>"

    for table in zero_count_tables:
        mail_body += f"<li>{table}</li>"


success_mail = EmailOperator(task_id='Success_mail',to=MAIL_ID,subject='Production Load Completed Successfully',html_content=mail_body,dag=dag)

# operators
load_start = PythonOperator(task_id='Start', python_callable=load_config, dag=dag)
load_success = PythonOperator(task_id='Load_Success_Update', python_callable=load_success, dag=dag)
log_delete= PythonOperator(task_id='Log_deletion_Update', python_callable=delete_old_files, dag=dag)
error_update = PythonOperator(task_id='Error_Logging', python_callable=batch_error, dag=dag, trigger_rule='one_failed')
# trigger =TriggerDagRunOperator(task_id="trigger_inc",trigger_dag_id="inc",dag=dag)
# workflow
load_start >> dummy[0]
dummy[n] >> load_success >> log_delete >> success_mail >> error_update
'''
load_start >> dummy[0]
dummy[n] >> load_success >> log_delete >> error_update
'''



