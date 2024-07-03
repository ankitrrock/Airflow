from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from utils.snowflake_connect import SnowflakeConnector
from airflow.operators.python import PythonOperator
from utils.pipedrive_etl.etl_helper import set_etl
from utils.pipedrive_etl.extract import extract_org
from utils.pipedrive_etl.load import load_org

from utils.pipedrive_etl.load_helper import load_mail
from utils.pipedrive_etl.transform import transform_org
from airflow.operators.email_operator import EmailOperator
import pandas as pd
from datetime import datetime
import csv

type = "INACTIVE"
table = "INACTIVE.ORGANIZATIONS_US_INACTIVE"
csv_file_name = "organizations_us_inactive.csv"
WORKFLOW_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    "email_on_retry": False,
    "email_on_failure": False,  # Enable email notification on task failure
    "email": [
        "aanand@ncscontractor.com",
        "ankit.anand@oodles.io",
    ],  # Add your email address here
}

dag = DAG(
    "PIPEDRIVE_ETL_ORGANIZATIONS_US_INACTIVE",
    description="PIPEDRIVE_ETL_ORGANIZATIONS_US_INACTIVE",
    default_args=WORKFLOW_DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2024, 3, 12),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
)
snowflake_connector = SnowflakeConnector()
snowflake_connector.connect()


def send_failure_email(context):
    subject = f"[Airflow] Task Failed: {context['task_instance'].task_id}"

    hostname = context["task_instance"].hostname
    print(context)

    html_content = f"""
    <html>
    <head></head>
    <body>
        <h2 style="color: red;">Task Failed: {context['task_instance'].task_id}</h2>
        <table border="1">
            <tr>
                <th>Attribute</th>
                <th>Value</th>
            </tr>
            <tr>
                <td>DAG ID</td>
                <td>{context['task_instance'].dag_id}</td>
            </tr>
            <tr>
                <td>Execution Date</td>
                <td>{context['execution_date']}</td>
            </tr>
            <tr>
                <td>Timestamp</td>
                <td>{datetime.now()}</td>
            </tr>
            <tr>
                <td>Error</td>
                <td>{context['exception']}</td>
            </tr>
            <tr>
                <td>Host Name</td>
                <td><a href="{hostname}">{hostname}</a></td>
            </tr>
        </table>
    </body>
    </html>
    """

    send_email(context["task"].email, subject, html_content)


def read_config():
    data_for_etl = snowflake_connector.execute_query(
        "SELECT * FROM PIPEDRIVE_DEV.CONFIG.ETL_PIPEDRIVE"
    )
    final_etl = set_etl(data_for_etl, streams_statusid="ORGANIZATIONS_US")
    if not "organizations" in final_etl:
        raise Exception("Please check Config Details")
    return final_etl


def extract(ti):
    data_for_etl = ti.xcom_pull(task_ids=["read_config"])[0]
    extract_data = {
        "deals": None,
        "organizations": None,
        "products": None,
        "persons": None,
    }
    if data_for_etl["organizations"] is not None:
        organizations = extract_org(
            deal_db=data_for_etl["organizations"], type=type, table=table
        )
        extract_data["organizations"] = organizations

    extract_data["transform_details"] = data_for_etl
    print(extract_data)
    return extract_data


def transform(ti):
    xcom_pull_obj = ti.xcom_pull(task_ids=["extract"])[0]
    load_etl = xcom_pull_obj["transform_details"]
    if xcom_pull_obj["organizations"] is not None:
        transform_org(xcom_pull_obj["organizations"], csv_file_name, table)
    return load_etl


def load(ti):
    load_data = ti.xcom_pull(task_ids=["transform"])[0]
    value = load_data
    if "organizations" in value and value["organizations"] is not None:
        load_org(value["organizations"], csv_file_name, table)

    # Send email notification about successful data loading
    mail_details = load_mail(load_data, dag.dag_id, csv_file_name)
    return mail_details


read_config = PythonOperator(
    task_id="read_config", python_callable=read_config, dag=dag
)
extract = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
transform = PythonOperator(task_id="transform", python_callable=transform, dag=dag)
load = PythonOperator(task_id="load", python_callable=load, dag=dag)
send_email_success_task = EmailOperator(
    task_id="send_email_success",
    to=[
        "bi@ncsmultistage.com",
        "servicedeskadmin@ncsmultistage.com",
        "aanand@ncscontractor.com",
    ],
    subject="Production Load Completed Successfully",
    html_content="{{ task_instance.xcom_pull(task_ids='load') }}",
    dag=dag,
)

read_config >> extract >> transform >> load >> send_email_success_task

for task in dag.tasks:
    task.on_failure_callback = send_failure_email
