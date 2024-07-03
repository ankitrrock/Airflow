from utils.snowflake_connect import SnowflakeConnector
from utils.consts import DATA_DIR
from utils.pipedrive_etl.etl_helper import (
    copy_data_from_csv_file_to_deals_table,
    copy_data_from_csv_file_to_persons_table,
    copy_data_from_csv_file_to_products_table,
    copy_data_from_csv_file_to_organizations_table,
)
import pandas as pd
from datetime import datetime
import csv
import ast


snowflake_connector = SnowflakeConnector()
snowflake_connector.connect()


def load_deal_helper(file_name, table_name, db=None):
    csv_file_path = f"{DATA_DIR}/{file_name}"
    copy_data_from_csv_file_to_deals_table(csv_file_path, table_name)
    MYSCHEMA, MY_TABLE = table_name.split(".")

    df = pd.read_sql(
        f"SELECT * FROM {MYSCHEMA}.{MY_TABLE};", snowflake_connector.connection
    )
    duplicate_deals_ids = df["ID"][df["ID"].duplicated()].values

    def delete_duplicate_deals(id):
        query = f"""DELETE FROM {MYSCHEMA}.{MY_TABLE}
                WHERE ID={id} AND UPDATE_TIME = (
                    SELECT MIN(UPDATE_TIME) FROM {MYSCHEMA}.{MY_TABLE} WHERE ID={id}
            );"""
        snowflake_connector.execute_query(query)

    for id in duplicate_deals_ids:
        delete_duplicate_deals(id)
    # if db:
    #     if db == 1:
    #         country = "US"
    #     if db == 2:
    #         country = "CANADA"
    #     query_delete_for_historical = f"""DELETE FROM PIPEDRIVE.HISTORICAL.DEALS_{country}_HIST
    #         WHERE ID IN (
    #             SELECT ID
    #             FROM PIPEDRIVE.TRANSACTIONAL.DEALS_{country}_INCR
    #         ); """
    #     snowflake_connector.execute_query(query_delete_for_historical)


def load_org_helper(file_name, table_name, db=None):
    csv_file_path = f"{DATA_DIR}/{file_name}"
    copy_data_from_csv_file_to_organizations_table(csv_file_path, table_name)
    MYSCHEMA, MY_TABLE = table_name.split(".")
    df = pd.read_sql(
        f"SELECT * FROM {MYSCHEMA}.{MY_TABLE};", snowflake_connector.connection
    )
    duplicate_organizations_ids = df["ID"][df["ID"].duplicated()].values

    def delete_duplicate_organizations(id):
        query = f"""DELETE FROM {MYSCHEMA}.{MY_TABLE}
                WHERE ID={id} AND UPDATE_TIME = (
                    SELECT MIN(UPDATE_TIME) FROM {MYSCHEMA}.{MY_TABLE} WHERE ID={id}
            );"""
        snowflake_connector.execute_query(query)

    for id in duplicate_organizations_ids:
        delete_duplicate_organizations(id)
    # if db:
    #     if db == 1:
    #         country = "US"
    #     if db == 2:
    #         country = "CANADA"
    #     query_delete_for_historical = f"""DELETE FROM PIPEDRIVE.HISTORICAL.ORGANIZATIONS_{country}_HIST
    #         WHERE ID IN (
    #             SELECT ID
    #             FROM PIPEDRIVE.TRANSACTIONAL.ORGANIZATIONS_{country}_INCR
    #         ); """
    #     snowflake_connector.execute_query(query_delete_for_historical)


def load_persons_helper(file_name, table_name, db=None):
    csv_file_path = f"{DATA_DIR}/{file_name}"
    copy_data_from_csv_file_to_persons_table(csv_file_path, table_name)
    MYSCHEMA, MY_TABLE = table_name.split(".")
    df = pd.read_sql(
        f"SELECT * FROM {MYSCHEMA}.{MY_TABLE};", snowflake_connector.connection
    )
    duplicate_persons_ids = df["ID"][df["ID"].duplicated()].values

    def delete_duplicate_persons(id):
        query = f"""DELETE FROM {MYSCHEMA}.{MY_TABLE}
                WHERE ID={id} AND UPDATE_TIME = (
                    SELECT MIN(UPDATE_TIME) FROM {MYSCHEMA}.{MY_TABLE} WHERE ID={id}
            );"""
        snowflake_connector.execute_query(query)

    for id in duplicate_persons_ids:
        delete_duplicate_persons(id)
    # if db:
    #     if db == 1:
    #         country = "US"
    #     if db == 2:
    #         country = "CANADA"
    #     query_delete_for_historical = f"""DELETE FROM PIPEDRIVE.HISTORICAL.PERSONS_{country}_HIST
    #         WHERE ID IN (
    #             SELECT ID
    #             FROM PIPEDRIVE.TRANSACTIONAL.PERSONS_{country}_INCR
    #         ); """
    #     snowflake_connector.execute_query(query_delete_for_historical)


def load_products_helper(file_name, table_name, db=None):
    csv_file_path = f"{DATA_DIR}/{file_name}"
    copy_data_from_csv_file_to_products_table(csv_file_path, table_name)
    MYSCHEMA, MY_TABLE = table_name.split(".")
    df = pd.read_sql(
        f"SELECT * FROM {MYSCHEMA}.{MY_TABLE};", snowflake_connector.connection
    )
    duplicate_products_ids = df["ID"][df["ID"].duplicated()].values

    def delete_duplicate_products(id):
        query = f"""DELETE FROM {MYSCHEMA}.{MY_TABLE}
                WHERE ID={id} AND UPDATE_TIME = (
                    SELECT MIN(UPDATE_TIME) FROM {MYSCHEMA}.{MY_TABLE} WHERE ID={id}
            );"""
        snowflake_connector.execute_query(query)

    for id in duplicate_products_ids:
        delete_duplicate_products(id)

    if db:
        if db == 1:
            country = "US"
        if db == 2:
            country = "CANADA"

        active_query = f"INSERT INTO ACTIVE.PRODUCTS_{country}_ACTIVE SELECT * FROM HISTORICAL.PRODUCTS_{country}_HIST WHERE ACTIVE_FLAG = TRUE;"
        inactive_query = f"INSERT INTO INACTIVE.PRODUCTS_{country}_INACTIVE SELECT * FROM HISTORICAL.PRODUCTS_{country}_HIST WHERE ACTIVE_FLAG = FALSE;"
        snowflake_connector.execute_query(active_query)
        snowflake_connector.execute_query(inactive_query)


def load_mail(data, dag_id, csv_file_name):
    BATCH_NAME = "PIPEDRIVE DATA MIGRATIONS"
    Cur_DateTime = datetime.now()
    ENV = "PIPEDRIVE DEV TESTING Instance"

    mail_body = f"""
    <html>
    <body>
        <h1 style="color: green; font-size: 12px">&#128994; {dag_id} DAG Production Load Completed Successfully</h1>
        <table border="1">
            <tr>
                <th>Attribute</th>
                <th>Value</th>
            </tr>
            <tr>
                <td>DAG ID</td>
                <td>{dag_id}</td>
            </tr>
            <tr>
                <td>Load Name</td>
                <td>{BATCH_NAME}</td>
            </tr>
            <tr>
                <td>Env</td>
                <td>{ENV}</td>
            </tr>
            <tr>
                <td>Time</td>
                <td>{Cur_DateTime}</td>
            </tr>
    """

    for table, count in data.items():
        if count == 1:
            country = "US"
        elif count == 2:
            country = "Canada"
        elif count == 3:
            country = "US,Canada"
        else:
            country = None
        rows = read_csv(
            file_path=f"/home/oodles/ncs_multistage/airflow/dags/data/{csv_file_name}"
        )
        if count:
            mail_body += f"<tr><td>No of rows</td><td>{rows}</td></tr>"
            mail_body += f"<tr><td>Country</td><td>{country}</td></tr>"
            mail_body += f"<tr><td>Table Name</td><td>{table.capitalize()}</td></tr>"

    mail_body += """
        </table>
    </body>
    </html>
    """
    return mail_body


def read_csv(file_path):
    with open(file_path, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        # Skip the header row
        next(reader, None)
        data = list(reader)
        rows = len(data)
    return rows
