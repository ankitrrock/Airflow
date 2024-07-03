import os
from airflow.models import Variable
import pandas as pd
from utils.consts import DATA_DIR
from utils.snowflake_connect import SnowflakeConnector
import csv
import ast

snowflake_connector = SnowflakeConnector()
snowflake_connector.connect()


persons_tgt_cols = pd.read_csv(DATA_DIR + "/persons_columns.csv")["columns"].values
persons_src_cols = list(map(lambda l: f"${l}", range(1, len(persons_tgt_cols) + 1)))
org_tgt_cols = pd.read_csv(DATA_DIR + "/organizations_columns.csv")["columns"].values
org_src_cols = list(map(lambda l: f"${l}", range(1, len(org_tgt_cols) + 1)))

product_tgt_cols = pd.read_csv(DATA_DIR + "/products_columns.csv")["columns"].values
product_src_cols = list(map(lambda l: f"${l}", range(1, len(product_tgt_cols) + 1)))


def copy_data_from_csv_file_to_persons_table(csv_file_path, table_name):
    MYDATABASE = Variable.get("SNOW_DATABASE_PIPEDRIVE")
    MYSCHEMA, MY_TABLE = table_name.split(".")

    csv_file_name = os.path.basename(csv_file_path)
    rows = []
    with open(csv_file_path, "r", newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            updated_row = update_row_persons(row)
            rows.append(updated_row)

    # Write the updated rows back to the CSV file
    fieldnames = reader.fieldnames
    with open(csv_file_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    add_data_to_stage = (
        f"PUT 'file://{csv_file_path}' @{MYDATABASE}.{MYSCHEMA}.MYSTAGE OVERWRITE=TRUE;"
    )
    copy_data_to_table = f"""copy into {MYDATABASE}.{MYSCHEMA}.{MY_TABLE}
                            (
                            {", ".join(persons_tgt_cols)}
                            ) from
                            (
                            SELECT
                            {", ".join(persons_src_cols)}
                            from '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz' (file_format => {MYDATABASE}.{MYSCHEMA}.MYFILEFORMAT)
                            );"""
    remove_data_from_stage = (
        f"rm '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz';"
    )

    snowflake_connector.execute_query(add_data_to_stage)
    snowflake_connector.execute_query(copy_data_to_table)
    snowflake_connector.execute_query(remove_data_from_stage)


def copy_data_from_csv_file_to_deals_table(csv_file_path, table_name):
    MYDATABASE = Variable.get("SNOW_DATABASE_PIPEDRIVE")
    MYSCHEMA, MY_TABLE = table_name.split(".")
    csv_file_name = os.path.basename(csv_file_path)
    add_data_to_stage = (
        f"PUT 'file://{csv_file_path}' @{MYDATABASE}.{MYSCHEMA}.MYSTAGE OVERWRITE=TRUE;"
    )
    copy_data_to_table = f"""copy into {MYDATABASE}.{MYSCHEMA}.{MY_TABLE}
                            (
                            ID, LABEL, TITLE, VALUE, ACTIVE, ORG_ID, STATUS, DELETED, USER_ID, ADD_TIME, CC_EMAIL, CURRENCY, ORG_NAME, STAGE_ID, WON_TIME, LOST_TIME, PERSON_ID, CLOSE_TIME, ORG_HIDDEN, OWNER_NAME, VISIBLE_TO, FILES_COUNT, LOST_REASON, NOTES_COUNT, PERSON_NAME, PIPELINE_ID, PROBABILITY, ROTTEN_TIME, UPDATE_TIME, PERSON_HIDDEN, FIRST_WON_TIME, PRODUCTS_COUNT, STAGE_ORDER_NR, WEIGHTED_VALUE, CREATOR_USER_ID, FOLLOWERS_COUNT, FORMATTED_VALUE, ACTIVITIES_COUNT, LAST_ACTIVITY_ID, NEXT_ACTIVITY_ID, STAGE_CHANGE_TIME, LAST_ACTIVITY_DATE, NEXT_ACTIVITY_DATE, NEXT_ACTIVITY_NOTE, NEXT_ACTIVITY_TIME, NEXT_ACTIVITY_TYPE, PARTICIPANTS_COUNT, EXPECTED_CLOSE_DATE, EMAIL_MESSAGES_COUNT, DONE_ACTIVITIES_COUNT, NEXT_ACTIVITY_SUBJECT, NEXT_ACTIVITY_DURATION, LAST_INCOMING_MAIL_TIME, LAST_OUTGOING_MAIL_TIME, UNDONE_ACTIVITIES_COUNT, WEIGHTED_VALUE_CURRENCY, FORMATTED_WEIGHTED_VALUE
                            ) from
                            (
                            SELECT
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57
                            from '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz' (file_format => {MYDATABASE}.{MYSCHEMA}.MYFILEFORMAT)
                            );"""
    remove_data_from_stage = (
        f"rm '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz';"
    )

    snowflake_connector.execute_query(add_data_to_stage)
    snowflake_connector.execute_query(copy_data_to_table)
    snowflake_connector.execute_query(remove_data_from_stage)


def copy_data_from_csv_file_to_products_table(csv_file_path, table_name):
    MYDATABASE = Variable.get("SNOW_DATABASE_PIPEDRIVE")
    MYSCHEMA, MY_TABLE = table_name.split(".")
    csv_file_name = os.path.basename(csv_file_path)
    add_data_to_stage = (
        f"PUT 'file://{csv_file_path}' @{MYDATABASE}.{MYSCHEMA}.MYSTAGE OVERWRITE=TRUE;"
    )
    copy_data_to_table = f"""copy into {MYDATABASE}.{MYSCHEMA}.{MY_TABLE}
                            (
                            {", ".join(product_tgt_cols)}
                            ) from
                            (
                            SELECT
                            {", ".join(product_src_cols)}
                            from '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz' (file_format => {MYDATABASE}.{MYSCHEMA}.MYFILEFORMAT)
                            );"""
    remove_data_from_stage = (
        f"rm '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz';"
    )

    snowflake_connector.execute_query(add_data_to_stage)
    snowflake_connector.execute_query(copy_data_to_table)
    snowflake_connector.execute_query(remove_data_from_stage)


def copy_data_from_csv_file_to_organizations_table(csv_file_path, table_name):
    MYDATABASE = Variable.get("SNOW_DATABASE_PIPEDRIVE")
    MYSCHEMA, MY_TABLE = table_name.split(".")
    csv_file_name = os.path.basename(csv_file_path)
    add_data_to_stage = (
        f"PUT 'file://{csv_file_path}' @{MYDATABASE}.{MYSCHEMA}.MYSTAGE OVERWRITE=TRUE;"
    )
    copy_data_to_table = f"""copy into {MYDATABASE}.{MYSCHEMA}.{MY_TABLE}
                            (
                            {", ".join(org_tgt_cols)}
                            ) from
                            (
                            SELECT
                            {", ".join(org_src_cols)}
                            from '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz' (file_format => {MYDATABASE}.{MYSCHEMA}.MYFILEFORMAT)
                            );"""
    remove_data_from_stage = (
        f"rm '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz';"
    )

    snowflake_connector.execute_query(add_data_to_stage)
    snowflake_connector.execute_query(copy_data_to_table)
    snowflake_connector.execute_query(remove_data_from_stage)


def set_etl(etl_details, streams_statusid):
    streams, statusid = streams_statusid.split("_")
    final_etl = {}
    if statusid == "US":
        check_status = [1, 3]
        etl_value = 1
    if statusid == "CANADA":
        check_status = [2, 3]
        etl_value = 2
    list_data = [list(record) for record in etl_details]
    for value in list_data:
        if value[1] == streams and value[0] == 0:
            if value[2] in check_status:
                final_etl[value[1]] = etl_value
    lowercase_dict = {key.lower(): value for key, value in final_etl.items()}
    print(lowercase_dict)
    return lowercase_dict


def reduce_call(source, db_source):
    # Fetch data from Snowflake
    data_for_etl = snowflake_connector.execute_query(
        "SELECT * FROM PIPEDRIVE_DEV.CONFIG.ETL_PIPEDRIVE"
    )

    # Convert the data into a list of lists
    list_data = [list(record) for record in data_for_etl]

    # Initialize the final_etl dictionary
    final_etl = {}

    # Iterate through the data to filter and extract relevant information
    for value in list_data:
        if value[1] == source:
            # Check the database source and update the final_etl dictionary accordingly
            if db_source == "US" and value[5] is not None:
                final_etl[value[1]] = value[5]
            elif db_source == "CANADA" and value[6] is not None:
                final_etl[value[1]] = value[6]

    # Convert keys to lowercase in the final dictionary
    lowercase_dict = {key.lower(): value for key, value in final_etl.items()}

    return lowercase_dict


# Function to update picture_id column with correct JSON format and file_size if None
def update_row_persons(row):
    # Check if picture_id value is a string representing a dictionary
    try:
        picture_id_dict = ast.literal_eval(row["picture_id"])
        if isinstance(picture_id_dict, dict):
            # Check if file_size is None and update it to 0 if needed
            if picture_id_dict.get("file_size") is None:
                picture_id_dict["file_size"] = 0
                row["picture_id"] = str(
                    picture_id_dict
                )  # Update picture_id column in the row
                print("Updated file_size to 0:", row)
        else:
            print("picture_id is not a dictionary")
    except (ValueError, SyntaxError):
        pass

    return row
