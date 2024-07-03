from utils.snowflake_connect import SnowflakeConnector
from utils.snowflake_getting_data import get_deals
from utils.snowflake_getting_data import (
    get_deals,
    get_persons,
    get_organizations,
    get_products,
)

snowflake_connector = SnowflakeConnector()
snowflake_connector.connect()


def extract_deal(deal_db, type=None, table=None):
    if type == "HIST":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    if type == "ACTIVE":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    if type == "INACTIVE":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)

    deleted_deals = get_deals(status="deleted", deal_db=deal_db, type=type)
    all_not_deleted_deals = get_deals(
        status="all_not_deleted", deal_db=deal_db, type=type
    )
    return deleted_deals, all_not_deleted_deals


def extract_org(deal_db, type=None, table=None):
    if type == "HIST":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    if type == "ACTIVE":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    if type == "INACTIVE":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    organizations = get_organizations(deal_db, type=type)
    return organizations


def extract_person(deal_db, type=None, table=None):
    if type == "HIST":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    if type == "ACTIVE":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    if type == "INACTIVE":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    persons = get_persons(deal_db, type=type)
    return persons


def extract_product(deal_db, type=None, table=None):
    if type == "HIST":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    if type == "ACTIVE":
        if deal_db == 1 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
        if deal_db == 2 or deal_db == 3:
            truncate_table = f"TRUNCATE {table};"
            snowflake_connector.execute_query(truncate_table)
    products = get_products(deal_db, type=type)
    return products
