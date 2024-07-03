from utils.snowflake_connect import SnowflakeConnector
import pandas as pd
from utils.consts import DATA_DIR
import json


snowflake_connector = SnowflakeConnector()
snowflake_connector.connect()


def transforming_deals_data(
    deleted_deals_data, all_not_deleted_deals_data, csv_file_name, table_name
):
    print(1)
    new_deals_ids = pd.read_sql(
        f"SELECT * FROM {table_name};", snowflake_connector.connection
    )["ID"].values
    print(1)
    deleted_deals_data = filter(
        lambda l: l["id"] not in new_deals_ids, deleted_deals_data
    )
    all_not_deleted_deals_data = filter(
        lambda l: l["id"] not in new_deals_ids, all_not_deleted_deals_data
    )
    print(1)
    columns = pd.read_csv(f"{DATA_DIR}/deals_columns.csv")["columns"]
    print(5)
    df = pd.concat(
        [
            pd.DataFrame(deleted_deals_data, columns=columns),
            pd.DataFrame(all_not_deleted_deals_data, columns=columns),
        ]
    )

    df["creator_user_id"] = df["creator_user_id"].apply(
        lambda l: l["value"] if type(l) == dict else l
    )
    df["user_id"] = df["user_id"].apply(lambda l: l["value"] if type(l) == dict else l)
    df["person_id"] = df["person_id"].apply(
        lambda l: l["value"] if type(l) == dict else l
    )
    df["org_id"] = df["org_id"].apply(lambda l: l["value"] if type(l) == dict else l)
    print(6)
    df.to_csv(f"{DATA_DIR}/{csv_file_name}", index=False)


def transforming_organizations_data(organizations_data, csv_file_name, table_name):
    new_organizations_ids = pd.read_sql(
        f"SELECT * FROM {table_name};", snowflake_connector.connection
    )["ID"].values

    organizations_data = filter(
        lambda l: l["id"] not in new_organizations_ids, organizations_data
    )

    columns = pd.read_csv(f"{DATA_DIR}/organizations_columns.csv")["columns"]
    df = pd.DataFrame(organizations_data, columns=columns)

    df["owner_id"] = df["owner_id"].apply(lambda l: json.dumps(l))

    df.to_csv(f"{DATA_DIR}/{csv_file_name}", index=False)


def transforming_persons_data(persons_data, csv_file_name, table_name):
    new_persons_ids = pd.read_sql(
        f"SELECT * FROM {table_name};", snowflake_connector.connection
    )["ID"].values

    persons_data = filter(lambda l: l["id"] not in new_persons_ids, persons_data)

    columns = pd.read_csv(f"{DATA_DIR}/persons_columns.csv")["columns"]
    df = pd.DataFrame(persons_data, columns=columns)

    df["org_id"] = df["org_id"].apply(lambda l: l["value"] if type(l) == dict else l)
    df["owner_id"] = df["owner_id"].apply(
        lambda l: l["value"] if type(l) == dict else l
    )
    df["next_activity_time"] = df["next_activity_time"].apply(lambda l: json.dumps(l))

    df.to_csv(f"{DATA_DIR}/{csv_file_name}", index=False)


def transforming_products_data(products_data, csv_file_name, table_name):
    new_products_ids = pd.read_sql(
        f"SELECT * FROM {table_name};", snowflake_connector.connection
    )["ID"].values

    products_data = filter(lambda l: l["id"] not in new_products_ids, products_data)

    columns = pd.read_csv(f"{DATA_DIR}/products_columns.csv")["columns"]
    df = pd.DataFrame(products_data, columns=columns)

    df["prices"] = df["prices"].apply(lambda l: json.dumps(l))
    df["owner_id"] = df["owner_id"].apply(
        lambda l: l["value"] if type(l) == dict else l
    )

    df.to_csv(f"{DATA_DIR}/{csv_file_name}", index=False)
