from utils.snowflake_connect import SnowflakeConnector
from utils.pipedrive_etl.transform_helper import (
    transforming_deals_data,
    transforming_organizations_data,
    transforming_persons_data,
    transforming_products_data,
)


snowflake_connector = SnowflakeConnector()
snowflake_connector.connect()


def transform_deal(xcom_pull_obj, csv_file_name, table_name):
    deleted_deals_data_full = xcom_pull_obj[0]["deleted_deals"]
    all_not_deleted_deals_data_full = xcom_pull_obj[0]["all_not_deleted_deals"]
    deleted_deals_data = deleted_deals_data_full["1"] or []
    all_not_deleted_deals_data = all_not_deleted_deals_data_full["1"] or []
    # deleted_deals_data_canada = deleted_deals_data_full["2"] or []
    # all_not_deleted_deals_data_canada = all_not_deleted_deals_data_full["2"] or []
    # if deleted_deals_data or all_not_deleted_deals_data:
    transforming_deals_data(
        deleted_deals_data=deleted_deals_data,
        all_not_deleted_deals_data=all_not_deleted_deals_data,
        csv_file_name=csv_file_name,
        table_name=table_name,
    )
    # if deleted_deals_data_canada or all_not_deleted_deals_data_canada:
    #     transforming_deals_data(
    #         deleted_deals_data=deleted_deals_data_canada,
    #         all_not_deleted_deals_data=all_not_deleted_deals_data_canada,
    #         csv_file_name=csv_file_name,
    #         table_name=table_name,
    #     )


def transform_org(xcom_pull_obj, csv_file_name, table_name):
    organizations_data = xcom_pull_obj["1"] or []
    # # organizations_data_canada = xcom_pull_obj["2"] or []
    # if organizations_data:
    transforming_organizations_data(
        organizations_data=organizations_data,
        csv_file_name=csv_file_name,
        table_name=table_name,
    )
    # if organizations_data_canada:
    #     transforming_organizations_data(
    #         organizations_data=organizations_data_canada,
    #         csv_file_name=csv_file_name,
    #         table_name=table_name,
    #     )


def transform_persons(xcom_pull_obj, csv_file_name, table_name):
    persons_data = xcom_pull_obj["1"] or []
    # persons_data_canada = xcom_pull_obj["2"] or []
    # if persons_data_us:
    transforming_persons_data(
        persons_data=persons_data,
        csv_file_name=csv_file_name,
        table_name=table_name,
    )
    # if persons_data_canada:
    #     transforming_persons_data(
    #         persons_data=persons_data_canada,
    #         csv_file_name=csv_file_name,
    #         table_name=table_name,
    #     )


def transform_products(xcom_pull_obj, csv_file_name, table_name):
    persons_data = xcom_pull_obj["1"] or []
    # persons_data_canada = xcom_pull_obj["2"] or []
    # if persons_data_us:
    transforming_products_data(
        products_data=persons_data,
        csv_file_name=csv_file_name,
        table_name=table_name,
    )
    # if persons_data_canada:
    #     transforming_products_data(
    #         products_data=persons_data_canada,
    #         csv_file_name=csv_file_name,
    #         table_name=table_name,
    #     )
