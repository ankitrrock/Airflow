from airflow.models import Variable
from utils.snowflake_connect import SnowflakeConnector
from utils.helper import gen_query_string
from utils.pipedrive_etl.etl_helper import reduce_call
from datetime import datetime, timedelta
import requests


snowflake_connector = SnowflakeConnector()
snowflake_connector.connect()


def get_deals(status, deal_db=None, type=type):
    today = datetime.today()
    three_months_ago = today - timedelta(days=3 * 30)
    update_params = {}
    # if type == "HIST":
    #     update_params["start_date"] = None
    #     update_params["end_date"] = "2023-12-31"
    # elif type == "ACTIVE":
    #     update_params["start_date"] = "2024-01-01"
    #     update_params["end_date"] = datetime.today().strftime("%Y-%m-%d")
    # elif type == "INACTIVE":
    #     update_params["start_date"] = three_months_ago.strftime("%Y-%m-%d")
    #     update_params["end_date"] = datetime.today().strftime("%Y-%m-%d")
    if status:
        update_params["status"] = status
    filter_id = None
    deals_data = {"1": None}
    if deal_db == 1 or deal_db == 3:
        if type == "HIST":
            filter_id = Variable.get("DEALS_US_HIST")
        if type == "ACTIVE":
            filter_id = Variable.get("DEALS_US_ACTIVE")
        if type == "INACTIVE":
            filter_id = Variable.get("DEALS_US_INACTIVE")
        token_value = "PIPEDRIVE_API_TOKEN_US"
        endpoint = "https://api.pipedrive.com/v1/deals?"
        response_data = api_request(endpoint, token_value, update_params, filter_id)
        if response_data:
            deals_data["1"] = response_data
    if deal_db == 2 or deal_db == 3:
        if type == "HIST":
            filter_id = Variable.get("DEALS_CANADA_HIST")
        if type == "ACTIVE":
            filter_id = Variable.get("DEALS_CANADA_ACTIVE")
        if type == "INACTIVE":
            filter_id = Variable.get("DEALS_CANADA_INACTIVE")
        token_value = "PIPEDRIVE_API_TOKEN_CANADA"
        endpoint = "https://api.pipedrive.com/v1/deals?"
        response_data = api_request(endpoint, token_value, update_params, filter_id)
        if response_data:
            deals_data["1"] = response_data

    return deals_data


def get_organizations(deal_db=None, type=type):
    today = datetime.today()
    three_months_ago = today - timedelta(days=3 * 30)
    update_params = {}
    # if type == "HIST":
    #     update_params["start_date"] = None
    #     update_params["end_date"] = "2023-12-31"
    # elif type == "ACTIVE":
    #     update_params["start_date"] = "2024-01-01"
    #     update_params["end_date"] = datetime.today().strftime("%Y-%m-%d")
    # elif type == "INACTIVE":
    #     update_params["start_date"] = three_months_ago.strftime("%Y-%m-%d")
    #     update_params["end_date"] = datetime.today().strftime("%Y-%m-%d")
    filter_id = None
    organizations_data = {"1": None}

    if deal_db == 1 or deal_db == 3:
        db_source = "US"
        if type == "HIST":
            filter_id = Variable.get("ORGANIZATIONS_US_HIST")
        if type == "ACTIVE":
            filter_id = Variable.get("ORGANIZATIONS_US_ACTIVE")
        if type == "INACTIVE":
            filter_id = Variable.get("ORGANIZATIONS_US_INACTIVE")
        token_value = "PIPEDRIVE_API_TOKEN_US"
        endpoint = "https://api.pipedrive.com/v1/organizations?"
        response_data = api_request(endpoint, token_value, update_params, filter_id)
        if response_data:
            organizations_data["1"] = response_data
    if deal_db == 2 or deal_db == 3:
        db_source = "CANADA"
        if type == "HIST":
            filter_id = Variable.get("ORGANIZATIONS_CANADA_HIST")
        if type == "ACTIVE":
            filter_id = Variable.get("ORGANIZATIONS_CANADA_ACTIVE")
        if type == "INACTIVE":
            filter_id = Variable.get("ORGANIZATIONS_CANADA_INACTIVE")
        token_value = "PIPEDRIVE_API_TOKEN_CANADA"
        endpoint = "https://api.pipedrive.com/v1/organizations?"
        response_data = api_request(endpoint, token_value, update_params, filter_id)
        if response_data:
            organizations_data["1"] = response_data
    return organizations_data


def get_persons(deal_db=None, type=type):
    today = datetime.today()
    three_months_ago = today - timedelta(days=3 * 30)
    update_params = {}
    # if type == "HIST":
    #     update_params["start_date"] = None
    #     update_params["end_date"] = "2023-12-31"
    # elif type == "ACTIVE":
    #     update_params["start_date"] = "2024-01-01"
    #     update_params["end_date"] = datetime.today().strftime("%Y-%m-%d")
    # elif type == "INACTIVE":
    #     update_params["start_date"] = three_months_ago.strftime("%Y-%m-%d")
    #     update_params["end_date"] = datetime.today().strftime("%Y-%m-%d")
    filter_id = None
    persons_data = {"1": None}
    if deal_db == 1 or deal_db == 3:
        db_source = "US"
        if type == "HIST":
            filter_id = Variable.get("PERSONS_US_HIST")
        if type == "ACTIVE":
            filter_id = Variable.get("PERSONS_US_ACTIVE")
        if type == "INACTIVE":
            filter_id = Variable.get("PERSONS_US_INACTIVE")
        token_value = "PIPEDRIVE_API_TOKEN_US"
        endpoint = "https://api.pipedrive.com/v1/persons?"
        response_data = api_request(endpoint, token_value, update_params, filter_id)
        if response_data:
            persons_data["1"] = response_data
    if deal_db == 2 or deal_db == 3:
        db_source = "CANADA"
        if type == "HIST":
            filter_id = Variable.get("PERSONS_CANADA_HIST")
        if type == "ACTIVE":
            filter_id = Variable.get("PERSONS_CANADA_ACTIVE")
        if type == "INACTIVE":
            filter_id = Variable.get("PERSONS_CANADA_INACTIVE")
        token_value = "PIPEDRIVE_API_TOKEN_CANADA"
        endpoint = "https://api.pipedrive.com/v1/persons?"
        response_data = api_request(endpoint, token_value, update_params, filter_id)
        if response_data:
            persons_data["1"] = response_data
    return persons_data


def get_products(deal_db=None, type=type):
    today = datetime.today()
    three_months_ago = today - timedelta(days=3 * 30)
    update_params = {}
    # if type == "HIST":
    #     update_params["start_date"] = None
    #     update_params["end_date"] = "2023-12-31"
    # elif type == "ACTIVE":
    #     update_params["start_date"] = "2024-01-01"
    #     update_params["end_date"] = datetime.today().strftime("%Y-%m-%d")
    # elif type == "INACTIVE":
    #     update_params["start_date"] = three_months_ago.strftime("%Y-%m-%d")
    #     update_params["end_date"] = datetime.today().strftime("%Y-%m-%d")
    filter_id = None
    products_data = {"1": None}
    if deal_db == 1 or deal_db == 3:
        if type == "HIST":
            filter_id = None
        if type == "ACTIVE":
            filter_id = None
        token_value = "PIPEDRIVE_API_TOKEN_US"
        endpoint = "https://api.pipedrive.com/v1/products?"
        response_data = api_request(endpoint, token_value, update_params, filter_id)
        if response_data:
            products_data["1"] = response_data
    if deal_db == 2 or deal_db == 3:
        if type == "HIST":
            filter_id = None
        if type == "ACTIVE":
            filter_id = None
        token_value = "PIPEDRIVE_API_TOKEN_CANADA"
        endpoint = "https://api.pipedrive.com/v1/products?"
        response_data = api_request(endpoint, token_value, update_params, filter_id)
        if response_data:
            products_data["1"] = response_data
    return products_data


def api_request(endpoint, token_value, update_params={}, filter_id=None):
    start = 0
    limit = 500
    retrieve_data = []
    while True:
        params = {
            "api_token": Variable.get(token_value),
            "start": start,
            "limit": limit,
        }
        if filter_id:
            params["filter_id"] = filter_id
        if update_params:
            params.update(update_params)
        print(params)
        query_string = gen_query_string(params)
        response = requests.get(f"{endpoint}{query_string}").json()
        if response["data"] != None:
            retrieve_data.extend(response["data"])
        else:
            retrieve_data = None
            break

        more_items_in_collection = response["additional_data"]["pagination"][
            "more_items_in_collection"
        ]
        if more_items_in_collection:
            start = response["additional_data"]["pagination"]["next_start"]
        else:
            break
    return retrieve_data
