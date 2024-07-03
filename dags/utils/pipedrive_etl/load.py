from utils.pipedrive_etl.load_helper import (
    load_deal_helper,
    load_org_helper,
    load_persons_helper,
    load_products_helper,
)


def load_deal(db, csv_file_name, table):
    if db == 1:
        file_name = csv_file_name
        table_name = table
        load_deal_helper(file_name, table_name, db)
    if db == 2:
        file_name = csv_file_name
        table_name = table
        load_deal_helper(file_name, table_name, db)


def load_org(db, csv_file_name, table):

    if db == 1:
        file_name = csv_file_name
        table_name = table
        load_org_helper(file_name, table_name, db)
    if db == 2:
        file_name = csv_file_name
        table_name = table
        load_org_helper(file_name, table_name, db)


def load_persons(db, csv_file_name, table):

    if db == 1:
        file_name = csv_file_name
        table_name = table
        load_persons_helper(file_name, table_name, db)
    if db == 2:
        file_name = csv_file_name
        table_name = table
        load_persons_helper(file_name, table_name, db)


def load_products(db, csv_file_name, table):

    if db == 1:
        file_name = csv_file_name
        table_name = table
        load_products_helper(file_name, table_name, db)
    if db == 2:
        file_name = csv_file_name
        table_name = table
        load_products_helper(file_name, table_name, db)
