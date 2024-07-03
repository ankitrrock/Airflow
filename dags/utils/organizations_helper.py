import os
from airflow.models import Variable
import pandas as pd
from utils.consts import DATA_DIR


tgt_cols = pd.read_csv(DATA_DIR+"/organizations_columns.csv")["columns"].values
src_cols = map(lambda l: f"${l}", range(1, len(tgt_cols)+1))

def copy_data_from_csv_file_to_organizations_table(csv_file_path, cur):
    MYDATABASE = Variable.get('SNOW_DATABASE_PIPEDRIVE')
    MYSCHEMA = "STREAMS"
    csv_file_name = os.path.basename(csv_file_path)
    add_data_to_stage = f"PUT 'file://{csv_file_path}' @{MYDATABASE}.{MYSCHEMA}.MYSTAGE OVERWRITE=TRUE;"
    copy_data_to_table = f"""copy into {MYDATABASE}.{MYSCHEMA}.ORGANIZATIONS
                            (
                            {", ".join(tgt_cols)}
                            ) from
                            (
                            SELECT
                            {", ".join(src_cols)}
                            from '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz' (file_format => {MYDATABASE}.{MYSCHEMA}.MYFILEFORMAT)
                            );"""
    remove_data_from_stage = f"rm '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz';"

    cur.execute(add_data_to_stage)
    cur.execute(copy_data_to_table)
    cur.execute(remove_data_from_stage)