import os
from airflow.models import Variable



def copy_data_from_csv_file_to_deals_table(csv_file_path, cur):
    MYDATABASE = Variable.get('SNOW_DATABASE_PIPEDRIVE')
    MYSCHEMA = "STREAMS"
    csv_file_name = os.path.basename(csv_file_path)
    add_data_to_stage = f"PUT 'file://{csv_file_path}' @{MYDATABASE}.{MYSCHEMA}.MYSTAGE OVERWRITE=TRUE;"
    copy_data_to_table = f"""copy into {MYDATABASE}.{MYSCHEMA}.DEALS
                            (
                            ID, LABEL, TITLE, VALUE, ACTIVE, ORG_ID, STATUS, DELETED, USER_ID, ADD_TIME, CC_EMAIL, CURRENCY, ORG_NAME, STAGE_ID, WON_TIME, LOST_TIME, PERSON_ID, CLOSE_TIME, ORG_HIDDEN, OWNER_NAME, VISIBLE_TO, FILES_COUNT, LOST_REASON, NOTES_COUNT, PERSON_NAME, PIPELINE_ID, PROBABILITY, ROTTEN_TIME, UPDATE_TIME, PERSON_HIDDEN, FIRST_WON_TIME, PRODUCTS_COUNT, STAGE_ORDER_NR, WEIGHTED_VALUE, CREATOR_USER_ID, FOLLOWERS_COUNT, FORMATTED_VALUE, ACTIVITIES_COUNT, LAST_ACTIVITY_ID, NEXT_ACTIVITY_ID, STAGE_CHANGE_TIME, LAST_ACTIVITY_DATE, NEXT_ACTIVITY_DATE, NEXT_ACTIVITY_NOTE, NEXT_ACTIVITY_TIME, NEXT_ACTIVITY_TYPE, PARTICIPANTS_COUNT, EXPECTED_CLOSE_DATE, EMAIL_MESSAGES_COUNT, DONE_ACTIVITIES_COUNT, NEXT_ACTIVITY_SUBJECT, NEXT_ACTIVITY_DURATION, LAST_INCOMING_MAIL_TIME, LAST_OUTGOING_MAIL_TIME, UNDONE_ACTIVITIES_COUNT, WEIGHTED_VALUE_CURRENCY, FORMATTED_WEIGHTED_VALUE
                            ) from
                            (
                            SELECT
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57
                            from '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz' (file_format => {MYDATABASE}.{MYSCHEMA}.MYFILEFORMAT)
                            );"""
    remove_data_from_stage = f"rm '@{MYDATABASE}.{MYSCHEMA}.MYSTAGE/{csv_file_name}.gz';"

    cur.execute(add_data_to_stage)
    cur.execute(copy_data_to_table)
    cur.execute(remove_data_from_stage)