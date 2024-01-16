
from datetime import datetime
from ast import Attribute
import psycopg2

import config

query_log = []

def run():
    
    source_data = get_source_data()
    target_db_data = get_target_db_data()
    sync_data(source_data=source_data,target_db_data=target_db_data)

    # add timestamp on logger file name
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    logger_file_name = f"query_{timestamp}.log"
    with open(logger_file_name, "w") as f:
        f.write("\n".join(query_log))


def sync_data(source_data: list[tuple], target_db_data: list[tuple]):
    with get_connection(config.TARGET_DB_NAME) as conn:
        with conn.cursor() as cur:
            for row in source_data:
                try:
                    # get primary key
                    primary_key_index = config.ATTIBUTES.index(config.PRIMARY_KEY)
                    primary_key_value = row[primary_key_index]

                    query = ''

                    # check data already in target data 
                    for target_row in target_db_data:
                        if target_row[primary_key_index] == primary_key_value:
                            update_query_part = ""
                            for index, attribute in enumerate(config.ATTIBUTES):
                                # check for data change
                                if row[index] == target_row[index]:
                                    continue
                                if len(update_query_part) > 0:
                                    update_query_part += ", "
                                update_query_part += f"{attribute} = '{row[index]}'"
                            if len(update_query_part) > 0:
                                query = f"UPDATE {config.TARGET_TABLE_NAME} SET {update_query_part} WHERE {config.PRIMARY_KEY} = '{primary_key_value}';"
                            
                            # delete this row from target db data
                            target_db_data.remove(target_row)
                            break
                    else:
                        attributes = ", ".join(config.ATTIBUTES)
                        query = f"INSERT INTO {config.TARGET_TABLE_NAME} ({attributes}) VALUES {row};"


                    if query and len(query) > 0:
                        query_log.append(query)
                        cur.execute(query=query)

                except Exception as e:
                    print(e)
            
            # remain terget db data will be deleted
            for row in target_db_data:
                try:
                    primary_key_index = config.ATTIBUTES.index(config.PRIMARY_KEY)
                    primary_key_value = row[primary_key_index]
                    query = f"DELETE FROM {config.TARGET_TABLE_NAME} WHERE {config.PRIMARY_KEY} = '{primary_key_value}';"
                    if query and len(query) > 0:
                        query_log.append(query)
                        cur.execute(query=query)
                except Exception as e:
                    print(e)


def get_target_db_data():
    """
    Get target db data to compare with source
    """
    with get_connection(config.TARGET_DB_NAME) as conn:
        with conn.cursor() as cur:
            attributes = ", ".join(config.ATTIBUTES)
            query = f"SELECT {attributes} FROM {config.TARGET_TABLE_NAME};"
            cur.execute(query=query)
            return cur.fetchall()

def get_source_data():
    """
    Get source db data
    """
    with get_connection(config.SOURCE_DB_NAME) as conn:
        with conn.cursor() as cur:
            attributes = ", ".join(config.ATTIBUTES)
            query = f"SELECT {attributes} FROM {config.SOURCE_TABLE_NAME};"
            cur.execute(query=query)
            return cur.fetchall()


def get_connection(dbname: str ) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        dbname=dbname,
        user=config.DB_USER,
        password=config.DB_PASS,
        host=config.DB_HOST,
        port=config.DB_PORT
    )