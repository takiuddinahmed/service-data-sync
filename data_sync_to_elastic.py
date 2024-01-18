from curses import raw
import traceback
from datetime import datetime
import psycopg2
from elasticsearch import Elasticsearch
import config

logs = []
def run():
    source_data = get_source_data()
    target_data = get_target_db_data()
    sync_data(source_data, target_data)

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    logger_file_name = f"elastic_log_{timestamp}.log"

    with open(logger_file_name, "w") as f:
        f.write("\n".join(str(log) for log in logs))


def sync_data(source_data: list[dict], target_data: list[dict]):
    with get_elastic_search() as es:
        for row in source_data:
            id = row["id"]
            data  = row["data"]
            
            # check data already in target data
            for target_row in target_data:
                if target_row["id"] == id:
                    
                    # check for any changes
                    if not compare_dictionaries(target_row["data"], data):
                        logs.append({
                            "action":"UPDATE",
                            "id": id,
                            "data": data
                        })
                        es.update(index=config.ELASTIC_SEARCH_INDEX, id=id, body=data)

                    # delete this row from target db data
                    target_data.remove(target_row)
                    break 
            else:
                logs.append({
                            "action":"CREATE",
                            "id": id,
                            "data": data
                        })
                es.index(index=config.ELASTIC_SEARCH_INDEX, id=id, body=data)
            
            es.indices.refresh(index=config.ELASTIC_SEARCH_INDEX)
        

        # remain terget db data will be deleted
        for row in target_data:
            id = row["id"]
            logs.append({
                "action":"DELETE",
                "id": id
            })
            es.delete(index=config.ELASTIC_SEARCH_INDEX, id=id)



def get_target_db_data():
    with get_elastic_search() as es:
        data = es.search(index=config.ELASTIC_SEARCH_INDEX, body={"query": {"match_all": {}}})
        data = data.get("hits").get("hits")

        return [
            {
            "id": row["_id"],
            "data": row["_source"]
            }
            for row in data
        ]

         

def get_elastic_search():
    es = None
    
    es = Elasticsearch(config.ELASTIC_SEARCH_DB_URI)
    # Check if the connection is successful
    if es.ping():
        return es
    else:
        raise Exception("Could not connect to Elasticsearch")
        
    


def get_source_data():
    """
    Get source db data
    """
    with get_connection(config.SOURCE_DB_NAME) as conn:
        with conn.cursor() as cur:
            attributes = ", ".join(config.ATTIBUTES)
            query = f"SELECT {attributes} FROM {config.SOURCE_TABLE_NAME};"
            cur.execute(query=query)
            raw_data =  cur.fetchall()
            formatted_data : list[dict] = []
            for row in raw_data:
                primary_key_index = config.ATTIBUTES.index(config.PRIMARY_KEY)
                primary_key_value = row[primary_key_index]
                id = primary_key_value
                data = {}
                for index, attribute in enumerate(config.ATTIBUTES):
                    if index == primary_key_index:
                        continue
                    data[attribute] = row[index]
                formatted_data.append({"id":id, "data":data})
            return formatted_data

def get_connection(dbname: str ) -> psycopg2.extensions.connection:
    return psycopg2.connect(
        dbname=dbname,
        user=config.DB_USER,
        password=config.DB_PASS,
        host=config.DB_HOST,
        port=config.DB_PORT
    )

def compare_dictionaries(dict1, dict2):
    # Check if the dictionaries have the same keys
    if set(dict1.keys()) != set(dict2.keys()):
        return False

    # Check if the values for each key are the same
    for key in dict1.keys():
        if dict1[key] != dict2[key]:
            return False

    # If all key-value pairs are the same, return True
    return True

if __name__ == "__main__":
    run()


