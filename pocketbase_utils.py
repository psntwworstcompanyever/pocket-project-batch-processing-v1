import logging
from pocketbase import PocketBase
from pocketbase.client import ClientResponseError

def initialize_pocketbase_client(url):
    return PocketBase(url)

def get_filtered_collection(client, collection_name, query_params=None):
    try:
        logging.info(f"Fetching data from collection: {collection_name} with params: {query_params}")
        records = client.collection(collection_name).get_full_list(
            query_params=query_params
        )
        logging.info(f"Records fetched: {len(records)}")
        return records
    except ClientResponseError as e:
        logging.error(f"HTTP error occurred: {e}")
        return None

def get_full_collection(client, collection_name):
    try:
        records = client.collection(collection_name).get_full_list()
        return records
    except ClientResponseError as e:
        logging.error(f"Failed to retrieve records: {e}")
        return None

def update_record_status(client, record_id, new_status):
    try:
        updated_record = client.collection("projects").update(
            record_id, {"status": new_status}
        )
        return updated_record
    except ClientResponseError as e:
        logging.error(f"HTTP error occurred: {e}")
    except Exception as err:
        logging.error(f"An error occurred: {err}")
