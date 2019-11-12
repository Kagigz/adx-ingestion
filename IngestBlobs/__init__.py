import logging
import os
import azure.functions as func
import json
from pathlib import Path

from ..shared_code import kusto_helpers
from ..shared_code import storage_helpers

def main(msg: func.QueueMessage):

    logging.info('IngestBlobs function processed a request.')

    INGEST_URI = os.environ['KUSTO_INGEST_URI']
    DATABASE = os.environ['KUSTO_DATABASE']
    AAD_TENANT_ID = os.environ['AAD_TENANT_ID']
    APPLICATION_ID = os.environ['APPLICATION_ID']
    APPLICATION_SECRET = os.environ['APPLICATION_SECRET']
    MAPPINGS_FILE = os.environ['MAPPINGS_FILE']
    STORAGE_NAME = os.environ['STORAGE_ACCOUNT_NAME']
    STORAGE_KEY = os.environ['STORAGE_ACCOUNT_KEY']
    CONTAINER = os.environ['DATA_CONTAINER']
    STATUS_TABLE = os.environ['STATUS_TABLE']

    path_mappings = os.path.abspath(os.path.join(os.path.dirname("__init__.py"), MAPPINGS_FILE))
    logging.info("Mappings file path: %s"%path_mappings)

    blob_to_ingest = None
    try:
        blob_to_ingest = storage_helpers.create_blob_from_message(msg.get_body())
        logging.info("Ingesting blob: %s"%str(blob_to_ingest))
    except Exception as e:
        logging.error("Could not get blob_to_ingest from queue message: %s"%e)

    ingest_KCSB = kusto_helpers.create_kusto_connection(INGEST_URI,AAD_TENANT_ID, APPLICATION_ID, APPLICATION_SECRET)
    kusto_client = None
    if(ingest_KCSB != None):
        kusto_client = kusto_helpers.get_kusto_client(ingest_KCSB)

    table_service = storage_helpers.create_table_service(STORAGE_NAME,STORAGE_KEY)

    if(kusto_client != None and blob_to_ingest != None and table_service != None):

        # Ingest blob in ADX
        blob_to_ingest['format'],blob_to_ingest['ingestionMapping'],blob_to_ingest['table'] = kusto_helpers.get_mappings_blob(blob_to_ingest['name'],path_mappings)
        logging.info('Queuing blob %s for ingestion to table %s'%(blob_to_ingest['name'],blob_to_ingest['table']))
        additional_properties = {'ignoreFirstRecord': 'true'}
        kusto_helpers.ingest_blob(kusto_client,DATABASE,blob_to_ingest,additional_properties)
        # Update blob status in status table to 'ingested'
        new_blob_status = {'PartitionKey': CONTAINER, 'RowKey': blob_to_ingest['name'], 'status' : 'ingested'}
        storage_helpers.insert_or_merge_entity(table_service,STATUS_TABLE,new_blob_status)

    else:
        logging.warning("Did not ingest blob successfully.")

        

