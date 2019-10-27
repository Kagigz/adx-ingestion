import logging
import os
import azure.functions as func
import json

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

    blobToIngest = None
    try:
        blobToIngest = storage_helpers.createBlobFromMessage(msg.get_body())
        logging.info("Ingesting blob: %s"%str(blobToIngest))
    except Exception as e:
        logging.error("Could not get blobToIngest from queue message: %s"%e)

    ingestKCSB = kusto_helpers.createKustoConnection(INGEST_URI,AAD_TENANT_ID, APPLICATION_ID, APPLICATION_SECRET)
    kustoClient = None
    if(ingestKCSB != None):
        kustoClient = kusto_helpers.getKustoClient(ingestKCSB)

    tableService = storage_helpers.createTableService(STORAGE_NAME,STORAGE_KEY)

    if(kustoClient != None and blobToIngest != None and tableService != None):

        # Ingest blob in ADX
        blobToIngest['format'],blobToIngest['ingestionMapping'],blobToIngest['table'] = kusto_helpers.getMappingsBlob(blobToIngest['name'],MAPPINGS_FILE)
        logging.info('Queuing blob %s for ingestion to table %s'%(blobToIngest['name'],blobToIngest['table']))
        additionalProperties = {'ignoreFirstRecord': 'true'}
        kusto_helpers.ingestBlob(kustoClient,DATABASE,blobToIngest,additionalProperties)
        # Update blob status in status table to 'ingested'
        newBlobStatus = {'PartitionKey': CONTAINER, 'RowKey': blobToIngest['name'], 'status' : 'ingested'}
        storage_helpers.insertOrMergeEntity(tableService,STATUS_TABLE,newBlobStatus)

    else:
        logging.warning("Did not ingest blob successfully.")

        

