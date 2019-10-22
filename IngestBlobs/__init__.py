import logging
import os
import azure.functions as func

from ..shared_code import kusto_helpers
from ..shared_code import storage_helpers
from ..shared_code import request_helpers

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('IngestBlobs function processed a request.')

    INGEST_URI = os.environ['KUSTO_INGEST_URI']
    DATABASE = os.environ['KUSTO_DATABASE']
    AAD_TENANT_ID = os.environ['AAD_TENANT_ID']
    APPLICATION_ID = os.environ['APPLICATION_ID']
    APPLICATION_SECRET = os.environ['APPLICATION_SECRET']

    ingestKCSB = kusto_helpers.createKustoConnection(INGEST_URI,AAD_TENANT_ID, APPLICATION_ID, APPLICATION_SECRET)

    kustoClient = None
    if(ingestKCSB != None):
        kustoClient = kusto_helpers.getKustoClient(ingestKCSB)

    blobList = []
    table = ""

    try:
        req_body = req.get_json()
        blobList = req_body['blobList']
        table = req_body['table']
    except:
        logging.error("Could not request body.")

    if(kustoClient != None and len(blobList) > 0):

        for blob in blobList:

            if(table != '' and blob['table'] == table):

                logging.info('Queuing blob %s'%blob['name'])
                additionalProperties = {'ignoreFirstRecord': 'true'}
                kusto_helpers.ingestBlob(kustoClient,DATABASE,blob,additionalProperties)
                logging.info('Done queuing up ingestion with Azure Data Explorer')
                
                #TODO: update the storage table with new status

    if kustoClient:
        return func.HttpResponse(f"OK",status_code=200)
    else:
        return func.HttpResponse(
             "Error",
             status_code=400
        )
