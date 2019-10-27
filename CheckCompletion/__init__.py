import datetime
import logging
import os
import azure.functions as func
import json

from ..shared_code import kusto_helpers
from ..shared_code import storage_helpers

def main(mytimer: func.TimerRequest) -> None:

    logging.info('CheckCompletion function processed a request.')
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')

    INGEST_URI = os.environ['KUSTO_INGEST_URI']
    AAD_TENANT_ID = os.environ['AAD_TENANT_ID']
    APPLICATION_ID = os.environ['APPLICATION_ID']
    APPLICATION_SECRET = os.environ['APPLICATION_SECRET']
    STORAGE_NAME = os.environ['STORAGE_ACCOUNT_NAME']
    STORAGE_KEY = os.environ['STORAGE_ACCOUNT_KEY']
    STATUS_TABLE = os.environ['STATUS_TABLE']

    ingestKCSB = kusto_helpers.createKustoConnection(INGEST_URI,AAD_TENANT_ID, APPLICATION_ID, APPLICATION_SECRET)

    tableService = storage_helpers.createTableService(STORAGE_NAME,STORAGE_KEY)

    kustoClient = None
    if(ingestKCSB != None):
        kustoClient = kusto_helpers.getKustoClient(ingestKCSB)
        statusQueue = kusto_helpers.getStatusQueue(kustoClient)

        if(kustoClient != None and tableService != None and statusQueue != None):

                # Checking success queue to see if ingestion of blobs succeeded
                if(kusto_helpers.isQueueEmpty(statusQueue.success) == True):
                    logging.info("Ingestion success queue is empty.")
                else:
                    logging.info("Ingestion success queue contains new messages.")
                    successMessagesList = kusto_helpers.emptyQueue(statusQueue.success)

                    # updating the successfully ingested blobs
                    for successMessage in successMessagesList:
                        print("Success Message: ")
                        print(successMessage)
                        blobName, containerName = kusto_helpers.getBlobInfo(successMessage.IngestionSourcePath)
                        # Update status table with success status
                        newBlobStatus = {'PartitionKey': containerName, 'RowKey': blobName, 'status' : 'success'}
                        storage_helpers.insertOrMergeEntity(tableService,STATUS_TABLE,newBlobStatus)

                # Checking failure queue to see if ingestion failed for some blobs
                if(kusto_helpers.isQueueEmpty(statusQueue.failure) == False):
                    logging.warning("There are new messages in the Ingestion failure queue.")
                    failureMessagesList = kusto_helpers.emptyQueue(statusQueue.failure)

                    # updating the blobs whose ingestion failed
                    for failureMessage in failureMessagesList:
                        blobName, containerName = kusto_helpers.getBlobInfo(failureMessage.IngestionSourcePath)
                        # Update status table with failure status
                        newBlobStatus = {'PartitionKey': containerName, 'RowKey': blobName, 'status' : 'failure'}
                        storage_helpers.insertOrMergeEntity(tableService,STATUS_TABLE,newBlobStatus)
        
        else:
            logging.warning("Could not check if ingestion was completed.")
        

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

