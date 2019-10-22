import datetime
import logging
import os
import azure.functions as func
import json

from ..shared_code import kusto_helpers
from ..shared_code import storage_helpers
from ..shared_code import request_helpers

def main(mytimer: func.TimerRequest) -> None:
    logging.info('CheckCompletion function processed a request.')
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    INGEST_URI = os.environ['KUSTO_INGEST_URI']
    AAD_TENANT_ID = os.environ['AAD_TENANT_ID']
    APPLICATION_ID = os.environ['APPLICATION_ID']
    APPLICATION_SECRET = os.environ['APPLICATION_SECRET']

    ingestKCSB = kusto_helpers.createKustoConnection(INGEST_URI,AAD_TENANT_ID, APPLICATION_ID, APPLICATION_SECRET)

    kustoClient = None
    if(ingestKCSB != None):
        kustoClient = kusto_helpers.getKustoClient(ingestKCSB)
        if(kustoClient != None):
            statusQueue = kusto_helpers.getStatusQueue(kustoClient)
            if(statusQueue != None):
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
                        #TODO: Update status table with success status


                if(kusto_helpers.isQueueEmpty(statusQueue.failure) == False):
                    logging.info("There are new messages in the Ingestion failure queue.")
                    failureMessagesList = kusto_helpers.emptyQueue(statusQueue.failure)

                    # updating the blobs whose ingestion failed
                    for failureMessage in failureMessagesList:
                        blobName, containerName = kusto_helpers.getBlobInfo(failureMessage.IngestionSourcePath)
                        #TODO: Update status table with failure status
        

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

