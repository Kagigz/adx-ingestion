import logging
import os, uuid, sys
import azure.functions as func
import asyncio
import uuid
import json
import pathlib

from ..shared_code import storage_helpers
from ..shared_code import request_helpers

def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('IngestionTrigger function processed a request.')

    STORAGE_NAME = os.environ['STORAGE_ACCOUNT_NAME']
    STORAGE_KEY = os.environ['STORAGE_ACCOUNT_KEY']
    SAS_TOKEN = os.environ['STORAGE_SAS_TOKEN']
    CONTAINER = os.environ['DATA_CONTAINER']
    STATUS_TABLE = os.environ['STATUS_TABLE']
    OPERATIONS_TABLE = os.environ['OPERATIONS_TABLE']
    UPLOAD_QUEUE = os.environ['UPLOAD_QUEUE']

    blobService = storage_helpers.createBlobService(STORAGE_NAME,STORAGE_KEY)
    queueService = storage_helpers.createQueueService(STORAGE_NAME,STORAGE_KEY)

    if(blobService != None and queueService != None):

        print("OK")
        blobGenerator = storage_helpers.listBlobs(blobService,CONTAINER)        
        # creating a blob list
        blobs = storage_helpers.generateBlobList(blobGenerator,CONTAINER,STORAGE_NAME,SAS_TOKEN)
        blobsToIngest = []

        for blob in blobs:
            #TODO: Check status of blob
            #status = blob['status']
            status = "NotFound"
            # If the blob was never ingested or the ingestion failed, we want to ingest it
            if(status == 'NotFound' or status == 'failure'):
                blobsToIngest.append(blob)
                storage_helpers.addToQueue(queueService,UPLOAD_QUEUE,storage_helpers.createQueueMessage(blob))
                #TODO: update status of blob to "queued"
            
        logging.info("%d blobs found in %s" % (len(blobs),CONTAINER))
        logging.info("%d blobs to ingest in %s" % (len(blobsToIngest),CONTAINER))

        #if(len(blobsToIngest) > 0):
        #TODO: put message in queue with blob path
        #TODO: Update operations table with new operation ID

    else:
        logging.error("Could not process the request.")

    if blobService:
        return func.HttpResponse(f"OK",status_code=200)
    else:
        return func.HttpResponse(
             "Error",
             status_code=400
        )
