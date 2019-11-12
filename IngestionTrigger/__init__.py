import logging
import os, uuid, sys
import azure.functions as func
import asyncio
import uuid
import json
import pathlib

from ..shared_code import storage_helpers

def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('IngestionTrigger function processed a request.')

    STORAGE_NAME = os.environ['STORAGE_ACCOUNT_NAME']
    STORAGE_KEY = os.environ['STORAGE_ACCOUNT_KEY']
    SAS_TOKEN = os.environ['STORAGE_SAS_TOKEN']
    CONTAINER = os.environ['DATA_CONTAINER']
    STATUS_TABLE = os.environ['STATUS_TABLE']
    UPLOAD_QUEUE = os.environ['UPLOAD_QUEUE']

    blob_service = storage_helpers.create_blob_service(STORAGE_NAME,STORAGE_KEY)
    queue_service = storage_helpers.create_queue_service(STORAGE_NAME,STORAGE_KEY)
    table_service = storage_helpers.create_table_service(STORAGE_NAME,STORAGE_KEY)

    if(blob_service != None and queue_service != None and table_service != None):

        print("OK")
        blob_generator = storage_helpers.list_blobs(blob_service,CONTAINER)        
        # creating a blob list
        blobs = storage_helpers.generate_blob_list(blob_generator,CONTAINER,STORAGE_NAME,SAS_TOKEN)
        blobs_to_ingest = []

        for blob in blobs:
            # Check if blob was already ingested or not with the status table
            # The partition key is the container name and the row key is the blob name 
            status = "NotFound"
            blob_status = storage_helpers.query_entity(table_service,STATUS_TABLE,CONTAINER,blob['name'])
            if(blob_status != None):
                status = blob_status
            # If the blob was never ingested or the ingestion failed, we want to ingest it
            if(status == 'NotFound' or status == 'failure'):
                blobs_to_ingest.append(blob)
                storage_helpers.add_to_queue(queue_service,UPLOAD_QUEUE,storage_helpers.create_queue_message(blob))
                # Update status of blob to "queued"
                newblob_status = {'PartitionKey': CONTAINER, 'RowKey': blob['name'], 'status' : 'queued'}
                storage_helpers.insert_or_merge_entity(table_service,STATUS_TABLE,newblob_status)
            
        logging.info("%d blobs found in %s" % (len(blobs),CONTAINER))
        logging.info("%d blobs to ingest in %s" % (len(blobs_to_ingest),CONTAINER))

    else:
        logging.warning("Could not trigger the ingestion process.")

    if blob_service:
        return func.HttpResponse(f"OK",status_code=200)
    else:
        return func.HttpResponse(
             "Error",
             status_code=400
        )
