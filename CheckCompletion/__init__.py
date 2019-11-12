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

    ingest_KCSB = kusto_helpers.create_kusto_connection(INGEST_URI,AAD_TENANT_ID, APPLICATION_ID, APPLICATION_SECRET)

    table_service = storage_helpers.create_table_service(STORAGE_NAME,STORAGE_KEY)

    kusto_client = None
    if(ingest_KCSB != None):
        kusto_client = kusto_helpers.get_kusto_client(ingest_KCSB)
        status_queue = kusto_helpers.get_status_queue(kusto_client)

        if(kusto_client != None and table_service != None and status_queue != None):

                # Checking success queue to see if ingestion of blobs succeeded
                if(kusto_helpers.is_queue_empty(status_queue.success) == True):
                    logging.info("Ingestion success queue is empty.")
                else:
                    logging.info("Ingestion success queue contains new messages.")
                    success_messages_list = kusto_helpers.empty_queue(status_queue.success)

                    # updating the successfully ingested blobs
                    for success_message in success_messages_list:
                        print("Success Message: ")
                        print(success_message)
                        blob_name, container_name = kusto_helpers.get_blob_info(success_message.IngestionSourcePath)
                        # Update status table with success status
                        new_blob_status = {'PartitionKey': container_name, 'RowKey': blob_name, 'status' : 'success'}
                        storage_helpers.insert_or_merge_entity(table_service,STATUS_TABLE,new_blob_status)

                # Checking failure queue to see if ingestion failed for some blobs
                if(kusto_helpers.is_queue_empty(status_queue.failure) == False):
                    logging.warning("There are new messages in the Ingestion failure queue.")
                    failure_messages_list = kusto_helpers.empty_queue(status_queue.failure)

                    # updating the blobs whose ingestion failed
                    for failureMessage in failure_messages_list:
                        blob_name, container_name = kusto_helpers.get_blob_info(failureMessage.IngestionSourcePath)
                        # Update status table with failure status
                        new_blob_status = {'PartitionKey': container_name, 'RowKey': blob_name, 'status' : 'failure'}
                        storage_helpers.insert_or_merge_entity(table_service,STATUS_TABLE,new_blob_status)
        
        else:
            logging.warning("Could not check if ingestion was completed.")
        

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

