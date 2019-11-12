import json
import logging

from azure.kusto.ingest import KustoIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, DataFormat, ReportLevel, ReportMethod
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.ingest.status import KustoIngestStatusQueues

# Instantiate connection with ADX cluster
def create_kusto_connection(uri, tenant_ID, app_ID, app_key):
    connection_string = None
    try:
        connection_string = KustoConnectionStringBuilder.with_aad_application_key_authentication(uri, app_ID, app_key, tenant_ID)
    except Exception as e:
        logging.error("Could not create a connection string:%s"%e)
    return connection_string

# Instantiante Kusto client
def get_kusto_client(kcsb):
    client = None
    try:
        client = KustoIngestClient(kcsb)
    except Exception as e:
        logging.error("Could not initialize Kusto Client:%s"%e)
    return client

# From a blob name, gets file format, destination table and ingestion mapping using the mappings file
def get_mappings_blob(blob_name,filePath):
    try:
        with open(filePath, 'r') as f:
            mappings = json.load(f)
        name_pattern = blob_name.split('-')[0]
        return (mappings[name_pattern]["format"], mappings[name_pattern]["ingestionMapping"], mappings[name_pattern]["table"])
    except Exception as e:
        logging.error("Error mapping file name to format, table and ingestion mapping: %s"%e)

# Ingests blob to ADX
def ingest_blob(client,db,blob,properties):
    INGESTION_PROPERTIES = IngestionProperties(database=db, table=blob['table'], dataFormat=DataFormat(blob['format']), mappingReference=blob['ingestionMapping'], additionalProperties=properties, reportLevel=ReportLevel.FailuresAndSuccesses)
    BLOB_DESCRIPTOR = BlobDescriptor(blob['path'],blob['size'])
    try:
        client.ingest_from_blob(BLOB_DESCRIPTOR, ingestion_properties=INGESTION_PROPERTIES)
        logging.info("Blob %s ingested succesfully."%blob['name'])
    except Exception as e:
        logging.error("Error ingesting blob %s: %s"%(blob['name'],e))

# Queries ADX
# Not used here
def query_kusto(query,client, database):
    try:
        response = client.execute_query(database, query)
        logging.info("Query reponse: %s" % str(response))
        dataframe_from_result_table(response.primary_results[0])
    except Exception as e:
        logging.error("Could not process query:%s"%e)

# Gets status queue of ADX cluster
def get_status_queue(client):
    status_queue = None
    try:
        status_queue = KustoIngestStatusQueues(client)
        logging.info("Initialized status queue successfully.")
    except Exception as e:
        logging.error("Error initializing status queue:%s"%e)
    return status_queue

# Checks if a queue is empty
def is_queue_empty(queue):
    return queue.is_empty()

# From a blob path, gets the name of a blob and the name of its container
def get_blob_info(path):
    parts = path.split('/')
    blob_name = parts[-1]
    container_name = parts[-2]
    return blob_name, container_name

# Empties a queue
def empty_queue(queue):
    message_list = []
    try:
        first_message = queue.pop()
        blob_name, container_name = get_blob_info(first_message[0].IngestionSourcePath)
        logging.info("First message in the queue: Blob '%s'"%blob_name)
        new_container_name = container_name
        for message in first_message:
            message_list.append(message)
        # while the partition key (container name) is the same and the list length is < 100 (max batch size: 100), we continue adding to the list
        while(queue.is_empty() == False and len(message_list) < 100 and new_container_name == container_name):
            new_message = queue.peek()
            blob_name, new_container_name = get_blob_info(new_message[0].IngestionSourcePath)
            if(new_container_name == container_name):
                new_message = queue.pop()
                for message in new_message:
                    message_list.append(message)
        logging.info("A batch of messages was removed from the queue.")
    except Exception as e:
        logging.error("There was an error while emptying the queue:%s"%e)
    return message_list
