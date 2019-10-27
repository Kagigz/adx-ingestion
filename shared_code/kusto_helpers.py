import json
import logging

from azure.kusto.ingest import KustoIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, DataFormat, ReportLevel, ReportMethod
from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.ingest.status import KustoIngestStatusQueues

# Instantiate connection with ADX cluster
def createKustoConnection(uri, tenantID, appID, appKey):
    connectionString = None
    try:
        connectionString = KustoConnectionStringBuilder.with_aad_application_key_authentication(uri, appID, appKey, tenantID)
    except Exception as e:
        logging.error("Could not create a connection string:%s"%e)
    return connectionString

# Instantiante Kusto client
def getKustoClient(kcsb):
    client = None
    try:
        client = KustoIngestClient(kcsb)
    except Exception as e:
        logging.error("Could not initialize Kusto Client:%s"%e)
    return client

# From a blob name, gets file format, destination table and ingestion mapping using the mappings file
def getMappingsBlob(blobName,filePath):
    try:
        with open(filePath, 'r') as f:
            mappings = json.load(f)
        namePattern = blobName.split('-')[0]
        return (mappings[namePattern]["format"], mappings[namePattern]["ingestionMapping"], mappings[namePattern]["table"])
    except Exception as e:
        logging.error("Error mapping file name to format, table and ingestion mapping: %s"%e)

# Ingests blob to ADX
def ingestBlob(client,db,blob,properties):
    INGESTION_PROPERTIES = IngestionProperties(database=db, table=blob['table'], dataFormat=DataFormat(blob['format']), mappingReference=blob['ingestionMapping'], additionalProperties=properties, reportLevel=ReportLevel.FailuresAndSuccesses)
    BLOB_DESCRIPTOR = BlobDescriptor(blob['path'],blob['size'])
    try:
        client.ingest_from_blob(BLOB_DESCRIPTOR, ingestion_properties=INGESTION_PROPERTIES)
        logging.info("Blob %s ingested succesfully."%blob['name'])
    except Exception as e:
        logging.error("Error ingesting blob %s: %s"%(blob['name'],e))

# Queries ADX
# Not used here
def queryKusto(query,client, database):
    try:
        response = client.execute_query(database, query)
        logging.info("Query reponse: %s" % str(response))
        dataframe_from_result_table(response.primary_results[0])
    except Exception as e:
        logging.error("Could not process query:%s"%e)

# Gets status queue of ADX cluster
def getStatusQueue(client):
    statusQueue = None
    try:
        statusQueue = KustoIngestStatusQueues(client)
        logging.info("Initialized status queue successfully.")
    except Exception as e:
        logging.error("Error initializing status queue:%s"%e)
    return statusQueue

# Checks if a queue is empty
def isQueueEmpty(queue):
    return queue.is_empty()

# From a blob path, gets the name of a blob and the name of its container
def getBlobInfo(path):
    parts = path.split('/')
    blobName = parts[-1]
    containerName = parts[-2]
    return blobName, containerName

# Empties a queue
def emptyQueue(queue):
    messageList = []
    try:
        firstMessage = queue.pop()
        blobName, containerName = getBlobInfo(firstMessage[0].IngestionSourcePath)
        logging.info("First message in the queue: Blob '%s'"%blobName)
        newContainerName = containerName
        for message in firstMessage:
            messageList.append(message)
        # while the partition key (container name) is the same and the list length is < 100 (max batch size: 100), we continue adding to the list
        while(queue.is_empty() == False and len(messageList) < 100 and newContainerName == containerName):
            newMessage = queue.peek()
            blobName, newContainerName = getBlobInfo(newMessage[0].IngestionSourcePath)
            if(newContainerName == containerName):
                newMessage = queue.pop()
                for message in newMessage:
                    messageList.append(message)
        logging.info("A batch of messages was removed from the queue.")
    except Exception as e:
        logging.error("There was an error while emptying the queue:%s"%e)
    return messageList
