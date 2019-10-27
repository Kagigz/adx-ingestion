import logging
import hashlib
import base64
import hmac
import datetime

from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.queue import QueueService, QueueMessageFormat
from azure.cosmosdb.table.tableservice import TableService
from azure.cosmosdb.table.models import Entity

##############
# BLOB STORAGE
##############

# Creates an Azure Blob Storage service
def createBlobService(accountName, accountKey):
    block_blob_service = None
    try:
        block_blob_service = BlockBlobService(account_name=accountName, account_key=accountKey)
    except Exception as e:
        logging.error("Could not instantiate blob service: %s"%e)
    return block_blob_service

# Lists all the blobs in a container
def listBlobs(blobService, containerName):
    generator = None
    try:
        generator = blobService.list_blobs(containerName)
        for blob in generator:
            print(blob.name)
    except Exception as e:
        logging.error("Could not list blobs in container %s: %s"%(containerName,e))
    return generator

# Creates a list of blob objects with name, path and size
def generateBlobList(generator,containerName,accountName,sas_token):
    blobs = []
    for blob in generator:
        newBlob = {}
        newBlob['name'] = blob.name
        newBlob['path'] = "https://"+accountName+".blob.core.windows.net/"+containerName+"/"+blob.name+sas_token
        newBlob['size'] = blob.properties.content_length
        blobs.append(newBlob)
    return blobs


###############
# QUEUE STORAGE
###############

# Creates an Azure Queue Storage service
def createQueueService(accountName, accountKey):
    queue_service = None
    try:
        queue_service = QueueService(account_name=accountName, account_key=accountKey)
        queue_service.encode_function = QueueMessageFormat.binary_base64encode
        queue_service.decode_function = QueueMessageFormat.binary_base64decode
    except Exception as e:
        logging.error("Could not instantiate queue service: %s"%e)
    return queue_service

# Adds a message to the queue
def addToQueue(queueService, queue, message):
    msg = base64.b64decode(message)
    try:
        queueService.put_message(queue,message)
        logging.info("Message '%s' put in queue %s."%(msg,queue))
    except Exception as e:
        logging.error("Could not put message '%s' in queue %s: %s"%(msg, queue, e))

# Peeks at the messages in the queue
def peekQueue(queueService,queue):
    messages = []
    try:
        messages = queueService.peek_messages(queue)
        logging.info("%d messages found in queue %s"%(len(messages),queue))
        for message in messages:
            print(message.content)
    except Exception as e:
        logging.error("Could not get messages in queue %s: %s"%(queue, e))
    return messages

# Creates a message for the queue with a blob's properties
# A queue message must be a base64 encoded string
def createQueueMessage(blob):
    msg = blob['name'] + '+' + str(blob['size']) + '+' + blob['path']
    msg = base64.b64encode(msg.encode('utf-8'))
    return msg

# Interprets a message from the queue to get a blob's properties
def createBlobFromMessage(msg):
    blob = {}
    msg = base64.b64decode(msg).decode('utf-8')
    parts = msg.split('+')
    blob['name'] = parts[0]
    blob['size'] = parts[1]
    blob['path'] = parts[2]
    return blob
       

###############
# TABLE STORAGE
###############

# Creates an Azure Table Storage service
def createTableService(accountName, accountKey):
    table_service = None
    try:
        table_service = TableService(account_name=accountName, account_key=accountKey)
    except Exception as e:
        logging.error("Could not instantiate table service: %s"%e)
    return table_service
    
# Creates an entity if it doesn't exist, updates it if it does
def insertOrMergeEntity(tableService, tableName, entity):
    try:
        tableService.insert_or_merge_entity(tableName, entity)
    except Exception as e:
        logging.error("Could not insert or update entity in table %s:%s"%(tableName,e))

# Queries a table to get an entity, returns None if the entity doesn't exist
def queryEntity(tableService, tableName, partitionKey, rowKey):
    try:
        entity = tableService.get_entity(tableName, partitionKey, rowKey)
        status = entity.status
        return status
    except Exception as e:
        logging.info("Could not query entity %s in table %s:%s"%(rowKey,tableName,e))
        return None

    