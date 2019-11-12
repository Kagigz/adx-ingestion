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
def create_blob_service(account_name, account_key):
    block_blob_service = None
    try:
        block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
    except Exception as e:
        logging.error("Could not instantiate blob service: %s"%e)
    return block_blob_service

# Lists all the blobs in a container
def list_blobs(blob_service, container_name):
    generator = None
    try:
        generator = blob_service.list_blobs(container_name)
        for blob in generator:
            print(blob.name)
    except Exception as e:
        logging.error("Could not list blobs in container %s: %s"%(container_name,e))
    return generator

# Creates a list of blob objects with name, path and size
def generate_blob_list(generator,container_name,account_name,sas_token):
    blobs = []
    for blob in generator:
        new_blob = {}
        new_blob['name'] = blob.name
        new_blob['path'] = "https://"+account_name+".blob.core.windows.net/"+container_name+"/"+blob.name+sas_token
        new_blob['size'] = blob.properties.content_length
        blobs.append(new_blob)
    return blobs


###############
# QUEUE STORAGE
###############

# Creates an Azure Queue Storage service
def create_queue_service(account_name, account_key):
    queue_service = None
    try:
        queue_service = QueueService(account_name=account_name, account_key=account_key)
        queue_service.encode_function = QueueMessageFormat.binary_base64encode
        queue_service.decode_function = QueueMessageFormat.binary_base64decode
    except Exception as e:
        logging.error("Could not instantiate queue service: %s"%e)
    return queue_service

# Adds a message to the queue
def add_to_queue(queue_service, queue, message):
    msg = base64.b64decode(message)
    try:
        queue_service.put_message(queue,message)
        logging.info("Message '%s' put in queue %s."%(msg,queue))
    except Exception as e:
        logging.error("Could not put message '%s' in queue %s: %s"%(msg, queue, e))

# Peeks at the messages in the queue
def peek_queue(queue_service,queue):
    messages = []
    try:
        messages = queue_service.peek_messages(queue)
        logging.info("%d messages found in queue %s"%(len(messages),queue))
        for message in messages:
            print(message.content)
    except Exception as e:
        logging.error("Could not get messages in queue %s: %s"%(queue, e))
    return messages

# Creates a message for the queue with a blob's properties
# A queue message must be a base64 encoded string
def create_queue_message(blob):
    msg = blob['name'] + '+' + str(blob['size']) + '+' + blob['path']
    msg = base64.b64encode(msg.encode('utf-8'))
    return msg

# Interprets a message from the queue to get a blob's properties
def create_blob_from_message(msg):
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
def create_table_service(account_name, account_key):
    table_service = None
    try:
        table_service = TableService(account_name=account_name, account_key=account_key)
    except Exception as e:
        logging.error("Could not instantiate table service: %s"%e)
    return table_service
    
# Creates an entity if it doesn't exist, updates it if it does
def insert_or_merge_entity(table_service, table_name, entity):
    try:
        table_service.insert_or_merge_entity(table_name, entity)
    except Exception as e:
        logging.error("Could not insert or update entity in table %s:%s"%(table_name,e))

# Queries a table to get an entity, returns None if the entity doesn't exist
def query_entity(table_service, table_name, partition_key, row_key):
    try:
        entity = table_service.get_entity(table_name, partition_key, row_key)
        status = entity.status
        return status
    except Exception as e:
        logging.info("Could not query entity %s in table %s:%s"%(row_key,table_name,e))
        return None

    