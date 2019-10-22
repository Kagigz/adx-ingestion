import logging
from azure.storage.blob import BlockBlobService, PublicAccess
from azure.storage.queue import QueueService
#from azure.cosmosdb.table.tableservice import TableService
#from azure.cosmosdb.table.models import Entity
#from azure.cosmosdb.table.tablebatch import TableBatch

def createBlobService(accountName, accountKey):
    block_blob_service = None
    try:
        block_blob_service = BlockBlobService(account_name=accountName, account_key=accountKey)
    except:
        logging.error("Could not instantiate blob service.")
    return block_blob_service

def listBlobs(blobService, containerName):
    try:
        generator = blobService.list_blobs(containerName)
        #for blob in generator:
            #print(blob.name)
    except:
        logging.error("Could not list blobs in container %s." % containerName)
    return generator

def generateBlobList(generator,containerName,accountName,sas_token):
    blobs = []
    for blob in generator:
        newBlob = {}
        newBlob['name'] = blob.name
        newBlob['container'] = containerName
        newBlob['path'] = "https://"+accountName+".blob.core.windows.net/"+containerName+"/"+blob.name+sas_token
        newBlob['table'] = blob.name.split('-')[0]
        newBlob['size'] = blob.properties.content_length
        blobs.append(newBlob)
    return blobs

""" def createTableService(accountName, accountKey):
    table_service = None
    try:
        table_service = TableService(account_name=accountName, account_key=accountKey)
    except:
        logging.error("Could not instantiate table service.")
    return table_service
    
def insertEntity(tableService, tableName, entity):
    try:
        tableService.insert_entity(tableName, entity)
    except:
        logging.error("Could not insert entity into table %s."%tableName)

def updateEntity(tableService, tableName, entity):
    try:
        tableService.update_entity(tableName, entity)
    except:
        logging.error("Could not update entity in table %s."%tableName)

def insertOrReplaceEntity(tableService, tableName, entity):
    try:
        tableService.insert_or_replace_entity(tableName, entity)
    except:
        logging.error("Could not insert or update entity in table %s."%tableName)

def batchUpdateEntities(tableService,tableName, entities):
    batch = TableBatch()
    for entity in entities:
        batch.insert_or_replace_entity(entity)
    try:
        tableService.commit_batch(tableName, batch)
    except:
        logging.error("Could not insert or update entities in batch in table %s."%tableName)


def queryEntity(tableService, tableName, partitionKey, rowKey):
    description = ""
    try:
        entity = tableService.get_entity(tableName, partitionKey, rowKey)
        description = entity.description
    except:
        logging.error("Could not insert or update entity in table %s."%tableName)
    return description
 """

    