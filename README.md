# Ingestion Pipeline for Azure Data Explorer (Kusto)

This repo is a **complete ingestion pipeline for ADX using Azure Python Functions**.
It allows to trigger the ingestion process that will check if new data needs to be ingested, then check if the ingestion was successful.

It can be used *as is*, but the idea is to use it in combination with an app for a better UX. The app should allow to trigger the ingestion process with the click of a button and show status updates to check that the data was ingested successfully.

## Description

The function app is composed of 3 functions:

**1. IngestionTrigger Function (HTTP triggered)**

This function lists all the blobs in a given container (configurable in settings) in Blob Storage. It then checks the status of each blob in a status table and queues the blobs that need to be ingested - they are either new or their previous ingestion failed. The status of the blobs are updated to "queued".

**2. IngestBlobs Function (Queue triggered)**

When blobs that need to be ingested are queued for ingestion, this function is triggered and creates a connection to ADX to ingest each blob. It then updates the status of the blobs to "ingested".

**3. CheckCompletion Function (Time triggered)**

Ingestion of blobs to ADX can take a few minutes, so we want to check that the newly ingested blobs have been successfully ingested by looking at the ingestion success and failure queues. Every minute, this function inspects the ingestion success and failure queues of the ADX cluster and if there are new messages (they are usually visible a few minutes after ingestion), it updates the blobs' status accordingly. The ingestion queues will not empty themselves until we retrieve the messages, so we're sure not to miss status updates.

## How to use

### Requirements

- Azure Function app
- ADX cluster with at least 1 database
- Service principal to access your ADX database (see [here](https://docs.microsoft.com/en-us/azure/kusto/management/access-control/how-to-provision-aad-app) for more information)
- Multipurpose Storage Account

### Steps

- Create a table (for status updates), a queue (for ingestion queue), and a blob container (for your data) in your storage account
- Add data in your container - you can use the data folder in this repo for sample data
- Create a table in your database
```
CREATE_TABLE_COMMAND = ".create table TrafficFlows (FlowID: string, SourceIP: string, DestinationIP: string, DestinationPort: int, Timestamp: string, FlowDuration: int)"
```
- Create a new ingestion mapping for your database
```
CREATE_MAPPING_COMMAND = """.create table TrafficFlows ingestion csv mapping 'TrafficFlows_CSV_Mapping' '[{"Name":"FlowID","datatype":"string","Ordinal":0}, {"Name":"SourceIP","datatype":"string","Ordinal":1},{"Name":"DestinationIP","datatype":"string","Ordinal":3},{"Name":"DestinationPort","datatype":"int","Ordinal":4},{"Name":"Timestamp","datatype":"string","Ordinal":6},{"Name":"FlowDuration","datatype":"int","Ordinal":7}]'"""
```
- Configure app settings (see below)
- Publish the code to your Function app

**Note:** If you don't want to use the sample data in this repo, and have your own data and own tables/ingestion mappings, you can edit the *mappings.json* file. The naming pattern for your data should be <NAME>-****. In the mappings file, you can then specify the format, destination table and ingestion mapping corresponding to each name.

### App settings

These are the environment variables you should configure before running this app (either on Azure or locally, in the *local.settings.json* file):

```
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsStorage": "<FUNCTIONS_STORAGE_CONNECTION_STRING>",
    "AzureStorage": "<GENERAL_STORAGE_CONNECTION_STRING>",
    "AAD_TENANT_ID": "<YOUR_TENANT_ID>",
    "APPLICATION_ID": "<SERVICE_PRINCIPAL_APP_ID>",
    "APPLICATION_SECRET": "<SERVICE_PRINCIPAL_APP_SECRET>",
    "KUSTO_URI": "https://<YOUR_CLUSTER_NAME>.<YOUR_CLUSTER_REGION>.kusto.windows.net",
    "KUSTO_INGEST_URI": "https://ingest-<YOUR_CLUSTER_NAME>.<YOUR_CLUSTER_REGION>.kusto.windows.net",
    "KUSTO_DATABASE": "<YOUR_DB_NAME>",
    "STORAGE_ACCOUNT_NAME": "<GENERAL_STORAGE_NAME>",
    "STORAGE_ACCOUNT_KEY": "<GENERAL_STORAGE_KEY>",
    "STORAGE_SAS_TOKEN": "<GENERAL_STORAGE_SAS_TOKEN>",
    "DATA_CONTAINER":"data",
    "STATUS_TABLE": "status",
    "UPLOAD_QUEUE":"uploadqueue",
    "MAPPINGS_FILE": "mappings.json"
  }
}
```


