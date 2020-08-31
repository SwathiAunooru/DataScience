from google.cloud import storage
import glob
import os
import logging

def uploadBlob_text(bucketName,text,timestr):
    logging.info("upload blog report logging")

    os.chdir("/application/raman_docker")
    cwd = os.getcwd()
    logging.info(cwd)
    
    jsConfigPath = cwd + '/GCSConfigJson.json'
    logging.info(jsConfigPath)
    # storage_client = storage.Client.from_service_account_json(r'D:\data-ingestion\main\raman_docker\GCSConfigJson.json')
    storage_client = storage.Client.from_service_account_json(jsConfigPath)
    bucket = storage_client.bucket(bucketName)
    filename = "Report"+str(timestr)+".txt"
    blob = bucket.blob(filename)
    blob.upload_from_string(text)
