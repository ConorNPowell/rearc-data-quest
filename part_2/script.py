### Importing Packages needed for project
import boto3
import pandas as pd
import numpy as np
import bs4
import requests
import logging
import datetime as datetime
import os
import re
import json
from datetime import timezone

logging.basicConfig(level=logging.INFO)

###Setting hard coded variables
data_usa_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
s3_bucket_name = 'rearc-data-quest-conor'
s3_folder = 'usa_data/'
formatted_date = datetime.datetime.now(timezone.utc).strftime("%m_%d_%Y")
delete_folder_filter = datetime.datetime.now(timezone.utc) - datetime.timedelta(days=2)

###Creating boto3 S3 client
s3_client = boto3.client('s3')

###Creating session to connect to API
logging.info(f'Creating session to connect to USA Data API at {datetime.datetime.now(timezone.utc)}')
usa_session = requests.session()

###Hitting Data USA API
data_usa_resposne = usa_session.get(data_usa_url)

###Proceed with logic if we get a 200
if data_usa_resposne.status_code == 200:
    logging.info(f'Sucuessfully received 200 from USA Data API at {datetime.datetime.now(timezone.utc)}')

    ###Making sure data comes in as JSON
    usa_data = data_usa_resposne.json()
    usa_json_data = json.dumps(usa_data)

    ###Formatting s3 folder info
    s3_folder_key = s3_folder+formatted_date+'/usa_data_api.json'

    ###Uploading to s3 folder
    logging.info(f'Uploading JSON data to S3 Bucket at {datetime.datetime.now(timezone.utc)}')
    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_folder_key, Body=usa_json_data)
    logging.info(f'Upload of USA Data API in JSON was uploaded at {datetime.datetime.now(timezone.utc)}')
else:
    logging.critical(f'Issue with connection to the Data USA API. Status Code {data_usa_response.status_code} given at {datetime.datetime.now()}')


###List folders in bucket function
def list_folders(bucket_name, folder_prefix):
    ###Getting meta data on folders present
    logging.info(f'Grabbing list of files in bucket {bucket_name} for folder {folder_prefix} for deletion at {datetime.datetime.now(timezone.utc)}')
    list_of_folders = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=folder_prefix, Delimiter='/')
    
    ###Empty variable to appened folder names into
    folders = []
    
    ###If statement for filtering data
    if 'CommonPrefixes' in list_of_folders:
        ###Loop through meta data
        for obj in list_of_folders['CommonPrefixes']:
            ###Appending folder name to empty folders variable
            folders.append(obj.get('Prefix'))
    return folders

###Delete folders listed from list_folders function
def delete_folder(bucket_name, folder_name):
    ###Getting all objects
    logging.info(f'Starting function to delete {folder_name} from bucket, {bucket_name}, at {datetime.datetime.now(timezone.utc)}')
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    delete_keys = {'Objects': []}

    ###Collection all object keys within the folder to delete
    if 'Contents' in objects_to_delete:
        for obj in objects_to_delete['Contents']:
            delete_keys['Objects'].append({'Key': obj['Key']})
    ###Deleting the collected keys
    if delete_keys['Objects']:
        logging.info(f'Attempting to delete folder, {folder_name}, in bucket, {bucket_name}, at {datetime.datetime.now(timezone.utc)}')
        s3_client.delete_objects(Bucket=bucket_name, Delete=delete_keys)
        logging.info(f'Deleted folder, {folder_name}, in bucket, {bucket_name}, at {datetime.datetime.now(timezone.utc)}')

###Calling list_folders function
folders = list_folders(s3_bucket_name, s3_folder)

###For loop for deletion
for folder in folders:
    ###Clean string for matching
    date_str = folder[len(s3_folder):-1] ###Get rid of prefix and ending slash
    try:
        ###Getting the actual folder date
        folder_date = datetime.datetime.strptime(date_str, '%m_%d_%Y').replace(tzinfo=timezone.utc)
        ###If statement for management
        if folder_date < delete_folder_filter:
            delete_folder(s3_bucket_name, folder)
    except ValueError:
        logging.critical(f'Skipping folder, {folder}, at {datetime.datetime.now(timezone.utc)}')
