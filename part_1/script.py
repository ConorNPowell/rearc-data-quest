import requests
from bs4 import BeautifulSoup
import os
import re
import boto3
from datetime import datetime
from datetime import timezone
import logging
import pytz


##Defining function to get the last modified time from S3
def get_s3_last_modified_time(bucket_name, key):
    try:
        s3_client = boto3.client('s3')
        last_modifed = None
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key)
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'] == key:
                    last_modified = obj['LastModified'].replace(tzinfo=timezone.utc)
        return last_modified
    except Exception as e:
        logging.info(f'Error getting S3 object metadata: {e}. At {datetime.now(timezone.utc)}')
        return None

###Logging session
logging.basicConfig(level=logging.INFO)
logging.info(f'Starting upload of BLS Data at {datetime.now(timezone.utc)}')

###S3 stuff
s3_client = boto3.client('s3')
s3_bucket_name = 'rearc-data-quest-conor'
s3_folder = 'bls_data/'

# Base URL
bls_url = 'https://download.bls.gov/pub/time.series/pr/'
bls_headers = {'User-Agent': 'conor.n.powell@gmail.com'}

###Hitting BLS Website
logging.info(f'Hitting BLS Website at {datetime.now(timezone.utc)}')
bls_session = requests.session()
bls_response = bls_session.get(bls_url, headers=bls_headers)

###Proceeding with logic if we get a good connection
if bls_response.status_code == 200:
    logging.info(f'Sucuessfully hit BLS Site and received a status code 200 at {datetime.now(timezone.utc)}')
    ###Parsing BLS Website with
    bls_soup_parsed = BeautifulSoup(bls_response.text, 'html.parser')

    file_info = []

    # Find the pre tag
    pre_tag = bls_soup_parsed.find('pre')

    # Process each line in the pre tag
    for line in pre_tag.find_all('br'):
        if line.previousSibling.text == "[To Parent Directory]":
            continue
        else:
            if "pr." in line.previousSibling.text:
                last_modified = f"{line.previousSibling.previousSibling.strip().split(' ')[0]} {line.previousSibling.previousSibling.strip().split(' ')[2]}{line.previousSibling.previousSibling.strip().split(' ')[3]}"
                file_info.append( {
                        'href': line.previousSibling.text,
                        'last_modified': datetime.strptime(last_modified, "%m/%d/%Y %I:%M%p")
                    })

    ###Empty directory in order to save files to them
    bls_download_dir = 'bls_files'
    os.makedirs(bls_download_dir, exist_ok=True)

    ###For loop to download each file and compare upload dates in S3
    for info in file_info:
        file_url = bls_url + info['href']
        file_bls_response = requests.get(file_url, headers=bls_headers)
        ###If statement to only proceed if we get a good status code
        if bls_response.status_code == 200:
            with open(os.path.join(bls_download_dir, info['href']), 'wb') as file:
                file.write(bls_response.content)

            ###Pulling last modified date to check if we need to upload
            last_modified_local = info['last_modified']
            last_modified_local = last_modified_local.replace(tzinfo=timezone.utc)
            s3_last_modified = get_s3_last_modified_time(s3_bucket_name,'bls_data/' + info['href'])
            if s3_last_modified is None or last_modified_local > s3_last_modified:
                    for root, dirs, files in os.walk(bls_download_dir):
                        for file_name in files:
                            if info['href'] in file_name:
                                ###Setting up file and S3 paths
                                file_path = os.path.join(root, file_name)
                                s3_key = os.path.join(s3_folder, file_name)
                    
                                ###File upload
                                s3_client.upload_file(file_path, s3_bucket_name, s3_key)
                                logging.info(f'Uploaded {file} to s3://{s3_bucket_name}/{s3_key} at {datetime.now(timezone.utc)}')
            else:
                logging.info(f'Skipped file {s3_key} (Already up-to-date) at {datetime.now(timezone.utc)}')

        else:
            logging.critical(f'Failed to download a file (Status Code: {file_bls_response.status_code}) at {datetime.now(timezone.utc)}. Please contact appropriate team members.')

    ###Deletion Prep
    ###Regrabbing S3 objects
    logging.info(f'Starting file clean up at {datetime.now(timezone.utc)}')
    list_response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_folder)
    s3_objects = []
    ###For loop to get them into file objects
    for obj in list_response.get('Contents', []):
        file_objects.append(obj['Key'])
      
    ###For loop to get local files out
    local_objects = []
    for root, dirs, files in os.walk(bls_download_dir):
        for file in files:
            local_objects.append(os.path.join(root, file))

    ###Formatting for proper matching
    download_objects = [os.path.normpath(s).replace('\\', '/') for s in local_objects]
    
    ###For loop to delete files we not longer find on BLS website
    for file in s3_objects:
        if file not in download_objects:
            s3_key = s3_folder + file
            logging.info(f'Deleting object {file} at {datetime.now(timezone.utc)}. File no longer found on BLS Website')
            s3_client.delete_object(Bucket=s3_bucket_name, Key = s3_key)
    
    ###Local file clean up
    logging.info(f'Starting local clean up at {datetime.now(timezone.utc)}')
    for root, dirs, files in os.walk(bls_download_dir):
        for file in files:
            os.remove(os.path.join(root, file))
    ###Dropping directory we created earlier
    os.rmdir(bls_download_dir)
    logging.info(f'Finished script at {datetime.now(timezone.utc)}')
else:
    logging.critical(f'Error connecting to BLS webiste. Status code {bls_response.status_code} received at {datetime.now(timezone.utc)}')
