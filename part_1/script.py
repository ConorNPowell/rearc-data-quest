import requests
from bs4 import BeautifulSoup
import os
import boto3
from datetime import datetime, timezone, timedelta
import logging
import re
import os
import json

# Function to get last modified time from S3
def get_s3_last_modified_time(bucket_name, key):
    try:
        s3_client = boto3.client('s3')
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key)
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'] == key:
                    return obj['LastModified'].replace(tzinfo=timezone.utc)
        return None
    except Exception as e:
        logging.error(f'Error getting S3 object metadata: {e}. At {datetime.now(timezone.utc)}')
        return None

# Set up logging
logging.basicConfig(level=logging.INFO)
logging.info(f'Starting upload of BLS Data at {datetime.now(timezone.utc)}')

# Initialize S3 client and bucket information
s3_client = boto3.client('s3')
s3_bucket_name = 'rearc-data-quest-conor'
bls_folder = 'bls_data/'

# BLS website URL and headers
bls_url = 'https://download.bls.gov/pub/time.series/pr/'
bls_headers = {'User-Agent': 'conor.n.powell@gmail.com'}

# Logging BLS website access
logging.info(f'Hitting BLS Website at {datetime.now(timezone.utc)}')
bls_response = requests.get(bls_url, headers=bls_headers)

# Check if BLS website response is successful
if bls_response.status_code == 200:
    logging.info(f'Successfully hit BLS Site and received a status code 200 at {datetime.now(timezone.utc)}')

    # Parse HTML content of BLS page
    bls_soup_parsed = BeautifulSoup(bls_response.content, 'html.parser')
    
    file_info = []
    # Find the pre tag
    pre_tag = bls_soup_parsed.find('pre')

    # Process each line in the pre tag
    for line in pre_tag.find_all('br'):
        if line.previousSibling.text == "[To Parent Directory]":
            continue
        else:
            if "pr." in line.previousSibling.text:
                last_modified = f"{line.previousSibling.previousSibling.strip().split(" ")[0]} {line.previousSibling.previousSibling.strip().split(" ")[2]}{line.previousSibling.previousSibling.strip().split(" ")[3]}"
                file_info.append( {
                        'href': line.previousSibling.text,
                        'last_modified': datetime.strptime(last_modified, "%m/%d/%Y %I:%M%p")
                    })
    # Create a directory to store downloaded files
    bls_download_dir = 'bls_files'
    os.makedirs(bls_download_dir, exist_ok=True)

    # Iterate through each file information
    for info in file_info:
        file_url = bls_url + info['href']
        file_url = file_url.replace('pr//','')
        file_url = file_url.replace('series/pub/time.series/','series/')
        print(file_url)
        file_bls_response = requests.get(file_url, headers=bls_headers)
        
        # Check if file download was successful
        if file_bls_response.status_code == 200:
            file_path = os.path.join(bls_download_dir, info['href'])
            
            # Save file to local directory
            with open(file_path, 'wb') as file:
                file.write(file_bls_response.content)

            # Get last modified time from local file
            #last_modified_local = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc)

            # Get last modified time from S3
            s3_last_modified = get_s3_last_modified_time(s3_bucket_name, bls_folder + info['href'])
            s3_aware_last_modifed = s3_last_modified.replace(tzinfo=timezone.utc)
            s3_aware_format = s3_aware_last_modifed.strftime("%m/%d/%y")

            file_last_modified = info['last_modified']
            file_aware_last_modified = file_last_modified.replace(tzinfo=timezone.utc)
            file_aware_format = file_aware_last_modified.strftime("%m/%d/%y")

            # Check if file needs to be uploaded to S3
            if s3_last_modified is None or file_aware_format > s3_aware_format:
                s3_key = bls_folder + info['href']
                s3_client.upload_file(file_path, s3_bucket_name, s3_key)
                logging.info(f'Uploaded {info["href"]} to s3://{s3_bucket_name}/{s3_key} at {datetime.now(timezone.utc)}')
            else:
                logging.info(f'Skipped file {info["href"]} (Already up-to-date) at {datetime.now(timezone.utc)}')

            # Remove local file after processing
            os.remove(file_path)

        else:
            logging.error(f'Failed to download file {info["filename"]} (Status Code: {file_bls_response.status_code}) at {datetime.now(timezone.utc)}')
            logging.error(f'Response content: {file_bls_response.content}')

    # Clean up local files after processing
    os.rmdir(bls_download_dir)

    logging.info(f'Finished script at {datetime.now(timezone.utc)}')

else:
    logging.error(f'Error connecting to BLS website. Status code {bls_response.status_code} received at {datetime.now(timezone.utc)}')


###Setting hard coded variables
data_usa_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
s3_bucket_name = 'rearc-data-quest-conor'
usa_folder = 'usa_data/'
formatted_date = datetime.now(timezone.utc).strftime("%m_%d_%Y")
delete_folder_filter = datetime.now(timezone.utc) - timedelta(days=2)

###Creating session to connect to API
logging.info(f'Creating session to connect to USA Data API at {datetime.now(timezone.utc)}')
usa_session = requests.session()

###Hitting Data USA API
data_usa_resposne = usa_session.get(data_usa_url)

###Proceed with logic if we get a 200
if data_usa_resposne.status_code == 200:
    logging.info(f'Sucuessfully received 200 from USA Data API at {datetime.now(timezone.utc)}')

    ###Making sure data comes in as JSON
    usa_data = data_usa_resposne.json()
    usa_json_data = json.dumps(usa_data)

    ###Formatting s3 folder info
    s3_folder_key = usa_folder+formatted_date+'/usa_data_api.json'

    ###Uploading to s3 folder
    logging.info(f'Uploading JSON data to S3 Bucket at {datetime.now(timezone.utc)}')
    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_folder_key, Body=usa_json_data)
    logging.info(f'Upload of USA Data API in JSON was uploaded at {datetime.now(timezone.utc)}')
else:
    logging.critical(f'Issue with connection to the Data USA API. Status Code {data_usa_resposne.status_code} given at {datetime.now()}')


###List folders in bucket function
def list_folders(bucket_name, folder_prefix):
    ###Getting meta data on folders present
    logging.info(f'Grabbing list of files in bucket {bucket_name} for folder {folder_prefix} for deletion at {datetime.now(timezone.utc)}')
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
    logging.info(f'Starting function to delete {folder_name} from bucket, {bucket_name}, at {datetime.now(timezone.utc)}')
    objects_to_delete = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    delete_keys = {'Objects': []}

    ###Collection all object keys within the folder to delete
    if 'Contents' in objects_to_delete:
        for obj in objects_to_delete['Contents']:
            delete_keys['Objects'].append({'Key': obj['Key']})
    ###Deleting the collected keys
    if delete_keys['Objects']:
        logging.info(f'Attempting to delete folder, {folder_name}, in bucket, {bucket_name}, at {datetime.now(timezone.utc)}')
        s3_client.delete_objects(Bucket=bucket_name, Delete=delete_keys)
        logging.info(f'Deleted folder, {folder_name}, in bucket, {bucket_name}, at {datetime.now(timezone.utc)}')

###Calling list_folders function
folders = list_folders(s3_bucket_name, usa_folder)

###For loop for deletion
for folder in folders:
    ###Clean string for matching
    date_str = folder[len(usa_folder):-1] ###Get rid of prefix and ending slash
    try:
        ###Getting the actual folder date
        folder_date = datetime.strptime(date_str, '%m_%d_%Y').replace(tzinfo=timezone.utc)
        ###If statement for management
        if folder_date < delete_folder_filter:
            delete_folder(s3_bucket_name, folder)
    except ValueError:
        logging.critical(f'Skipping folder, {folder}, at {datetime.now(timezone.utc)}')
