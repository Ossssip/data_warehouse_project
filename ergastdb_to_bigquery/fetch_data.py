# -*- coding: utf-8 -*-
import tempfile
import time
from google.cloud import bigquery




#getting data from API

import requests
import json


def save_data_to_json(data, file_path):
    with open(file_path, "w") as json_file:
        # Convert each dictionary in the list to a JSON string
        json_data = '\n'.join(json.dumps(d) for d in data)
        json_file.write(json_data)
        
def push_to_bigquery(table_id,file):
    #change the path to service account secret if needed
    client = bigquery.Client.from_service_account_json('../secrets/bq.json')
    # Specify your project ID and dataset ID
    project_id = 'cohesive-gadget-386815'
    dataset_id = 'races'
    
    # Specify the table ID and the path to your JSON file
    #table_id = 'tmp2'
    # file_path = './nd-seasons.json'
    
    # Create a reference to the dataset and table
    table_ref = client.dataset(dataset_id).table(table_id)
    
    # Define the job configuration
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True
    
    # Start the load job
    with open(file, 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    # job = client.load_table_from_file(file, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    
    # Check if the job completed successfully
    if job.state == 'DONE':
        print(f'File {file} uploaded to {project_id}.{dataset_id}.{table_id}')
    else:
        print(f'Error uploading file {file}: {job.errors}')

def fetch_from_ergast(endpoint,keyword,bq_table_name):
    # API Endpoint
    api_url = f"http://ergast.com/api/f1/{endpoint}.json"
   
    # Retrieve data from the API
    # keyword='SeasonTable'
    #max allowed by terms of use is 1000
    limit=30
    response = requests.get(api_url+f'?limit={limit}')
    if response.status_code != 200:
        raise ValueError('error fetching data: ', response.status_code)
    else:
        data = response.json()
        records = data['MRData'][keyword]
        #get the total number of records, if we got not all the data, fetch more
        total = int(data['MRData']['total'])
        if total>limit:
            print('total records:', str(total))
            fetched = limit
            while fetched < total:
                print('fetching', str(limit), 'more records...')
                time.sleep(.1) # to not spam the db
                tmp = requests.get(api_url+f'?limit={limit}&offset={fetched}')
                if tmp.status_code != 200:
                    raise ValueError('error fetching data: ', tmp.status_code)   
                tmp = tmp.json()['MRData'][keyword]
                for key in records.keys(): #actually only expect to have one key
                      # Combine the lists from dict1 and dict2 for the current key
                      records[key] = records[key] + tmp[key]
                fetched = fetched + limit
                
        save_data_to_json(records[list(records.keys())[0]], 'tmp.json')
        push_to_bigquery(bq_table_name,'tmp.json')
        
    
    
#fetch_from_ergast('seasons','SeasonTable','tmp2')     
#fetch_from_ergast('results','RaceTable','tmp3')     
fetch_from_ergast('driverStandings','StandingsTable','driverStandings')  
 