# -*- coding: utf-8 -*-
from google.cloud import bigquery
from zipfile import ZipFile
from csv import DictReader

import requests
import time
import tempfile
import os.path

zip_file_url = 'https://ergast.com/downloads/f1db_csv.zip'
project_id = 'cohesive-gadget-386815'
dataset_id = 'races_raw'
client = bigquery.Client.from_service_account_json('../secrets/bq.json') #update with the path to your key

#need this as bigquery schema autordetect is not reliable, 
#easier to declare all vars string 
def getschema(file_path):
    schema = []
    with open(file_path, 'r') as read_obj:
        # pass the file object to DictReader() to get the DictReader object
        csv_dict_reader = DictReader(read_obj)
        # get column names from a csv file
        column_names = csv_dict_reader.fieldnames
    for c in column_names:
        schema.append(bigquery.SchemaField(c,"STRING"))
    return schema

#need to obtain the table reference to recreate it
def table_reference(project_id, dataset_id, table_id):
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = bigquery.TableReference(dataset_ref, table_id)
    return table_ref

def upload_csv(client, table_ref, csv_file):
    client.delete_table(table_ref, not_found_ok=True) #delete old data

    load_job_configuration = bigquery.LoadJobConfig()
    load_job_configuration.autodetect = True
    load_job_configuration.schema = getschema(csv_file)
    load_job_configuration.source_format = bigquery.SourceFormat.CSV
    #load_job_configuration.skip_leading_rows = 1
    # load_job_configuration.allow_quoted_newlines = True

    with open(csv_file, 'rb') as source_file:
        print('Uploading', source_file.name)
        upload_job = client.load_table_from_file(
            source_file,
            destination=table_ref,          
            location='US',
            job_config=load_job_configuration
        )

    while upload_job.state != 'DONE':
        time.sleep(2)
        upload_job.reload()
        print(upload_job.state)
    print(upload_job.result())
    
#create dataset, if needed
client.create_dataset(dataset_id, exists_ok=True)

#fetch the recent version of the ergast database and push files to bigquery
u = requests.get(zip_file_url)
with tempfile.NamedTemporaryFile() as zip_file_path:
    zip_file_path.write(u.content)
    with ZipFile(zip_file_path, 'r') as zip_file:
         for file in zip_file.namelist():
                 table_name = file.split('.')[0]
                 with tempfile.TemporaryDirectory() as tmpdirname:
                     # Extract the file from the zip and save it in the temporary file
                     zip_file.extract(file, tmpdirname)
                     table_ref = table_reference(project_id, dataset_id, table_name)
                     upload_csv(client, table_ref, os.path.join(tmpdirname,file))

